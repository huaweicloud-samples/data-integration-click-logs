package com.clicklogs.Handlers;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.clicklogs.model.ClickLogRequest;
import com.clicklogs.support.FunctionSettings;
import com.google.gson.Gson;
import com.huawei.services.runtime.Context;
import com.obs.services.ObsClient;
import com.obs.services.model.PutObjectRequest;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Kafka 触发器函数：消费 DMS Kafka 主题中的点击日志事件，完成列式序列化与对象存储写入，
 * 并同步写入键值数据库。
 *
 * <p>入口签名采用 FunctionGraph Java 运行时支持的 {@code String} 形态：运行时把触发器
 * 事件以 JSON 字符串注入，由本类解析为事件 Map 后复用既有逻辑。</p>
 */
public class ClickLoggerStreamHandler {

    private static final Schema CLICK_LOG_SCHEMA = buildSchema();
    private static final SimpleDateFormat DATE_FMT = new SimpleDateFormat("yyyy-MM-dd");
    private static final Gson GSON = new Gson();
    private static final long THROUGHPUT_UNITS = 5L;
    private static final String OBJECT_PREFIX = "clicklog/data=";

    /** 单次调用最多回传的失败原因条数，避免响应体随批次规模膨胀。 */
    private static final int MAX_REPORTED_FAILURES = 5;

    /** 键值库数据面使用的实例账号，非云平台访问密钥。 */
    private static final String DEFAULT_STORE_USER = "rwuser";

    private static volatile ObsClient obsClient;
    private static volatile String obsConfigKey;
    private static volatile AmazonDynamoDB keyValueClient;
    private static volatile String keyValueConfigKey;
    private static volatile boolean tableReady;

    /**
     * 函数入口。event 为 KAFKA 触发器事件的 JSON 字符串，context 由运行时注入。
     */
    public Map<String, Object> handleRequest(String event, Context context) {
        return handleRequestInner(readEvent(event), context);
    }

    private Map<String, Object> handleRequestInner(Map<String, Object> event, Context context) {
        List<Map<String, Object>> records = extractRecords(event);
        if (records.isEmpty()) {
            log(context, "trigger event carried no record");
            return Collections.emptyMap();
        }

        String bucket = FunctionSettings.get(context, "OBS_BUCKET");
        String obsEndpoint = FunctionSettings.getOrDefault(context, "OBS_ENDPOINT",
                "obs." + FunctionSettings.getOrDefault(context, "REGION", "") + ".myhuaweicloud.com");
        String endpoint = FunctionSettings.get(context, "GEMINIDB_ENDPOINT");
        String tableName = FunctionSettings.getOrDefault(context, "GEMINIDB_TABLE", "click_log_table");
        String region = FunctionSettings.get(context, "REGION");
        String storeUser = FunctionSettings.getOrDefault(context, "GEMINIDB_USER", DEFAULT_STORE_USER);
        String storePassword = FunctionSettings.get(context, "GEMINIDB_PASSWORD");

        if (!FunctionSettings.hasCredentials(context)) {
            log(context, "object storage access credentials are not configured");
            return Collections.emptyMap();
        }
        if (StringUtils.isBlank(endpoint) || StringUtils.isBlank(storePassword)) {
            log(context, "key-value store connection configuration is incomplete");
            return Collections.emptyMap();
        }
        String ak = FunctionSettings.accessKey(context);
        String sk = FunctionSettings.secretKey(context);

        int written = 0;
        List<String> failures = new ArrayList<>();
        for (Map<String, Object> record : records) {
            try {
                ClickLogRequest request = decodeRecord(record);
                if (request == null) {
                    continue;
                }
                byte[] parquetBytes = writeParquet(request);
                String objectKey = OBJECT_PREFIX + DATE_FMT.format(new Date()) + "/"
                        + System.currentTimeMillis() + "-" + request.getRequestid() + ".parquet";
                uploadToObs(bucket, obsEndpoint, ak, sk, objectKey, parquetBytes, context);
                writeToKeyValueStore(request, endpoint, region, storeUser, storePassword, tableName, context);
                written++;
            } catch (Exception ex) {
                String reason = describe(ex);
                log(context, "record processing failed: " + reason);
                if (failures.size() < MAX_REPORTED_FAILURES) {
                    failures.add(reason);
                }
            }
        }
        log(context, "consumer finished, written=" + written + ", received=" + records.size());

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("received", records.size());
        summary.put("written", written);
        if (!failures.isEmpty()) {
            summary.put("errors", failures);
        }
        return summary;
    }

    /**
     * 归一化触发器事件中的记录列表，兼容通道触发与嵌套事件两种载荷形态。
     */
    private List<Map<String, Object>> extractRecords(Map<String, Object> event) {
        List<Map<String, Object>> records = new ArrayList<>();
        if (event == null) {
            return records;
        }
        Object payload = event.get("records");
        if (payload == null) {
            payload = event.get("Records");
        }
        if (payload instanceof List) {
            for (Object item : (List<?>) payload) {
                if (item instanceof Map) {
                    Map<String, Object> record = asMap(item);
                    // Kafka 触发器：records[].messages[] 为消息字符串列表
                    Object messages = record.get("messages");
                    if (messages instanceof List) {
                        for (Object message : (List<?>) messages) {
                            Map<String, Object> wrapper = new LinkedHashMap<>();
                            wrapper.put("data", String.valueOf(message));
                            records.add(wrapper);
                        }
                        continue;
                    }
                    records.add(record);
                } else if (item instanceof String) {
                    // 极简形态：records 直接是字符串列表
                    Map<String, Object> wrapper = new LinkedHashMap<>();
                    wrapper.put("data", String.valueOf(item));
                    records.add(wrapper);
                }
            }
        }
        if (records.isEmpty() && event.get("data") != null) {
            records.add(event);
        }
        return records;
    }

    private ClickLogRequest decodeRecord(Map<String, Object> record) {
        String payload = recordPayload(record);
        if (StringUtils.isBlank(payload)) {
            return null;
        }
        return GSON.fromJson(payload, ClickLogRequest.class);
    }

    private String recordPayload(Map<String, Object> record) {
        Object data = record.get("value");
        if (data == null) {
            data = record.get("data");
        }
        if (data == null) {
            data = record.get("raw_data");
        }
        if (data == null) {
            return null;
        }
        String raw = String.valueOf(data);
        if (StringUtils.isBlank(raw)) {
            return null;
        }
        try {
            String decoded = new String(Base64.getDecoder().decode(raw), StandardCharsets.UTF_8);
            if (looksLikeJson(decoded)) {
                return decoded;
            }
        } catch (IllegalArgumentException ignored) {
            // 非 Base64 载荷，按明文处理
        }
        return raw;
    }

    private boolean looksLikeJson(String text) {
        String trimmed = text.trim();
        return trimmed.startsWith("{") && trimmed.endsWith("}");
    }

    private byte[] writeParquet(ClickLogRequest request) throws Exception {
        File tmp = File.createTempFile("clicklog-" + request.getRequestid() + "-", ".parquet");
        // ParquetWriter 要求目标路径尚未存在：createTempFile 仅用于取得进程内唯一的文件名，
        // 这里先移除占位文件，再由写盘器按同一路径创建，否则会抛 FileAlreadyExistsException。
        if (!tmp.delete()) {
            throw new IllegalStateException("temp file placeholder could not be cleared: " + tmp.getAbsolutePath());
        }
        try {
            ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new Path(tmp.toURI()))
                    .withSchema(CLICK_LOG_SCHEMA)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withConf(new Configuration())
                    .build();
            try {
                GenericRecord row = new GenericData.Record(CLICK_LOG_SCHEMA);
                row.put("requestid", request.getRequestid());
                row.put("contextid", request.getContextid());
                row.put("callerid", request.getCallerid());
                row.put("component", request.getComponent());
                row.put("action", request.getAction());
                row.put("type", request.getType());
                row.put("clientip", request.getClientip());
                row.put("user", request.getUser());
                row.put("createdtime", request.getCreatedtime());
                writer.write(row);
            } finally {
                writer.close();
            }

            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            try (FileInputStream input = new FileInputStream(tmp)) {
                byte[] chunk = new byte[8192];
                int read;
                while ((read = input.read(chunk)) > 0) {
                    buffer.write(chunk, 0, read);
                }
            }
            return buffer.toByteArray();
        } finally {
            if (!tmp.delete()) {
                tmp.deleteOnExit();
            }
        }
    }

    private void uploadToObs(String bucket, String endpoint, String ak, String sk,
                            String key, byte[] payload, Context context) {
        ObsClient client = obsClientFor(endpoint, ak, sk);
        PutObjectRequest putRequest = new PutObjectRequest(bucket, key);
        putRequest.setInput(new ByteArrayInputStream(payload));
        client.putObject(putRequest);
        log(context, "object stored, key=" + key + ", bytes=" + payload.length);
    }

    private void writeToKeyValueStore(ClickLogRequest request, String endpoint, String region,
                                      String storeUser, String storePassword, String tableName, Context context) {
        DynamoDB dynamoDb = new DynamoDB(keyValueClientFor(endpoint, region, storeUser, storePassword));
        ensureTable(dynamoDb, tableName, context);

        Item item = new Item()
                .withString("requestid", request.getRequestid())
                .withString("contextid", request.getContextid())
                .withString("callerid", request.getCallerid())
                .withString("type", request.getType())
                .withString("component", request.getComponent())
                .withString("action", request.getAction())
                .withString("createdtime", request.getCreatedtime())
                .withString("clientip", request.getClientip())
                .withString("user", request.getUser());

        Table table = dynamoDb.getTable(tableName);
        table.putItem(new PutItemSpec().withItem(item));
        log(context, "record stored, requestid=" + request.getRequestid());
    }

    /**
     * 数据表与二级索引在首次写入时按幂等方式创建，避免依赖手工初始化步骤。
     */
    private void ensureTable(DynamoDB dynamoDb, String tableName, Context context) {
        if (tableReady) {
            return;
        }
        synchronized (ClickLoggerStreamHandler.class) {
            if (tableReady) {
                return;
            }
            try {
                dynamoDb.getTable(tableName).describe();
                tableReady = true;
                return;
            } catch (ResourceNotFoundException notFound) {
                log(context, "table not found, creating: " + tableName);
            }
            try {
                dynamoDb.createTable(new CreateTableRequest()
                        .withTableName(tableName)
                        .withKeySchema(
                                new KeySchemaElement("requestid", KeyType.HASH),
                                new KeySchemaElement("contextid", KeyType.RANGE))
                        .withAttributeDefinitions(
                                new AttributeDefinition("requestid", ScalarAttributeType.S),
                                new AttributeDefinition("contextid", ScalarAttributeType.S),
                                new AttributeDefinition("callerid", ScalarAttributeType.S))
                        .withProvisionedThroughput(new ProvisionedThroughput(THROUGHPUT_UNITS, THROUGHPUT_UNITS))
                        .withGlobalSecondaryIndexes(new GlobalSecondaryIndex()
                                .withIndexName("ContextCallerIndex")
                                .withKeySchema(
                                        new KeySchemaElement("contextid", KeyType.HASH),
                                        new KeySchemaElement("callerid", KeyType.RANGE))
                                .withProjection(new Projection().withProjectionType(ProjectionType.ALL))
                                .withProvisionedThroughput(
                                        new ProvisionedThroughput(THROUGHPUT_UNITS, THROUGHPUT_UNITS))));
            } catch (ResourceInUseException alreadyExists) {
                log(context, "table already exists: " + tableName);
            }
            tableReady = true;
        }
    }

    private ObsClient obsClientFor(String endpoint, String ak, String sk) {
        String configKey = "obs|" + endpoint;
        ObsClient cached = obsClient;
        if (cached != null && configKey.equals(obsConfigKey)) {
            return cached;
        }
        synchronized (ClickLoggerStreamHandler.class) {
            if (obsClient == null || !configKey.equals(obsConfigKey)) {
                obsClient = new ObsClient(ak, sk, endpoint);
                obsConfigKey = configKey;
            }
            return obsClient;
        }
    }

    private AmazonDynamoDB keyValueClientFor(String endpoint, String region, String storeUser, String storePassword) {
        String configKey = "kv|" + endpoint + "|" + region + "|" + storeUser;
        AmazonDynamoDB cached = keyValueClient;
        if (cached != null && configKey.equals(keyValueConfigKey)) {
            return cached;
        }
        synchronized (ClickLoggerStreamHandler.class) {
            if (keyValueClient == null || !configKey.equals(keyValueConfigKey)) {
                keyValueClient = AmazonDynamoDBClientBuilder.standard()
                        .withEndpointConfiguration(new EndpointConfiguration(endpoint, region))
                        .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(storeUser, storePassword)))
                        .build();
                keyValueConfigKey = configKey;
            }
            return keyValueClient;
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object value) {
        return (Map<String, Object>) value;
    }

    /**
     * 展开异常链，返回最内层原因的类型与描述，便于在调用结果中定位写入失败环节。
     */
    private static String describe(Throwable ex) {
        Throwable root = ex;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String message = root.getMessage();
        return root.getClass().getName() + (message == null ? "" : ": " + message);
    }

    private void log(Context context, String message) {
        if (context != null && context.getLogger() != null) {
            context.getLogger().log(message);
        }
    }

    /**
     * 将运行期注入的事件 JSON 字符串解析为事件 Map；遇到空串或解析异常时回退为空 Map。
     */
    private static Map<String, Object> readEvent(String event) {
        if (event == null || event.trim().isEmpty()) {
            return Collections.emptyMap();
        }
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> parsed = GSON.fromJson(event, Map.class);
            return parsed == null ? Collections.emptyMap() : parsed;
        } catch (Exception ex) {
            return Collections.emptyMap();
        }
    }

    private static Schema buildSchema() {
        String schemaJson = "{"
                + "\"type\":\"record\","
                + "\"name\":\"ClickLog\","
                + "\"namespace\":\"com.clicklogs.model\","
                + "\"fields\":["
                + "{\"name\":\"requestid\",\"type\":\"string\"},"
                + "{\"name\":\"contextid\",\"type\":\"string\"},"
                + "{\"name\":\"callerid\",\"type\":\"string\"},"
                + "{\"name\":\"component\",\"type\":\"string\"},"
                + "{\"name\":\"action\",\"type\":\"string\"},"
                + "{\"name\":\"type\",\"type\":\"string\"},"
                + "{\"name\":\"clientip\",\"type\":\"string\"},"
                + "{\"name\":\"user\",\"type\":\"string\"},"
                + "{\"name\":\"createdtime\",\"type\":\"string\"}"
                + "]}";
        return new Schema.Parser().parse(schemaJson);
    }
}
