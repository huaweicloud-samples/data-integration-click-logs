package com.clicklogs.Handlers;

import com.clicklogs.model.ClickLogRequest;
import com.clicklogs.model.ResponseBuilder;
import com.clicklogs.support.FunctionSettings;
import com.google.gson.Gson;
import com.google.gson.JsonParser;
import com.huawei.services.runtime.Context;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

/**
 * 接收 API 网关转发的点击日志事件，校验必填字段后以 JSON 形式投递给 DMS Kafka 主题。
 * 列式序列化与对象存储写入、键值库写入由 Kafka 触发器驱动的消费者函数承担。
 *
 * <p>入口签名采用 FunctionGraph Java 运行时支持的 {@code String} 形态：运行时把请求体
 * 以 JSON 字符串注入，由本类解析为事件 Map 后复用既有逻辑。</p>
 */
public class ClickLoggerHandler {

    private static final Gson GSON = new Gson();
    private static final String DATE_PATTERN = "MM-dd-yyyy HH:mm:ss";

    /** 生产者客户端在函数实例内复用，避免每次调用重新建链。 */
    private static volatile Producer<String, String> kafkaProducer;
    private static volatile String kafkaConfigKey;

    /**
     * 函数入口。event 为 API 网关触发器事件的 JSON 字符串，context 由运行时注入。
     */
    public Map<String, Object> handleRequest(String event, Context context) {
        return handleRequestInner(readEvent(event), context);
    }

    private Map<String, Object> handleRequestInner(Map<String, Object> event, Context context) {
        try {
            ClickLogRequest request = parseBody(extractBody(event));
            fillDefaults(request);
            validateOrFail(request);

            String bootstrapServers = FunctionSettings.get(context, "KAFKA_BOOTSTRAP_SERVERS");
            String topic = FunctionSettings.get(context, "KAFKA_TOPIC");
            if (StringUtils.isBlank(bootstrapServers) || StringUtils.isBlank(topic)) {
                log(context, "kafka configuration is incomplete");
                return new ResponseBuilder().badRequest("kafka configuration is incomplete")
                        .originHeader("*").build().toMap();
            }

            send(bootstrapServers, topic, request, context);
            return new ResponseBuilder().ok().originHeader("*").build().toMap();
        } catch (IllegalArgumentException ex) {
            log(context, "validation failed: " + ex.getMessage());
            return new ResponseBuilder().badRequest(ex.getMessage()).originHeader("*").build().toMap();
        } catch (Exception ex) {
            log(context, "internal error: " + ex.getMessage());
            return new ResponseBuilder().badRequest("internal error").originHeader("*").build().toMap();
        }
    }

    private String extractBody(Map<String, Object> event) {
        if (event == null) {
            return null;
        }
        Object body = event.get("body");
        if (body == null) {
            return null;
        }
        String raw = String.valueOf(body);
        if (Boolean.parseBoolean(String.valueOf(event.get("isBase64Encoded")))) {
            return new String(Base64.getDecoder().decode(raw), StandardCharsets.UTF_8);
        }
        return raw;
    }

    private ClickLogRequest parseBody(String body) {
        if (StringUtils.isBlank(body)) {
            throw new IllegalArgumentException("empty body");
        }
        return GSON.fromJson(JsonParser.parseString(body).getAsJsonObject(), ClickLogRequest.class);
    }

    private void fillDefaults(ClickLogRequest request) {
        if (StringUtils.isBlank(request.getUser())) {
            request.setUser("GUEST");
        }
        if (StringUtils.isBlank(request.getClientip())) {
            request.setClientip("APIGWY");
        }
        if (StringUtils.isBlank(request.getCreatedtime())) {
            request.setCreatedtime(new SimpleDateFormat(DATE_PATTERN).format(new Date()));
        }
    }

    private void validateOrFail(ClickLogRequest request) {
        if (StringUtils.isBlank(request.getRequestid())) {
            throw new IllegalArgumentException("requestid missing");
        }
        if (StringUtils.isBlank(request.getContextid())) {
            throw new IllegalArgumentException("contextid missing");
        }
        if (StringUtils.isBlank(request.getCallerid())) {
            throw new IllegalArgumentException("callerid missing");
        }
        if (StringUtils.isBlank(request.getType())) {
            throw new IllegalArgumentException("type missing");
        }
        if (StringUtils.isBlank(request.getComponent())) {
            throw new IllegalArgumentException("component missing");
        }
        if (StringUtils.isBlank(request.getAction())) {
            throw new IllegalArgumentException("action missing");
        }
    }

    private void send(String bootstrapServers, String topic, ClickLogRequest request, Context context) throws Exception {
        Producer<String, String> producer = producerFor(bootstrapServers);
        ProducerRecord<String, String> record =
                new ProducerRecord<>(topic, request.getRequestid(), GSON.toJson(request));
        RecordMetadata meta = producer.send(record).get();
        log(context, "kafka send accepted, topic=" + meta.topic()
                + ", partition=" + meta.partition() + ", offset=" + meta.offset());
    }

    private Producer<String, String> producerFor(String bootstrapServers) {
        Producer<String, String> cached = kafkaProducer;
        if (cached != null && bootstrapServers.equals(kafkaConfigKey)) {
            return cached;
        }
        synchronized (ClickLoggerHandler.class) {
            if (kafkaProducer == null || !bootstrapServers.equals(kafkaConfigKey)) {
                Properties props = new Properties();
                props.put("bootstrap.servers", bootstrapServers);
                props.put("key.serializer",
                        "org.apache.kafka.common.serialization.StringSerializer");
                props.put("value.serializer",
                        "org.apache.kafka.common.serialization.StringSerializer");
                props.put("acks", "1");
                props.put("retries", "3");
                kafkaProducer = new KafkaProducer<>(props);
                kafkaConfigKey = bootstrapServers;
            }
            return kafkaProducer;
        }
    }

    private void log(Context context, String message) {
        if (context != null && context.getLogger() != null) {
            context.getLogger().log(message);
        }
    }

    /**
     * 将运行期注入的事件 JSON 字符串解析为事件 Map；遇到空串或解析异常时回退为空 Map，
     * 由上层逻辑给出明确的校验失败信息，避免函数崩溃。
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
}
