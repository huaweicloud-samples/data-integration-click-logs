locals {
  function_code_url  = "https://${huaweicloud_obs_bucket.click_log_parquet.bucket}.obs.${var.region}.myhuaweicloud.com/${var.functiongraph_jar_obs_path}"
  function_code_name = "click-logger.jar"

  # 三个函数共用的非敏感配置项
  common_user_data = {
    REGION       = var.region
    # 必须用解析后的项目 ID：函数配置项不接受空值，留空变量直接下发会报 FSS.1009
    PROJECT_ID   = local.project_id
    OBS_ENDPOINT = "obs.${var.region}.myhuaweicloud.com"
  }

  # 键值库兼容接口接入地址。优先取显式变量；未提供时按实例的负载均衡地址与端口自动拼装。
  # 注意：函数配置项不接受空值，直接下发空字符串会在创建函数时报 FSS.1009
  # （Invalid environment variable），因此这里始终给出可用的完整地址。
  geminidb_endpoint = var.geminidb_endpoint != "" ? var.geminidb_endpoint : format(
    "http://%s:%d",
    huaweicloud_geminidb_instance.click_log_db.lb_ip_address,
    huaweicloud_geminidb_instance.click_log_db.port,
  )

  # Kafka Topic 名称（与 dms.tf 中 huaweicloud_dms_kafka_topic.click_log.name 保持一致）。
  # 注意：此处用字面量而非引用 topic 资源，避免函数创建因 Kafka 实例 FROZEN（DMS.00400026）
  # 导致 topic 资源创建失败、进而阻断整个 apply。topic 资源待实例解冻后再单独 apply。
  kafka_topic_name = "${var.project_name}-topic"
}

# 密钥类配置（对象存储访问密钥、键值库实例密码）不随基础设施代码下发，
# 请在函数配置项中单独设置，参见 docs/deployment-guide.md 的配置章节。

# 入口函数：校验请求并投递到 DMS Kafka 主题
resource "huaweicloud_fgs_function" "click_logger" {
  name          = "${var.project_name}-click-logger"
  agency        = huaweicloud_identity_agency.functiongraph_click_logs.name
  handler       = "com.clicklogs.Handlers.ClickLoggerHandler.handleRequest"
  memory_size   = var.function_memory_size
  timeout       = var.function_timeout
  runtime       = "Java8"
  code_type     = "obs"
  code_url      = local.function_code_url
  code_filename = local.function_code_name
  app           = "default"

  vpc_id     = huaweicloud_vpc.data_integration_click_logs_vpc.id
  network_id = huaweicloud_vpc_subnet.data_integration_click_logs_subnet.id

  # 不显式指定 LTS 日志组/流：FunctionGraph 会用调用方凭证鉴权，当前 AK/SK 无 LTS 权限会报 FSS.1212，
  # 改为由 FGS 自动托管 LTS 日志（使用 FGS 服务自身权限，绕开该限制）。
  user_data = jsonencode(merge(local.common_user_data, {
    KAFKA_BOOTSTRAP_SERVERS = huaweicloud_dms_kafka_instance.click_log.port_protocol[0].private_plain_address
    KAFKA_TOPIC             = local.kafka_topic_name
  }))

  # 访问云服务的密钥（AK/SK）按运行环境单独下发，不纳入托管配置；
  # 忽略该字段的差异，避免后续 apply 覆盖掉已下发的密钥导致函数失去访问权限。
  lifecycle {
    ignore_changes = [user_data]
  }

  depends_on = [huaweicloud_obs_bucket_object.click_logger_jar]
}

# 鉴权函数：只读取白名单配置，无需访问网络
resource "huaweicloud_fgs_function" "click_logger_authorizer" {
  name          = "${var.project_name}-click-logger-authorizer"
  agency        = huaweicloud_identity_agency.functiongraph_click_logs.name
  handler       = "com.clicklogs.Handlers.APIGatewayAuthorizerHandler.handleRequest"
  memory_size   = 512
  # 超时需覆盖冷启动：函数包较大，首次调用要完成运行时与依赖类加载。
  # 鉴权函数在请求进入后端的路径上同步执行，超时会导致网关报认证失败。
  timeout       = 60
  runtime       = "Java8"
  code_type     = "obs"
  code_url      = local.function_code_url
  code_filename = local.function_code_name
  app           = "default"

  # 见 click_logger：FGS 自动托管 LTS，避免 FSS.1212
  user_data = jsonencode({
    AUTH_TOKENS = var.auth_tokens
  })

  depends_on = [huaweicloud_obs_bucket_object.click_logger_jar]
}

# 消费函数：从通道拉取记录，生成列式文件落桶并写入键值库
resource "huaweicloud_fgs_function" "click_logger_stream" {
  name          = "${var.project_name}-click-logger-stream"
  agency        = huaweicloud_identity_agency.functiongraph_click_logs.name
  handler       = "com.clicklogs.Handlers.ClickLoggerStreamHandler.handleRequest"
  memory_size   = var.function_memory_size
  timeout       = var.function_timeout
  runtime       = "Java8"
  code_type     = "obs"
  code_url      = local.function_code_url
  code_filename = local.function_code_name
  app           = "default"

  vpc_id     = huaweicloud_vpc.data_integration_click_logs_vpc.id
  network_id = huaweicloud_vpc_subnet.data_integration_click_logs_subnet.id

  # 见 click_logger：FGS 自动托管 LTS，避免 FSS.1212
  user_data = jsonencode(merge(local.common_user_data, {
    OBS_BUCKET        = huaweicloud_obs_bucket.click_log_parquet.bucket
    GEMINIDB_ENDPOINT = local.geminidb_endpoint
    GEMINIDB_USER     = var.geminidb_user
    GEMINIDB_TABLE    = var.geminidb_table
  }))

  # 见 click_logger：密钥类配置项单独下发，忽略差异以免被 apply 覆盖。
  lifecycle {
    ignore_changes = [user_data]
  }

  depends_on = [huaweicloud_obs_bucket_object.click_logger_jar]
}

# Kafka 主题到消费函数的触发：函数经 VPC 内网消费主题消息，按批触发
resource "huaweicloud_fgs_function_trigger" "kafka_to_click_logger_stream" {
  function_urn = huaweicloud_fgs_function.click_logger_stream.urn
  type         = "KAFKA"
  # 触发器创建后默认为 DISABLED，需显式置为 ACTIVE 才会真正消费主题消息
  status       = "ACTIVE"

  # 事件源参数：同时给出 topic_id 与 topic_ids，并显式声明 kafka_ssl_enable，
  # 与服务端回读值保持一致，否则每次 plan 都会出现无法收敛的差异。
  event_data = jsonencode({
    instance_id      = huaweicloud_dms_kafka_instance.click_log.id
    topic_id         = huaweicloud_dms_kafka_topic.click_log.id
    topic_ids        = [huaweicloud_dms_kafka_topic.click_log.id]
    batch_size       = 100
    kafka_ssl_enable = false
  })
}
