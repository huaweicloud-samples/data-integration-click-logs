variable "region" {
  description = "区域名称"
  type        = string
  default     = "cn-north-4"
}

variable "project_name" {
  description = "项目名称，用于拼接各资源名称"
  type        = string
  default     = "data-integration-click-logs"
}

variable "project_id" {
  description = "项目 ID。留空时按区域名自动解析当前区域的项目"
  type        = string
  default     = ""
}

variable "domain_name" {
  description = "账号名，即 IAM 委托的归属账号"
  type        = string
}

variable "vpc_cidr" {
  description = "虚拟私有云网段"
  type        = string
  default     = "10.0.0.0/16"
}

variable "subnet_cidr" {
  description = "子网网段"
  type        = string
  default     = "10.0.1.0/24"
}

variable "availability_zone" {
  description = "子网所在可用区"
  type        = string
  default     = "cn-north-4a"
}

variable "availability_zones" {
  description = "键值库集群可用区，逗号分隔的三个可用区"
  type        = string
  default     = "cn-north-4a,cn-north-4b,cn-north-4c"
}

variable "obs_bucket_name" {
  description = "对象存储桶名称，需全局唯一，用于存放列式数据文件与函数代码"
  type        = string
  default     = "data-integration-click-logs-parquet"
}

variable "kafka_topic_partitions" {
  description = "DMS Kafka 主题分区数"
  type        = number
  default     = 1
}

variable "geminidb_flavor" {
  description = "键值库实例规格码。GeminiDB 兼容 DynamoDB 接口复用 Cassandra 规格族，必须用 geminidb.cassandra.* 而非 geminidb.dynamodb.*，否则报 DBS.200054"
  type        = string
  default     = "geminidb.cassandra.xlarge.4"
}

variable "geminidb_node_count" {
  description = "键值库实例节点数量"
  type        = number
  default     = 3
}

variable "geminidb_storage_size" {
  description = "键值库单节点存储空间（GB）"
  type        = number
  default     = 100
}

variable "obs_vpcep_service_id" {
  description = "对象存储网关型终端节点服务 ID，区域相关，供 VPC 内实例以私网方式访问对象存储"
  type        = string
  default     = "3bfbef70-c26e-4ed8-a900-185c90cc5f89"
}

variable "geminidb_password" {
  description = "键值库管理员密码，长度 8 到 32 位，需包含大小写字母、数字与特殊字符中的三类"
  type        = string
  sensitive   = true
}

variable "geminidb_user" {
  description = "键值库数据面使用的实例账号，实例创建时确定"
  type        = string
  default     = "rwuser"
}

variable "geminidb_endpoint" {
  description = "键值库兼容接口的接入地址，形如 http://<负载均衡地址>:<端口>，创建实例后按输出填入"
  type        = string
  default     = ""
}

variable "geminidb_table" {
  description = "键值库中承载点击日志的数据表名称"
  type        = string
  default     = "click_log_table"
}

variable "auth_tokens" {
  description = "鉴权白名单，分号分隔，格式为 ALLOW=<调用方标识>"
  type        = string
  default     = "ALLOW=ORDERAPP;ALLOW=BILLAPP;"
}

variable "functiongraph_jar_obs_path" {
  description = "函数代码包在对象存储中的对象路径"
  type        = string
  default     = "function-code/click-logger.jar"
}

variable "function_memory_size" {
  description = "业务函数内存大小（MB）"
  type        = number
  default     = 2048
}

variable "function_timeout" {
  description = "业务函数超时时间（秒）"
  type        = number
  default     = 300
}
