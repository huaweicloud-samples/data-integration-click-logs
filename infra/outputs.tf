output "obs_bucket_name" {
  value = huaweicloud_obs_bucket.click_log_parquet.bucket
}

output "apig_api_url" {
  value = "https://${huaweicloud_apig_instance.click_log_api.ingress_address}/clicklogger"
}

output "kafka_topic_name" {
  value = huaweicloud_dms_kafka_topic.click_log.name
}

output "kafka_bootstrap_address" {
  description = "DMS Kafka 实例内网明文接入地址（函数经 VPC 内网连接，端口 9092）"
  value       = huaweicloud_dms_kafka_instance.click_log.port_protocol[0].private_plain_address
}

output "geminidb_instance_id" {
  value = huaweicloud_geminidb_instance.click_log_db.id
}

output "geminidb_endpoint" {
  description = "键值库兼容接口接入地址（含协议与端口，可直接回填到 geminidb_endpoint 变量）"
  value       = local.geminidb_endpoint
  sensitive   = true
}

output "functiongraph_click_logger_urn" {
  value = huaweicloud_fgs_function.click_logger.urn
}

output "functiongraph_click_logger_authorizer_urn" {
  value = huaweicloud_fgs_function.click_logger_authorizer.urn
}

output "functiongraph_click_logger_stream_urn" {
  value = huaweicloud_fgs_function.click_logger_stream.urn
}

output "lts_group_id" {
  value = huaweicloud_lts_group.click_log_lts.id
}
