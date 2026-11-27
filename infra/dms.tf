# 缓冲层：DMS Kafka 实例 + Topic，承担流式缓冲
#
# 背景：DIS 在 cn-north-4 的 AK/SK 接口不可用（DIS.4107，控制台 Token 可买但 API 拒收），
# 故改用标准 IAM 鉴权的 DMS Kafka 作缓冲层。实例位于 VPC 内，启用 vpc_client_plain
# 明文访问，函数经内网 9092 直连，无需 SASL 凭据；安全组 allow_internal 已放行同 SG 入向。
# 无公网 NAT 暴露。

data "huaweicloud_dms_kafka_flavors" "click_log" {
  type               = "cluster"
  availability_zones = [var.availability_zone]
  storage_spec_code  = "dms.physical.storage.ultra.v2"
}

resource "huaweicloud_dms_kafka_instance" "click_log" {
  name               = "${var.project_name}-kafka"
  engine_version     = "2.3.0"
  vpc_id             = huaweicloud_vpc.data_integration_click_logs_vpc.id
  network_id         = huaweicloud_vpc_subnet.data_integration_click_logs_subnet.id
  security_group_id  = huaweicloud_networking_secgroup.data_integration_click_logs_sg.id
  availability_zones = [var.availability_zone]
  flavor_id          = data.huaweicloud_dms_kafka_flavors.click_log.flavors[0].id
  storage_spec_code  = data.huaweicloud_dms_kafka_flavors.click_log.flavors[0].ios[0].storage_spec_code
  storage_space      = 600
  broker_num         = 3
  ssl_enable         = false
  vpc_client_plain   = true
  charging_mode      = "postPaid"

  tags = {
    project = var.project_name
  }
}

resource "huaweicloud_dms_kafka_topic" "click_log" {
  instance_id = huaweicloud_dms_kafka_instance.click_log.id
  name        = "${var.project_name}-topic"
  partitions  = var.kafka_topic_partitions
}
