resource "huaweicloud_geminidb_instance" "click_log_db" {
  name              = "${var.project_name}-db"
  mode              = "Cluster"
  password          = var.geminidb_password
  availability_zone = var.availability_zones
  vpc_id            = huaweicloud_vpc.data_integration_click_logs_vpc.id
  subnet_id         = huaweicloud_vpc_subnet.data_integration_click_logs_subnet.id
  security_group_id = huaweicloud_networking_secgroup.data_integration_click_logs_sg.id
  charging_mode     = "postPaid"

  # 兼容键值协议接口：该接口保持底层宽列协议，同时对外提供键值格式的服务
  datastore {
    type           = "dynamodb"
    storage_engine = "rocksDB"
    version        = ""
  }

  flavor {
    num       = var.geminidb_node_count
    size      = var.geminidb_storage_size
    storage   = "ULTRAHIGH"
    spec_code = var.geminidb_flavor
  }

  backup_strategy {
    start_time = "02:00-03:00"
    keep_days  = 7
  }

  # 保护现有实例：密码由控制台单独管理/重置，terraform 不据此重建或变更实例。
  # 未知原密码时切勿让 terraform 比对密码，否则可能触发 ForceNew 导致实例重建、数据丢失。
  lifecycle {
    ignore_changes = [password]
  }
}

# 数据表与二级索引由消费者函数在首次写入时按协议接口幂等创建，
# 当前资源编排尚无可声明表结构的资源类型。
