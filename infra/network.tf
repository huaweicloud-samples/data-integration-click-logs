resource "huaweicloud_vpc" "data_integration_click_logs_vpc" {
  name = "${var.project_name}-vpc"
  cidr = var.vpc_cidr
}

resource "huaweicloud_vpc_subnet" "data_integration_click_logs_subnet" {
  name              = "${var.project_name}-subnet"
  cidr              = var.subnet_cidr
  gateway_ip        = cidrhost(var.subnet_cidr, 1)
  vpc_id            = huaweicloud_vpc.data_integration_click_logs_vpc.id
  availability_zone = var.availability_zone
}

resource "huaweicloud_networking_secgroup" "data_integration_click_logs_sg" {
  name        = "${var.project_name}-sg"
  description = "click log ingestion security group"
}

# 入方向：仅放通同网段内的访问，公网不可直达
resource "huaweicloud_networking_secgroup_rule" "allow_internal" {
  direction         = "ingress"
  ethertype         = "IPv4"
  protocol          = "tcp"
  port_range_min    = 1
  port_range_max    = 65535
  remote_ip_prefix  = var.vpc_cidr
  security_group_id = huaweicloud_networking_secgroup.data_integration_click_logs_sg.id
}

# 出方向：函数与网关需访问数据接入、对象存储、键值库的服务端点
resource "huaweicloud_networking_secgroup_rule" "allow_egress_tcp" {
  direction         = "egress"
  ethertype         = "IPv4"
  protocol          = "tcp"
  port_range_min    = 1
  port_range_max    = 65535
  remote_ip_prefix  = "0.0.0.0/0"
  security_group_id = huaweicloud_networking_secgroup.data_integration_click_logs_sg.id
}

# 出方向：域名解析走 UDP
resource "huaweicloud_networking_secgroup_rule" "allow_egress_udp" {
  direction         = "egress"
  ethertype         = "IPv4"
  protocol          = "udp"
  port_range_min    = 1
  port_range_max    = 65535
  remote_ip_prefix  = "0.0.0.0/0"
  security_group_id = huaweicloud_networking_secgroup.data_integration_click_logs_sg.id
}

# 对象存储的私网访问出口：VPC 内不放通公网 NAT/EIP，函数要读写对象存储
# 必须经网关型终端节点，由系统为该服务的地址段写入指向终端节点的下一跳路由。
data "huaweicloud_vpc_route_table" "click_logs_default" {
  vpc_id = huaweicloud_vpc.data_integration_click_logs_vpc.id
}

resource "huaweicloud_vpcep_endpoint" "data_integration_click_logs_obs" {
  service_id  = var.obs_vpcep_service_id
  vpc_id      = huaweicloud_vpc.data_integration_click_logs_vpc.id
  routetables = [data.huaweicloud_vpc_route_table.click_logs_default.id]
}
