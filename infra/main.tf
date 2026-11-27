# ============================================================================
# 主入口：Terraform 配置与提供方要求
# ============================================================================
# 本实践用 huaweicloud 提供方搭建「网关接入 → 通道缓冲 → 函数消费 → 落桶 + 写库」
# 的数据接入链路。各资源的定义分散在同目录下的独立文件中：
#   network.tf       VPC / 子网 / 安全组
#   storage.tf       对象存储桶与函数产物对象
#   dms.tf           DMS Kafka 实例与主题（缓冲层）
#   geminidb.tf      键值库实例
#   apig.tf          网关实例 / 分组 / 自定义认证 / API
#   functiongraph.tf 三个函数与 Kafka 触发器
#   iam.tf           委托与项目级角色
#   lts.tf           日志组与日志流
#   variables.tf     输入变量
#   outputs.tf       输出值
# ============================================================================

terraform {
  required_version = ">= 1.5"

  required_providers {
    huaweicloud = {
      source  = "huaweicloud/huaweicloud"
      version = "~> 1.97"
    }
  }

  backend "local" {}
}
