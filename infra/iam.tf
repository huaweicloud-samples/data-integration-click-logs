# 函数访问对象存储与键值库所需的委托（缓冲层改用 DMS Kafka，Kafka 走 VPC 内网明文，无需额外授权）
#
# 委托类型必须是「云服务委托」，即由函数工作流服务（op_svc_cff）代持：
# 账号委托（delegated_domain_name）不会被 FunctionGraph 接受，创建函数时会报 FSS.1501
# （The delegate not exist）。op_svc_ 前缀表示委托给云服务，op_svc_cff 即函数工作流。
resource "huaweicloud_identity_agency" "functiongraph_click_logs" {
  name                   = "${var.project_name}-fgs-agency"
  delegated_service_name = "op_svc_cff"
  duration               = "FOREVER"

  # 对象存储属全局服务，其权限需在全局（domain）级授予，项目级授权对桶操作不生效。
  # 同时授予函数工作流自带的全局最小权限集，该策略是函数工作流自身能力校验的基线。
  domain_roles = [
    "OBS OperateAccess",
    "fgs_default_global_role",
  ]

  project_role {
    project = var.region
    roles = [
      "OBS OperateAccess",
      "GeminiDB FullAccess",
      # 函数日志投递到 LTS 需要 FGS 经本委托访问日志组/流，缺省会报 FSS.1212
      "LTS FullAccess",
      # 消息缓冲走 VPC 内网，FGS 需经本委托创建/删除弹性网卡，
      # 缺省会报 "no operational permissions vpc, check the func xrole permissions"
      "VPC Administrator",
      # Kafka 触发器消费主题消息需要数据接入服务权限
      "DMS Administrator",
      # 函数工作流自带的项目级最小权限集（含 VPC 等基线权限）
      "fgs_default_region_role",
    ]
  }
}
