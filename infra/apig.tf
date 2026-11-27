resource "huaweicloud_apig_instance" "click_log_api" {
  name              = "${var.project_name}-apig"
  edition           = "BASIC"
  vpc_id            = huaweicloud_vpc.data_integration_click_logs_vpc.id
  subnet_id         = huaweicloud_vpc_subnet.data_integration_click_logs_subnet.id
  security_group_id = huaweicloud_networking_secgroup.data_integration_click_logs_sg.id
  maintain_begin    = "02:00:00"
}

resource "huaweicloud_apig_group" "click_log_group" {
  instance_id = huaweicloud_apig_instance.click_log_api.id
  name        = "${var.project_name}-group"
}

# 前端鉴权：请求进入后端前先调用鉴权函数校验调用方标识
# 注意：网关自定义认证名称只允许中文/英文字母/数字/下划线，不支持中划线，
# 因此这里把项目名前缀里的中划线替换为下划线，否则创建会报 APIG.2011。
resource "huaweicloud_apig_custom_authorizer" "click_log_authorizer" {
  instance_id  = huaweicloud_apig_instance.click_log_api.id
  name         = "${replace(var.project_name, "-", "_")}_front_auth"
  type         = "FRONTEND"
  function_urn = huaweicloud_fgs_function.click_logger_authorizer.urn
  cache_age    = 0

  identity {
    location = "HEADER"
    name     = "Authorization"
  }
}

resource "huaweicloud_apig_api" "click_log_api" {
  instance_id             = huaweicloud_apig_instance.click_log_api.id
  group_id                = huaweicloud_apig_group.click_log_group.id
  name                    = "click-logger"
  type                    = "Public"
  request_method          = "POST"
  request_path            = "/clicklogger"
  request_protocol        = "HTTPS"
  security_authentication = "AUTHORIZER"
  authorizer_id           = huaweicloud_apig_custom_authorizer.click_log_authorizer.id
  simple_authentication   = false

  # 后端函数地址不要带版本后缀：网关侧把版本单独存放在 version 字段中，
  # 若 function_urn 带 :latest 会与回读值不一致，产生无法更新的差异
  # （网关不支持修改后端，plan 会直接报 func_graph can't be updated）。
  # 后端超时需覆盖函数冷启动：函数包较大，首次调用要完成运行时与依赖类加载，
  # 默认 5 秒会在冷启动时直接返回 APIG.0203「Backend timeout」。
  # 实例侧 backend_timeout 特性允许的上限为 60000 毫秒。
  func_graph {
    function_urn     = trimsuffix(huaweicloud_fgs_function.click_logger.urn, ":latest")
    version          = "latest"
    invocation_type  = "sync"
    network_type     = "V1"
    request_protocol = "HTTPS"
    timeout          = 60000
  }

  tags = [var.project_name]
}

# 把 API 发布到 RELEASE 环境。缺了这一步，网关侧会返回 APIG.0101
# 「The API does not exist or has not been published in the environment」，
# 请求在匹配阶段就被拒绝、不会进入鉴权与后端函数。
resource "huaweicloud_apig_api_publishment" "click_log_api_release" {
  instance_id = huaweicloud_apig_instance.click_log_api.id
  api_id      = huaweicloud_apig_api.click_log_api.id
  env_id      = "DEFAULT_ENVIRONMENT_RELEASE_ID"
  description = "点击日志接入接口的正式发布版本"
}
