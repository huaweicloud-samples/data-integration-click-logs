# ============================================================================
# 提供方配置与当前项目标识解析
# ============================================================================

provider "huaweicloud" {
  region = var.region
}

# 当前区域的项目 ID：未显式指定时按区域名解析
data "huaweicloud_identity_projects" "current" {
  name = var.region
}

locals {
  project_id = var.project_id != "" ? var.project_id : data.huaweicloud_identity_projects.current.projects[0].id
}
