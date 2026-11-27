resource "huaweicloud_obs_bucket" "click_log_parquet" {
  bucket        = var.obs_bucket_name
  acl           = "private"
  versioning    = true
  force_destroy = true

  # 仅对列式数据目录设置过期清理，函数代码包不受影响
  lifecycle_rule {
    name    = "expire-parquet"
    enabled = true
    prefix  = "clicklog/"

    expiration {
      days = 30
    }
  }
}

# 三个函数共用同一个产物包，按需上传
resource "huaweicloud_obs_bucket_object" "click_logger_jar" {
  bucket = huaweicloud_obs_bucket.click_log_parquet.bucket
  key    = var.functiongraph_jar_obs_path
  source = "../src/clicklogger/target/clicklogger-1.0-SNAPSHOT.jar"
}
