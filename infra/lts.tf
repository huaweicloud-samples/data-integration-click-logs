resource "huaweicloud_lts_group" "click_log_lts" {
  group_name  = "${var.project_name}-lts"
  ttl_in_days = 7
}

resource "huaweicloud_lts_stream" "business" {
  group_id    = huaweicloud_lts_group.click_log_lts.id
  stream_name = "click-logger-business"
}

resource "huaweicloud_lts_stream" "consumer" {
  group_id    = huaweicloud_lts_group.click_log_lts.id
  stream_name = "click-logger-consumer"
}

resource "huaweicloud_lts_stream" "authorizer" {
  group_id    = huaweicloud_lts_group.click_log_lts.id
  stream_name = "click-logger-authorizer"
}
