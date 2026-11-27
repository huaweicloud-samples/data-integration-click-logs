region            = "cn-north-4"
project_name      = "data-integration-click-logs"
domain_name       = "GT-dgcmy"
obs_bucket_name   = "data-integration-click-logs-parquet"

# 键值库接入地址。留空时由实例的负载均衡地址自动推导；当前实例的负载均衡地址
# 所在端口挂在 VPC 默认安全组下，未放通本项目函数所在网段，函数侧无法直连，
# 因此这里显式指定同实例的节点直连地址（同子网、同项目安全组，已验证可达）。
geminidb_endpoint = "http://10.0.1.124:8000"

# geminidb_password 为必填敏感变量，请勿以明文写入会被提交的文件。
# 执行 apply 时通过环境变量传入（推荐）：
#   $env:TF_VAR_geminidb_password = "你的实例密码"
# 或在命令中显式传入：
#   terraform apply -var="geminidb_password=你的实例密码"
# 密码要求：8-32 位，且包含大小写字母、数字、特殊字符中的至少三类。
