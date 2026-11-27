# 部署指导

本文档说明如何通过 Terraform 与 Maven 部署「点击日志采集与落盘」最佳实践。整体流程为：本地构建 Java 函数产物，初始化 Terraform 并规划资源，执行 apply 完成云端资源创建，回填运行时配置，最后通过 curl 与控制台验证落桶、写库与日志。请按顺序执行各步骤，遇到错误可参考第 9 节的故障排除。

## 1. 前置条件

- 华为云账号，且所用身份具备 APIG、FunctionGraph、OBS、GeminiDB、DMS Kafka、LTS、IAM、VPC 的 FullAccess 权限或等效细粒度策略，否则部分资源无法创建。若使用细粒度策略，请确认已覆盖本实践涉及的全部服务操作。
- 本地工具链：Terraform 1.5 及以上版本；huaweicloud provider 版本约 1.97；JDK 8；Maven 3.6 及以上。可通过 `terraform version`、`mvn -v`、`java -version` 快速校验。
- 访问凭据：在华为云「我的凭证」页面获取 Access Key，用于 Terraform 调用云 API，以及供函数调用对象存储与 Kafka 主题。请妥善保管凭据，避免提交到代码仓库。
- 账号名：在 IAM 控制台「委托」页面创建委托时显示的账号名，部署脚本需要该值作为 `domain_name`，用于绑定委托所在账号。
- 可选工具：obsutil 命令行工具，用于验证落桶与清理桶内对象。若未安装，可直接使用控制台方式完成对应步骤。

## 2. 源码构建

```bash
cd data-integration-click-logs/src/clicklogger
mvn clean package -DskipTests
ls target/clicklogger-1.0-SNAPSHOT.jar
```

说明：函数工程依赖包含 DMS Kafka 生产者客户端、对象存储 SDK、Parquet、Avro、Hadoop 等组件；执行打包后会生成包含全部依赖的 fat jar，随后由 Terraform 上传至对象存储供函数引用。该 fat jar 同时包含 Authorizer、ClickLogger 与 ClickLoggerStream 三个函数的处理逻辑，部署时由函数按 handler 入口区分调用。

构建前请确认 `src/clicklogger/lib/` 下已按 `lib/README.md` 的说明放置函数运行时接口包，否则编译会因缺少 `com.huawei.services.runtime` 而失败。

## 3. Terraform 初始化

```bash
cd data-integration-click-logs/infra
export HW_ACCESS_KEY=<your-ak>
export HW_SECRET_KEY=<your-sk>
export TF_VAR_geminidb_password=<your-password>
export TF_VAR_domain_name=<your-account-name>
terraform init
```

上述配置项含义：`HW_ACCESS_KEY` 与 `HW_SECRET_KEY` 为步骤 1 中获取的访问凭据，供 Terraform 调用云 API；`TF_VAR_geminidb_password` 为键值库实例管理员密码，需满足长度与字符复杂度要求；`TF_VAR_domain_name` 为账号名。

provider 版本已在 `required_providers` 中固定为约 1.97，避免版本漂移导致配置不兼容。`terraform init` 会拉取 huaweicloud provider 并初始化后端状态，首次执行需网络可达；若使用离线镜像，请按团队的镜像配置方式调整。

## 4. 配置调整（可选）

在 `infra/variables.tf` 中可调整以下变量：

- `region`：部署区域，默认 `cn-north-4`；
- `project_name`：项目名称前缀，会体现在资源命名与委托名中；
- `obs_bucket_name`：对象存储桶名，需全局唯一；
- `geminidb_flavor`：键值库实例规格，影响性能与容量；
- `geminidb_node_count` / `geminidb_storage_size`：键值库节点数与单节点容量；
- `kafka_topic_partitions`：DMS Kafka 主题分区数；
- `auth_tokens`：鉴权白名单（分号分隔），决定哪些调用方可通过鉴权；
- `function_memory_size` / `function_timeout`：函数内存与超时，影响单实例并发与处理窗口。

变量既可在 `variables.tf` 中直接修改默认值，也可通过 `terraform apply -var="..."` 或同名环境变量（前缀 `TF_VAR_`）覆盖。若团队多人协作，建议将可变参数放在独立的 `terraform.tfvars` 中管理，仓库已提供 `terraform.tfvars.example` 作为模板。

必改项：

- `geminidb_password`：需满足复杂度要求，否则实例创建会失败；
- `domain_name`：需与账号名一致，否则委托创建会报 `domain not found`；
- `obs_bucket_name`：桶名全局唯一，请改为未被占用的名称，避免 `InvalidBucketName`。

## 5. Terraform Plan 校验

```bash
terraform validate
terraform plan -out=tfplan
```

`terraform validate` 用于检查配置语法；`terraform plan` 会列出即将创建的资源清单。预期规划创建约 22 个资源：VPC 1 个、子网 1 个、安全组 1 个及其规则 3 条、对象存储桶 1 个及对象 1 个、DMS Kafka 实例 1 个及主题 1 个、键值库实例 1 个、IAM 委托 1 个、函数 3 个、Kafka 触发器 1 个、网关实例 / 分组 / 自定义认证 / API 各 1 个、日志组 1 个及日志流 3 个。

建议将 plan 输出保存到 `tfplan` 文件，供 apply 阶段直接使用。请在执行 apply 前核对 plan 中的资源名称与数量是否符合预期。

## 6. Terraform Apply 部署

```bash
terraform apply tfplan
```

预计耗时 5 至 10 分钟，其中键值库实例创建耗时较长，请等待命令返回完成提示。apply 成功后 Terraform 会输出 `apig_api_url`、`kafka_topic_name`、`kafka_bootstrap_address`、`geminidb_endpoint` 等关键信息，请记录备用。若部署中途因网络中断而失败，可重新执行 `terraform apply tfplan`，Terraform 会基于已有状态继续补齐剩余资源，无需从零开始。

## 7. 部署后配置

部署完成后需要补两项运行时配置，函数才能完整跑通链路。

### 7.1 设置密钥类配置项

出于凭据安全的考虑，访问密钥与数据库密码不随基础设施代码下发，请在函数配置中单独设置：

- `click-logger` 与 `click-logger-stream`：设置 `HUAWEICLOUD_SDK_AK`、`HUAWEICLOUD_SDK_SK`，取值与步骤 3 中 Terraform 使用的访问凭据一致；
- `click-logger-stream`：额外设置 `GEMINIDB_PASSWORD`，取值为键值库实例管理员密码。

在函数工作流控制台打开对应函数，进入「配置」页的配置项区域添加上述键值对并保存即可，保存后无需重新部署代码。

### 7.2 回填键值库接入地址

兼容接口的接入地址是实例维度的，无法从区域推导，需在实例创建后回填：

```bash
cd data-integration-click-logs/infra
terraform output -raw geminidb_endpoint
```

将结果填入 `terraform.tfvars` 的 `geminidb_endpoint`（形如 `http://<负载均衡地址>:<端口>`），再执行一次 `terraform apply`，使消费函数获得正确的接入地址。

## 8. 部署后验证

按"接口 → Kafka 投递 → 落桶 → 写库 → 日志"的顺序逐项验证，确保整条链路连通。

### 8.1 获取关键输出

```bash
URL=$(terraform output -raw apig_api_url)
TOPIC=$(terraform output -raw kafka_topic_name)
```

### 8.2 调用 API

```bash
curl -i -X POST "$URL" \
  -H "Authorization: ALLOW=ORDERAPP" \
  -H "Content-Type: application/json" \
  -d '{"requestid":"r-001","contextid":"c-001","callerid":"web","component":"login","action":"click","type":"webpage","user":"alice","clientip":"10.0.0.1"}'
```

预期结果：HTTP 200，响应头包含 CORS 相关字段。其中 `Authorization: ALLOW=ORDERAPP` 需与 `auth_tokens` 白名单中的某一项一致，否则会返回 401。

**注意**：HTTP 200 仅表明业务函数成功把事件投递到 DMS Kafka 主题；落桶与写库由 Kafka 触发器异步调用消费者函数完成，需再等待 1 至 2 分钟观察对象存储桶与键值库表。

### 8.3 验证 Kafka 投递

控制台方式：进入「分布式消息服务 DMS」→ Kafka 专享版 → 实例 → 打开 `${project_name}-kafka` → 监控，查看生产消息数是否递增。

### 8.4 验证落桶

控制台方式：进入「对象存储服务 OBS」→ 桶列表 → 打开目标桶 → 对象列表，应能看到形如 `clicklog/data=YYYY-MM-DD/<timestamp>-r-001.parquet` 的文件。

命令行方式：

```bash
obsutil ls obs://<your-bucket-name>/clicklog/data=$(date +%Y-%m-%d)/
```

若未看到文件，请先确认 API 调用返回 200，再检查 `click-logger-consumer` 日志流中是否有对象上传相关记录。

### 8.5 验证键值库表

控制台方式：进入「云数据库 GeminiDB」→ 实例列表 → 打开 `${project_name}-db` → 表管理 → `click_log_table`，按 requestid 查询 `r-001`，应能看到一条记录。

表与二级索引由消费者函数在首次写入时创建，因此首次调用可能比后续调用多一次建表开销。若表为空，请确认实例已就绪、消费者函数已被触发，并参考第 9 节排查写入失败原因。

### 8.6 验证日志

控制台方式：进入「云日志服务 LTS」→ 日志组 → 打开 `${project_name}-lts` → 日志流，应能看到三条日志流：`click-logger-business`（业务入口）、`click-logger-consumer`（数据落地）、`click-logger-authorizer`（鉴权）。

这三条流分别对应业务函数投递日志、消费者函数写入日志与鉴权函数日志，是排障时首要查看的位置。网关访问日志可通过网关侧的日志能力另行开启。

## 9. 故障排除

- apply 报错 `InvalidBucketName`：桶名包含非法字符或已被占用，请修改 `obs_bucket_name` 后重试。
- apply 报错 `domain not found`：`domain_name` 取值错误，请核对账号名。
- 函数调用返回 401：请求头 `Authorization` 的值不在 `AUTH_TOKENS` 白名单内，请核对是否一致；注意白名单以分号分隔且区分大小写。
- 函数调用返回 400 且提示 Kafka 配置不完整：检查函数的配置项是否正确设置了 `KAFKA_BOOTSTRAP_SERVERS` 与 `KAFKA_TOPIC`。
- 函数调用返回 400 且提示访问凭据未配置：按第 7.1 节为函数补设访问密钥配置项。
- 对象存储长时间无新对象：查看 `click-logger-consumer` 日志流，确认 Kafka 触发器状态为已启用且最近一次触发时间不为 0；若触发器未启动，可在控制台手工触发一次排查。
- 写库失败：查看 `click-logger-consumer` 日志流，确认键值库接入地址指向实例负载均衡地址、实例状态为可用、数据面账号为实例自带账号（`rwuser`）与实例密码；兼容接口的数据面不接受云平台访问密钥。
- 键值库表不存在：消费者函数会在首次写入时自动建表；若权限不足导致建表失败，可在控制台手工建表，结构为 requestid + contextid 主键、`ContextCallerIndex` 二级索引（contextid + callerid）。

## 10. 清理资源

```bash
terraform destroy
# 若桶内仍有对象，需先清空再销毁
obsutil rm obs://<your-bucket-name>/clicklog/ -r -f
obsutil rb obs://<your-bucket-name> -f
```

提示：使用完毕后及时释放资源，避免环境残留。对象存储桶若仍有对象，`terraform destroy` 可能无法直接删除，需先清空桶内数据。`terraform destroy` 会按依赖逆序删除资源，键值库实例释放同样需要数分钟。Kafka 主题数据由消费者按批消费后无需保留，不需手工清理。

## 11. 关键参数说明

- `geminidb_flavor`：键值库实例规格码，默认 `geminidb.dynamodb.xlarge.4`，需与所创建实例类型在控制台的规格列表一致；规格越高吞吐能力越强，仅做功能验证时可下调。
- `geminidb_node_count`：键值库节点数，默认 3，用于多可用区部署。
- `geminidb_endpoint`：兼容接口接入地址，实例创建后回填；数据面端口固定，不随实例规格变化。
- `function_memory_size`：函数内存，默认 2048 MB，影响单实例并发与处理上限；事件量较大时可适当上调。
- `function_timeout`：函数超时时间，默认 300 秒，需覆盖列式编码与双写耗时。
- `auth_tokens`：鉴权白名单，多个标识以分号分隔，需与调用方 `Authorization` 头匹配；生产环境建议替换为真实身份体系。
- `obs_bucket_name`：对象存储桶名（全局唯一），删除后重建可能因名称占用而失败，建议命名包含项目名与时间后缀。
- `kafka_topic_partitions`：DMS Kafka 主题分区数，默认 1，可在 `infra/dms.tf` 中按需调整。
