# lib 目录说明

## 构建依赖说明

函数工作流的 Java 运行时接口包（`com.huawei.services.runtime`，提供 `Context` 与
`RuntimeLogger`）**未发布到 Maven 中央仓库**，但由 FunctionGraph 运行时在运行期自带。

本项目采用**编译期桩**方案，已将最小可用的 `Context` / `RuntimeLogger` 接口放在
`src/main/java/com/huawei/services/runtime/`，并通过 `pom.xml` 的 shade 过滤
（`com/huawei/services/runtime/**`）排除出部署 JAR——运行期由函数运行时提供真实实现。
因此本目录**不再需要手工放置 `fss-java-sdk.jar`**，仓库可直接 `mvn clean package` 构建。

## 校验

```bash
cd src/clicklogger
mvn clean package -DskipTests
```

预期输出 `BUILD SUCCESS`，并在 `target/` 下生成可用于部署的 JAR（胖包，已排除运行时 SDK）。

> 若后续希望使用华为云官方完整 SDK 替换桩，将其放入本目录并在 `pom.xml` 以 `system`
> 作用域重新引用即可；运行时行为不变。
