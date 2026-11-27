package com.clicklogs.support;

import com.huawei.services.runtime.Context;
import org.apache.commons.lang3.StringUtils;

/**
 * 函数运行期配置读取工具。
 *
 * <p>读取顺序为：先读运行环境注入的配置项，再读函数上配置的自定义数据。
 * 访问凭据走同一顺序，避免把凭据写进代码或基础设施模板。</p>
 */
public final class FunctionSettings {

    /** 访问密钥标识所对应的配置项名称。 */
    public static final String ACCESS_KEY = "HUAWEICLOUD_SDK_AK";

    /** 访问密钥所对应的配置项名称。 */
    public static final String SECRET_KEY = "HUAWEICLOUD_SDK_SK";

    /** 项目标识所对应的配置项名称。 */
    public static final String PROJECT_ID = "HUAWEICLOUD_SDK_PROJECT_ID";

    private FunctionSettings() {
    }

    /**
     * 读取配置项，不存在时返回 {@code null}。
     */
    public static String get(Context context, String key) {
        String fromEnvironment = System.getenv(key);
        if (StringUtils.isNotBlank(fromEnvironment)) {
            return fromEnvironment.trim();
        }
        if (context == null) {
            return null;
        }
        try {
            String fromFunctionData = context.getUserData(key);
            return StringUtils.isBlank(fromFunctionData) ? null : fromFunctionData.trim();
        } catch (Exception ignored) {
            return null;
        }
    }

    /**
     * 读取配置项，缺失时回落到给定默认值。
     */
    public static String getOrDefault(Context context, String key, String fallback) {
        String value = get(context, key);
        return StringUtils.isBlank(value) ? fallback : value;
    }

    public static String accessKey(Context context) {
        return get(context, ACCESS_KEY);
    }

    public static String secretKey(Context context) {
        return get(context, SECRET_KEY);
    }

    public static boolean hasCredentials(Context context) {
        return StringUtils.isNotBlank(accessKey(context)) && StringUtils.isNotBlank(secretKey(context));
    }
}
