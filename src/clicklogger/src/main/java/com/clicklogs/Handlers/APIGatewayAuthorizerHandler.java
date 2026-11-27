package com.clicklogs.Handlers;

import com.clicklogs.support.FunctionSettings;
import com.google.gson.Gson;
import com.huawei.services.runtime.Context;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * API 网关前端自定义认证函数：校验请求头中的调用方标识是否命中白名单。
 * 返回值形如 {"statusCode":200,"body":"{\"status\":\"allow\",\"context\":{...}}"}，
 * 网关据此决定是否放行请求。
 *
 * <p>入口签名采用 FunctionGraph Java 运行时支持的 {@code String} 形态：运行时把前端请求
 * 事件以 JSON 字符串注入，由本类解析为事件 Map 后复用既有逻辑。</p>
 */
public class APIGatewayAuthorizerHandler {

    private static final Gson GSON = new Gson();
    private static final String AUTH_HEADER = "Authorization";
    private static final String STATUS_ALLOW = "allow";
    private static final String STATUS_DENY = "deny";

    /**
     * 函数入口。event 为 API 网关转发的前端请求事件 JSON 字符串，context 由运行时注入。
     */
    public Map<String, Object> handleRequest(String event, Context context) {
        return handleRequestInner(readEvent(event), context);
    }

    private Map<String, Object> handleRequestInner(Map<String, Object> event, Context context) {
        String token = extractToken(event);
        String whitelist = FunctionSettings.get(context, "AUTH_TOKENS");

        if (StringUtils.isBlank(whitelist)) {
            log(context, "whitelist configuration is missing");
            return deny("whitelist configuration is missing");
        }
        if (StringUtils.isBlank(token)) {
            log(context, "authorization header is missing");
            return deny("authorization header is missing");
        }

        List<String> allowed = Arrays.asList(whitelist.split(";"));
        if (!allowed.contains(token.trim())) {
            log(context, "caller is not whitelisted");
            return deny("caller is not whitelisted");
        }
        return allow();
    }

    private String extractToken(Map<String, Object> event) {
        if (event == null) {
            return null;
        }
        Object headers = event.get("headers");
        if (!(headers instanceof Map)) {
            return null;
        }
        Map<?, ?> headerMap = (Map<?, ?>) headers;
        Object value = headerMap.get(AUTH_HEADER);
        if (value == null) {
            value = headerMap.get(AUTH_HEADER.toLowerCase());
        }
        return value == null ? null : String.valueOf(value);
    }

    private Map<String, Object> allow() {
        Map<String, Object> context = new LinkedHashMap<>();
        context.put("caller", "whitelisted");
        context.put("authorizedAt", System.currentTimeMillis());
        return response(200, STATUS_ALLOW, context);
    }

    /**
     * 认证不通过。注意：网关要求认证函数本身始终返回 200，放行与否由响应体中的
     * status 字段表达；若函数返回非 200，网关会把这次调用判定为认证过程异常
     * （APIG.0601 frontend auth failed），而不是按“拒绝”处理。
     */
    private Map<String, Object> deny(String reason) {
        Map<String, Object> context = new LinkedHashMap<>();
        context.put("reason", reason);
        return response(200, STATUS_DENY, context);
    }

    private Map<String, Object> response(int statusCode, String status, Map<String, Object> context) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", status);
        body.put("context", context);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("statusCode", statusCode);
        response.put("body", GSON.toJson(body));
        return response;
    }

    private void log(Context context, String message) {
        if (context != null && context.getLogger() != null) {
            context.getLogger().log(message);
        }
    }

    /**
     * 将运行期注入的事件 JSON 字符串解析为事件 Map；遇到空串或解析异常时回退为空 Map。
     */
    private static Map<String, Object> readEvent(String event) {
        if (event == null || event.trim().isEmpty()) {
            return Collections.emptyMap();
        }
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> parsed = GSON.fromJson(event, Map.class);
            return parsed == null ? Collections.emptyMap() : parsed;
        } catch (Exception ex) {
            return Collections.emptyMap();
        }
    }
}
