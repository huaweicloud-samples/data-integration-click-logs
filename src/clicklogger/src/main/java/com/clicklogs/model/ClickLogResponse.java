package com.clicklogs.model;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 业务处理函数返回的标准响应结构，包含状态码、响应头与响应体。
 */
public class ClickLogResponse {

    private int statusCode;
    private Map<String, String> headers;
    private String body;

    public ClickLogResponse() {
    }

    public ClickLogResponse(int statusCode, Map<String, String> headers, String body) {
        this.statusCode = statusCode;
        this.headers = headers;
        this.body = body;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    /**
     * 转换为函数运行时可直接返回的响应结构。
     */
    public Map<String, Object> toMap() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("statusCode", statusCode);
        result.put("headers", headers == null ? new LinkedHashMap<String, String>() : headers);
        result.put("body", body == null ? "" : body);
        result.put("isBase64Encoded", false);
        return result;
    }
}
