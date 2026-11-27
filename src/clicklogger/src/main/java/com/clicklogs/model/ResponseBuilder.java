package com.clicklogs.model;

import java.util.HashMap;
import java.util.Map;

/**
 * 响应构建辅助类，用于以链式方式组装 ClickLogResponse。
 */
public class ResponseBuilder {

    private int statusCode = 200;
    private Map<String, String> headers = new HashMap<>();
    private String body = "";

    public ResponseBuilder() {
    }

    public ResponseBuilder ok() {
        this.statusCode = 200;
        this.body = "{\"message\":\"ok\"}";
        return this;
    }

    public ResponseBuilder badRequest(String message) {
        this.statusCode = 400;
        this.body = "{\"message\":\"" + message + "\"}";
        return this;
    }

    public ResponseBuilder originHeader(String origin) {
        this.headers.put("Access-Control-Allow-Origin", origin);
        return this;
    }

    public ClickLogResponse build() {
        return new ClickLogResponse(this.statusCode, this.headers, this.body);
    }
}
