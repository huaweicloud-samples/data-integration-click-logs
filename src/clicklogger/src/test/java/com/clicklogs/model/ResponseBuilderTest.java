package com.clicklogs.model;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 响应契约用例：网关依据状态码与响应头决定对调用方的返回，
 * 这里锁定该契约，避免后续调整破坏调用方预期。
 */
class ResponseBuilderTest {

    @Test
    void okResponseCarriesStatusAndCrossOriginHeader() {
        Map<String, Object> response = new ResponseBuilder().ok().originHeader("*").build().toMap();

        assertEquals(200, response.get("statusCode"));
        assertEquals(false, response.get("isBase64Encoded"));
        assertTrue(String.valueOf(response.get("body")).contains("ok"));

        @SuppressWarnings("unchecked")
        Map<String, String> headers = (Map<String, String>) response.get("headers");
        assertEquals("*", headers.get("Access-Control-Allow-Origin"));
    }

    @Test
    void badRequestResponseCarriesReason() {
        Map<String, Object> response = new ResponseBuilder()
                .badRequest("requestid missing").originHeader("*").build().toMap();

        assertEquals(400, response.get("statusCode"));
        assertTrue(String.valueOf(response.get("body")).contains("requestid missing"));
    }
}
