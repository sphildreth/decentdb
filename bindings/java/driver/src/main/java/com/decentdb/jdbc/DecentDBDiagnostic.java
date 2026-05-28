package com.decentdb.jdbc;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Structured diagnostic payload attached to native DecentDB failures. */
public final class DecentDBDiagnostic {
    private final String rawJson;
    private final Integer nativeCode;
    private final String codeName;
    private final String subcode;
    private final String sqlState;
    private final Boolean retryable;
    private final Boolean permanent;

    private DecentDBDiagnostic(String rawJson) {
        this.rawJson = rawJson;
        this.nativeCode = intField(rawJson, "code");
        this.codeName = stringField(rawJson, "code_name");
        this.subcode = stringField(rawJson, "subcode");
        this.sqlState = stringField(rawJson, "sqlstate");
        this.retryable = boolField(rawJson, "retryable");
        this.permanent = boolField(rawJson, "permanent");
    }

    public static DecentDBDiagnostic fromJson(String rawJson) {
        if (rawJson == null || rawJson.isBlank()) {
            return null;
        }
        return new DecentDBDiagnostic(rawJson);
    }

    public String getRawJson() { return rawJson; }
    public Integer getNativeCode() { return nativeCode; }
    public String getCodeName() { return codeName; }
    public String getSubcode() { return subcode; }
    public String getSqlState() { return sqlState; }
    public Boolean isRetryable() { return retryable; }
    public Boolean isPermanent() { return permanent; }

    private static String stringField(String json, String key) {
        Matcher matcher = Pattern
            .compile("\"" + Pattern.quote(key) + "\"\\s*:\\s*\"([^\"\\\\]*(?:\\\\.[^\"\\\\]*)*)\"")
            .matcher(json);
        return matcher.find() ? unescape(matcher.group(1)) : null;
    }

    private static Integer intField(String json, String key) {
        Matcher matcher = Pattern
            .compile("\"" + Pattern.quote(key) + "\"\\s*:\\s*(-?\\d+)")
            .matcher(json);
        if (!matcher.find()) {
            return null;
        }
        try {
            return Integer.parseInt(matcher.group(1));
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private static Boolean boolField(String json, String key) {
        Matcher matcher = Pattern
            .compile("\"" + Pattern.quote(key) + "\"\\s*:\\s*(true|false)")
            .matcher(json);
        return matcher.find() ? Boolean.valueOf(matcher.group(1)) : null;
    }

    private static String unescape(String value) {
        return value
            .replace("\\\"", "\"")
            .replace("\\\\", "\\")
            .replace("\\/", "/")
            .replace("\\n", "\n")
            .replace("\\r", "\r")
            .replace("\\t", "\t");
    }
}
