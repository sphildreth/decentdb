package com.decentdb.jdbc;

import org.junit.jupiter.api.Test;

import java.sql.Types;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for type mapping.
 * No native library required.
 */
class TypeMappingTest {

    @Test
    void integerType() {
        assertEquals(Types.BIGINT, TypeMapping.jdbcTypeFromName("INTEGER"));
        assertEquals(Types.BIGINT, TypeMapping.jdbcTypeFromName("INT"));
        assertEquals(Types.BIGINT, TypeMapping.jdbcTypeFromName("BIGINT"));
    }

    @Test
    void caseInsensitive() {
        assertEquals(Types.BIGINT, TypeMapping.jdbcTypeFromName("integer"));
        assertEquals(Types.VARCHAR, TypeMapping.jdbcTypeFromName("text"));
        assertEquals(Types.DOUBLE, TypeMapping.jdbcTypeFromName("real"));
    }

    @Test
    void textType() {
        assertEquals(Types.VARCHAR, TypeMapping.jdbcTypeFromName("TEXT"));
        assertEquals(Types.VARCHAR, TypeMapping.jdbcTypeFromName("VARCHAR"));
    }

    @Test
    void boolType() {
        assertEquals(Types.BOOLEAN, TypeMapping.jdbcTypeFromName("BOOLEAN"));
        assertEquals(Types.BOOLEAN, TypeMapping.jdbcTypeFromName("BOOL"));
    }

    @Test
    void decimalType() {
        assertEquals(Types.DECIMAL, TypeMapping.jdbcTypeFromName("DECIMAL"));
        assertEquals(Types.DECIMAL, TypeMapping.jdbcTypeFromName("NUMERIC"));
    }

    @Test
    void blobType() {
        assertEquals(Types.BINARY, TypeMapping.jdbcTypeFromName("BLOB"));
        assertEquals(Types.BINARY, TypeMapping.jdbcTypeFromName("BYTEA"));
    }

    @Test
    void dateTimeTypes() {
        assertEquals(Types.DATE, TypeMapping.jdbcTypeFromName("DATE"));
        assertEquals(Types.TIME, TypeMapping.jdbcTypeFromName("TIME"));
        assertEquals(Types.TIMESTAMP, TypeMapping.jdbcTypeFromName("TIMESTAMP"));
    }

    @Test
    void unknownType() {
        assertEquals(Types.OTHER, TypeMapping.jdbcTypeFromName("UUID"));
        assertEquals(Types.OTHER, TypeMapping.jdbcTypeFromName("JSON"));
        assertEquals(Types.OTHER, TypeMapping.jdbcTypeFromName("UNKNOWN_TYPE"));
        assertEquals(Types.OTHER, TypeMapping.jdbcTypeFromName(null));
    }

    @Test
    void kindMapping() {
        assertEquals(Types.BIGINT, TypeMapping.jdbcTypeFromKind(DecentDBNative.KIND_INT64));
        assertEquals(Types.DOUBLE, TypeMapping.jdbcTypeFromKind(DecentDBNative.KIND_FLOAT64));
        assertEquals(Types.VARCHAR, TypeMapping.jdbcTypeFromKind(DecentDBNative.KIND_TEXT));
        assertEquals(Types.BINARY, TypeMapping.jdbcTypeFromKind(DecentDBNative.KIND_BLOB));
        assertEquals(Types.BOOLEAN, TypeMapping.jdbcTypeFromKind(DecentDBNative.KIND_BOOL));
        assertEquals(Types.DECIMAL, TypeMapping.jdbcTypeFromKind(DecentDBNative.KIND_DECIMAL));
        assertEquals(Types.NULL, TypeMapping.jdbcTypeFromKind(DecentDBNative.KIND_NULL));
    }
}
