package com.decentdb.jdbc;

import java.sql.Types;

/**
 * Maps DecentDB native type names and kind constants to JDBC {@link Types}.
 */
final class TypeMapping {

    private TypeMapping() {}

    /**
     * Maps a DecentDB column type name (as returned by the C API or catalog)
     * to a JDBC SQL type constant.
     */
    static int jdbcTypeFromName(String typeName) {
        if (typeName == null) return Types.OTHER;
        switch (typeName.toUpperCase().trim()) {
            case "INTEGER":
            case "INT":
            case "BIGINT":
            case "INT8":
                return Types.BIGINT;
            case "SMALLINT":
            case "INT4":
            case "INT2":
                return Types.INTEGER;
            case "REAL":
            case "DOUBLE":
            case "DOUBLE PRECISION":
            case "FLOAT":
            case "FLOAT8":
            case "FLOAT4":
                return Types.DOUBLE;
            case "NUMERIC":
            case "DECIMAL":
                return Types.DECIMAL;
            case "TEXT":
            case "VARCHAR":
            case "CHAR":
            case "CHARACTER VARYING":
                return Types.VARCHAR;
            case "BOOLEAN":
            case "BOOL":
                return Types.BOOLEAN;
            case "BLOB":
            case "BYTEA":
                return Types.BINARY;
            case "DATE":
                return Types.DATE;
            case "TIME":
                return Types.TIME;
            case "TIMESTAMP":
            case "TIMESTAMPTZ":
                return Types.TIMESTAMP;
            case "UUID":
                return Types.OTHER;
            case "JSON":
            case "JSONB":
                return Types.OTHER;
            default:
                return Types.OTHER;
        }
    }

    /**
     * Maps a DecentDB value kind constant to a JDBC SQL type constant.
     */
    static int jdbcTypeFromKind(int kind) {
        switch (kind) {
            case DecentDBNative.KIND_INT64:
            case DecentDBNative.KIND_INT0:
            case DecentDBNative.KIND_INT1:
                return Types.BIGINT;
            case DecentDBNative.KIND_FLOAT64: return Types.DOUBLE;
            case DecentDBNative.KIND_TEXT:    return Types.VARCHAR;
            case DecentDBNative.KIND_BLOB:    return Types.BINARY;
            case DecentDBNative.KIND_BOOL:
            case DecentDBNative.KIND_BOOL_FALSE:
            case DecentDBNative.KIND_BOOL_TRUE:
                return Types.BOOLEAN;
            case DecentDBNative.KIND_DECIMAL: return Types.DECIMAL;
            case DecentDBNative.KIND_NULL:    return Types.NULL;
            default:                          return Types.OTHER;
        }
    }

    /** Returns a human-readable SQL type name for display. */
    static String typeName(int jdbcType) {
        switch (jdbcType) {
            case Types.BIGINT:  return "BIGINT";
            case Types.INTEGER: return "INTEGER";
            case Types.DOUBLE:  return "DOUBLE";
            case Types.DECIMAL: return "DECIMAL";
            case Types.VARCHAR: return "TEXT";
            case Types.BOOLEAN: return "BOOLEAN";
            case Types.BINARY:  return "BLOB";
            case Types.DATE:    return "DATE";
            case Types.TIME:    return "TIME";
            case Types.TIMESTAMP: return "TIMESTAMP";
            case Types.NULL:    return "NULL";
            default:            return "OTHER";
        }
    }
}
