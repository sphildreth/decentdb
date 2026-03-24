package com.decentdb.jdbc;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

/**
 * Builds {@link SQLException} instances with appropriate SQLState codes.
 *
 * SQLState classes used:
 *   "08001" - connection failure
 *   "08006" - connection dropped
 *   "22000" - data exception
 *   "23000" - integrity constraint violation
 *   "42000" - syntax error or access rule violation
 *   "HY000" - general error
 *   "S1000" - general error (ODBC-compat)
 */
final class Errors {

    private Errors() {}

    static SQLException connection(String msg) {
        return new SQLException(msg, "08001");
    }

    static SQLException connectionClosed(String msg) {
        return new SQLException(msg, "08003");
    }

    static SQLException syntax(String msg, int vendorCode) {
        return new SQLException(msg, "42000", vendorCode);
    }

    static SQLException constraint(String msg, int vendorCode) {
        return new SQLException(msg, "23000", vendorCode);
    }

    static SQLException general(String msg, int vendorCode) {
        return new SQLException(msg, "HY000", vendorCode);
    }

    static SQLFeatureNotSupportedException notSupported(String feature) {
        return new SQLFeatureNotSupportedException(
            "Feature not supported by DecentDB: " + feature, "0A000");
    }

    /**
     * Converts a DecentDB C API error code + message into the most
     * appropriate {@link SQLException} subtype.
     */
    static SQLException fromNative(int code, String msg) {
        switch (code) {
            case DecentDBNative.ERR_CONSTRAINT:
                return constraint(msg, code);
            case DecentDBNative.ERR_SQL:
                return syntax(msg, code);
            case DecentDBNative.ERR_IO:
            case DecentDBNative.ERR_CORRUPTION:
                return new SQLException(msg, "08006", code);
            default:
                return general(msg, code);
        }
    }

    /** Throws if the native result code indicates an error. */
    static void checkResult(long dbHandle, int result) throws SQLException {
        if (result != 0) {
            String msg = DecentDBNative.dbLastErrorMessage(dbHandle);
            int code = DecentDBNative.dbLastErrorCode(dbHandle);
            int effectiveCode = code != 0 ? code : result;
            throw fromNative(effectiveCode, msg != null ? msg : "Unknown native error (code " + result + ")");
        }
    }

    static void checkStatus(long dbHandle, int status) throws SQLException {
        if (status != 0) {
            String msg = DecentDBNative.dbLastErrorMessage(dbHandle);
            int code = DecentDBNative.dbLastErrorCode(dbHandle);
            int effectiveCode = code != 0 ? code : status;
            throw fromNative(effectiveCode, msg != null ? msg : "Native status error (code " + status + ")");
        }
    }
}
