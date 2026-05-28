package com.decentdb.jdbc;

import java.sql.SQLException;

/** SQLException carrying DecentDB's structured diagnostic payload. */
public final class DecentDBSQLException extends SQLException {
    private final DecentDBDiagnostic diagnostic;

    DecentDBSQLException(
        String reason,
        String sqlState,
        int vendorCode,
        DecentDBDiagnostic diagnostic
    ) {
        super(reason, sqlState, vendorCode);
        this.diagnostic = diagnostic;
    }

    public DecentDBDiagnostic getDiagnostic() {
        return diagnostic;
    }
}
