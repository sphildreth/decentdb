package com.decentdb.jdbc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DiagnosticSerializationTest {
    @Test
    void sqlExceptionPreservesDiagnosticAcrossSerialization() throws Exception {
        String json = "{\"code\":9,\"code_name\":\"busy\",\"sqlstate\":\"55006\",\"retryable\":true}";
        DecentDBSQLException original = new DecentDBSQLException(
            "database busy", "55006", 9, DecentDBDiagnostic.fromJson(json));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(original);
        }
        try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            DecentDBSQLException restored = (DecentDBSQLException) input.readObject();
            assertEquals(original.getMessage(), restored.getMessage());
            assertEquals(original.getSQLState(), restored.getSQLState());
            assertEquals(original.getErrorCode(), restored.getErrorCode());
            assertEquals(json, restored.getDiagnostic().getRawJson());
            assertEquals(9, restored.getDiagnostic().getNativeCode());
            assertTrue(restored.getDiagnostic().isRetryable());
        }
    }
}
