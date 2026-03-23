package org.jkiss.dbeaver.ext.decentdb.model;

import org.jkiss.code.NotNull;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.generic.model.GenericView;
import org.jkiss.dbeaver.ext.generic.model.meta.GenericMetaModel;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;

import java.sql.SQLException;
import java.util.Map;

/**
 * DecentDB-specific meta model.
 * <p>
 * Overrides the generic model to provide view DDL by querying the
 * virtual system view {@code decentdb_system_views} that is intercepted
 * by the JDBC driver and resolved from the catalog without touching
 * the SQL engine.
 */
public class DecentDBMetaModel extends GenericMetaModel {

    @Override
    public String getViewDDL(
        @NotNull DBRProgressMonitor monitor,
        @NotNull GenericView sourceObject,
        @NotNull Map<String, Object> options
    ) throws DBException {
        try (JDBCSession session = DBUtils.openMetaSession(monitor, sourceObject, "Read DecentDB view definition")) {
            try (JDBCPreparedStatement stmt = session.prepareStatement(
                "SELECT sql_text FROM decentdb_system_views WHERE name=?"
            )) {
                stmt.setString(1, sourceObject.getName());
                try (JDBCResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        String ddl = rs.getString(1);
                        if (ddl != null && !ddl.isEmpty()) {
                            return "CREATE OR REPLACE VIEW " + sourceObject.getName() + " AS\n"
                                + formatSql(ddl) + ";";
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new DBException("Error reading view definition", e);
        }
        return "-- View definition not available";
    }

    /**
     * Lightweight SQL formatter: inserts newlines before major clauses
     * and indents the body for readability in the DBeaver DDL editor.
     */
    private static String formatSql(String sql) {
        // Keywords that start a new major clause (case-insensitive)
        String[] clauseKeywords = {
            "SELECT ", "FROM ", "INNER JOIN ", "LEFT JOIN ", "RIGHT JOIN ",
            "FULL JOIN ", "CROSS JOIN ", "JOIN ", "LEFT OUTER JOIN ",
            "RIGHT OUTER JOIN ", "FULL OUTER JOIN ",
            "WHERE ", "GROUP BY ", "HAVING ", "ORDER BY ",
            "LIMIT ", "OFFSET ", "UNION ALL ", "UNION ", "EXCEPT ", "INTERSECT ",
            "ON "
        };

        StringBuilder sb = new StringBuilder(sql.length() + 64);
        String remaining = sql.trim();

        while (!remaining.isEmpty()) {
            // Find the earliest next clause keyword
            int bestPos = -1;
            String bestKw = null;
            for (String kw : clauseKeywords) {
                int pos = indexOfIgnoreCase(remaining, kw, 1); // skip pos 0
                if (pos > 0 && (bestPos < 0 || pos < bestPos)) {
                    bestPos = pos;
                    bestKw = kw;
                }
            }
            if (bestPos < 0) {
                sb.append(remaining);
                break;
            }
            sb.append(remaining, 0, bestPos);
            sb.append('\n');
            // Indent sub-clauses (ON, JOIN variants)
            String upper = bestKw.trim().toUpperCase();
            if (upper.equals("ON") || upper.endsWith("JOIN")) {
                sb.append("  ");
            }
            remaining = remaining.substring(bestPos);
        }
        return sb.toString();
    }

    private static int indexOfIgnoreCase(String str, String target, int fromIndex) {
        int targetLen = target.length();
        int maxPos = str.length() - targetLen;
        for (int i = fromIndex; i <= maxPos; i++) {
            if (str.regionMatches(true, i, target, 0, targetLen)) {
                return i;
            }
        }
        return -1;
    }
}
