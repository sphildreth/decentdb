import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Simulates exactly what DecentDBMetaModel.getViewDDL() does:
 * Prepares "SELECT sql_text FROM decentdb_system_views WHERE name=?",
 * binds the view name, and reads the result.
 */
public class TestViewDDLQuery {
    public static void main(String[] args) throws Exception {
        Class.forName("com.decentdb.jdbc.DecentDBDriver");
        String dbPath = "jdbc:decentdb:/tmp/db-tests/demo.ddb";

        try (Connection conn = DriverManager.getConnection(dbPath)) {
            String[] views = {"v_top_users", "v_user_post_counts", "v_recent_posts"};

            for (String viewName : views) {
                System.out.println("=== Testing view: " + viewName + " ===");
                try (PreparedStatement ps = conn.prepareStatement(
                        "SELECT sql_text FROM decentdb_system_views WHERE name=?")) {
                    System.out.println("  PreparedStatement class: " + ps.getClass().getName());
                    ps.setString(1, viewName);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            String ddl = rs.getString(1);
                            System.out.println("  DDL: " + (ddl != null ? ddl.substring(0, Math.min(80, ddl.length())) + "..." : "NULL"));
                        } else {
                            System.out.println("  NO ROWS RETURNED");
                        }
                    }
                } catch (Exception e) {
                    System.out.println("  ERROR: " + e.getClass().getName() + ": " + e.getMessage());
                }
            }
        }
    }
}
