import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class TestViewsDDL {
    public static void main(String[] args) throws Exception {
        Class.forName("com.decentdb.jdbc.DecentDBDriver");
        String dbPath = "jdbc:decentdb:../../demo.ddb";
        try (Connection conn = DriverManager.getConnection(dbPath)) {
            try (PreparedStatement ps = conn.prepareStatement("SELECT sql_text FROM decentdb_system_views WHERE name=?")) {
                ps.setString(1, "v_user_post_counts");
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        System.out.println("DDL for v_user_post_counts: " + rs.getString("sql_text"));
                    } else {
                        System.out.println("Nothing returned!");
                    }
                }
            }
        }
    }
}
