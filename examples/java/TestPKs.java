import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.DatabaseMetaData;

public class TestPKs {
    public static void main(String[] args) throws Exception {
        Class.forName("com.decentdb.jdbc.DecentDBDriver");
        String dbPath = "jdbc:decentdb:../../demo.ddb";
        try (Connection conn = DriverManager.getConnection(dbPath)) {
            DatabaseMetaData metaData = conn.getMetaData();
            
            System.out.println("Primary Keys for 'users':");
            try (ResultSet rs = metaData.getPrimaryKeys(null, null, "users")) {
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    String pkName = rs.getString("PK_NAME");
                    System.out.println("  " + columnName + " (" + pkName + ")");
                }
            }
            System.out.println("Primary Keys for 'posts':");
            try (ResultSet rs = metaData.getPrimaryKeys(null, null, "posts")) {
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    String pkName = rs.getString("PK_NAME");
                    System.out.println("  " + columnName + " (" + pkName + ")");
                }
            }
        }
    }
}
