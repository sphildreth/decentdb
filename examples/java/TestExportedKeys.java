import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.DatabaseMetaData;

public class TestExportedKeys {
    public static void main(String[] args) throws Exception {
        Class.forName("com.decentdb.jdbc.DecentDBDriver");
        String dbPath = "jdbc:decentdb:../../demo.ddb";
        try (Connection conn = DriverManager.getConnection(dbPath)) {
            DatabaseMetaData metaData = conn.getMetaData();
            
            System.out.println("Exported Keys for 'users':");
            try (ResultSet rs = metaData.getExportedKeys(null, null, "users")) {
                while (rs.next()) {
                    String pkTable = rs.getString("PKTABLE_NAME");
                    String pkColumn = rs.getString("PKCOLUMN_NAME");
                    String fkTable = rs.getString("FKTABLE_NAME");
                    String fkColumn = rs.getString("FKCOLUMN_NAME");
                    System.out.println("  " + pkTable + "." + pkColumn + " -> " + fkTable + "." + fkColumn);
                }
            }
            
            System.out.println("Cross Reference users -> posts:");
            try (ResultSet rs = metaData.getCrossReference(null, null, "users", null, null, "posts")) {
                while (rs.next()) {
                    String pkTable = rs.getString("PKTABLE_NAME");
                    String pkColumn = rs.getString("PKCOLUMN_NAME");
                    String fkTable = rs.getString("FKTABLE_NAME");
                    String fkColumn = rs.getString("FKCOLUMN_NAME");
                    System.out.println("  " + pkTable + "." + pkColumn + " -> " + fkTable + "." + fkColumn);
                }
            }
        }
    }
}
