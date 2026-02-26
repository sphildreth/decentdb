import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.DatabaseMetaData;

public class TestFKs {
    public static void main(String[] args) throws Exception {
        Class.forName("com.decentdb.jdbc.DecentDBDriver");
        String dbPath = "jdbc:decentdb:../../demo.ddb";
        try (Connection conn = DriverManager.getConnection(dbPath)) {
            DatabaseMetaData metaData = conn.getMetaData();
            
            System.out.println("Imported Keys for 'posts':");
            try (ResultSet rs = metaData.getImportedKeys(null, null, "posts")) {
                while (rs.next()) {
                    String pkTable = rs.getString("PKTABLE_NAME");
                    String pkColumn = rs.getString("PKCOLUMN_NAME");
                    String fkTable = rs.getString("FKTABLE_NAME");
                    String fkColumn = rs.getString("FKCOLUMN_NAME");
                    System.out.println("  " + fkTable + "." + fkColumn + " -> " + pkTable + "." + pkColumn);
                }
            }
            System.out.println("Imported Keys for 'post_tags':");
            try (ResultSet rs = metaData.getImportedKeys(null, null, "post_tags")) {
                while (rs.next()) {
                    String pkTable = rs.getString("PKTABLE_NAME");
                    String pkColumn = rs.getString("PKCOLUMN_NAME");
                    String fkTable = rs.getString("FKTABLE_NAME");
                    String fkColumn = rs.getString("FKCOLUMN_NAME");
                    System.out.println("  " + fkTable + "." + fkColumn + " -> " + pkTable + "." + pkColumn);
                }
            }
        }
    }
}
