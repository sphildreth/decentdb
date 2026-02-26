import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.DatabaseMetaData;

public class DecentDBJdbcExample {
    public static void main(String[] args) throws Exception {
        // Explicitly load the DecentDB JDBC driver class
        Class.forName("com.decentdb.jdbc.DecentDBDriver");
        
        // Define connection string (using a local database file)
        String dbPath = "jdbc:decentdb:java_example.ddb";
        System.out.println("Connecting to " + dbPath + "...");
        
        try (Connection conn = DriverManager.getConnection(dbPath)) {
            System.out.println("Connected successfully.\n");
            
            try (Statement stmt = conn.createStatement()) {
                // Cleanup from previous runs
                stmt.execute("DROP VIEW IF EXISTS v_active_users");
                stmt.execute("DROP TABLE IF EXISTS users");
                
                // 1. Create a table
                System.out.println("Creating table 'users'...");
                stmt.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, active BOOL)");
                
                // 2. Insert test data
                System.out.println("Inserting data into 'users'...");
                stmt.execute("INSERT INTO users VALUES (1, 'Alice', true)");
                stmt.execute("INSERT INTO users VALUES (2, 'Bob', false)");
                stmt.execute("INSERT INTO users VALUES (3, 'Charlie', true)");
                
                // 3. Create a view
                System.out.println("Creating view 'v_active_users' to filter only active users...");
                stmt.execute("CREATE VIEW v_active_users AS SELECT id, name FROM users WHERE active = true");
            }
            
            // 4. Showcase DatabaseMetaData getting tables AND views
            System.out.println("\n--- Fetching Tables and Views via DatabaseMetaData ---");
            DatabaseMetaData metaData = conn.getMetaData();
            
            // Query for both "TABLE" and "VIEW"
            String[] typesToRetrieve = new String[] { "TABLE", "VIEW" };
            
            try (ResultSet rs = metaData.getTables(null, null, "%", typesToRetrieve)) {
                while (rs.next()) {
                    String tableType = rs.getString("TABLE_TYPE");
                    String tableName = rs.getString("TABLE_NAME");
                    System.out.println(String.format("[%s] %s", tableType, tableName));
                    
                    // Display Columns for this Table/View using DatabaseMetaData.getColumns
                    try (ResultSet cols = metaData.getColumns(null, null, tableName, "%")) {
                        while (cols.next()) {
                            String colName = cols.getString("COLUMN_NAME");
                            String colType = cols.getString("TYPE_NAME");
                            System.out.println("    - " + colName + " (" + colType + ")");
                        }
                    }
                }
            }
            
            // 5. Query the view
            System.out.println("\n--- Querying 'v_active_users' View ---");
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM v_active_users ORDER BY id ASC")) {
                while (rs.next()) {
                    int id = rs.getInt("id");
                    String name = rs.getString("name");
                    System.out.println("User ID: " + id + " | Name: " + name);
                }
            }
        }
        
        System.out.println("\nExample completed successfully!");
    }
}
