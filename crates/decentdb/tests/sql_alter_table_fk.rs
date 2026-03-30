use decentdb::{Db, DbConfig};
use tempfile::TempDir;

#[test]
fn test_alter_table_add_fk() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Db::create(&db_path, DbConfig::default()).unwrap();
    
    db.execute("CREATE TABLE y (id INT PRIMARY KEY);").unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, x INT);").unwrap();
    db.execute("ALTER TABLE t ADD CONSTRAINT fk_t_y FOREIGN KEY (x) REFERENCES y(id);").unwrap();
    db.execute("ALTER TABLE t DROP CONSTRAINT fk_t_y;").unwrap();
}
