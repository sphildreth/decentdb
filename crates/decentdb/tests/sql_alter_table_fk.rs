use decentdb::{Db, DbConfig, Value};
use tempfile::TempDir;

#[test]
fn test_alter_table_add_fk() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Db::create(&db_path, DbConfig::default()).unwrap();

    db.execute("CREATE TABLE y (id INT PRIMARY KEY);").unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, x INT);")
        .unwrap();
    db.execute("ALTER TABLE t ADD CONSTRAINT fk_t_y FOREIGN KEY (x) REFERENCES y(id);")
        .unwrap();
    db.execute("ALTER TABLE t DROP CONSTRAINT fk_t_y;").unwrap();
}

#[test]
fn test_alter_table_add_fk_with_actions() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("fk-actions.db");
    let db = Db::create(&db_path, DbConfig::default()).unwrap();

    db.execute("CREATE TABLE y (id INT PRIMARY KEY);").unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, x INT);")
        .unwrap();
    db.execute(
        "ALTER TABLE t ADD CONSTRAINT fk_t_y FOREIGN KEY (x) REFERENCES y(id) ON DELETE SET NULL ON UPDATE CASCADE;",
    )
    .unwrap();

    db.execute("INSERT INTO y VALUES (1), (2);").unwrap();
    db.execute("INSERT INTO t VALUES (10, 1), (20, 2);")
        .unwrap();
    db.execute("UPDATE y SET id = 100 WHERE id = 1;").unwrap();

    let result = db.execute("SELECT x FROM t WHERE id = 10;").unwrap();
    assert_eq!(result.rows()[0].values()[0], Value::Int64(100));

    db.execute("DELETE FROM y WHERE id = 2;").unwrap();
    let result = db.execute("SELECT x FROM t WHERE id = 20;").unwrap();
    assert!(matches!(result.rows()[0].values()[0], Value::Null));
}

#[test]
fn test_alter_table_add_fk_with_restrict_actions() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("fk-restrict-actions.db");
    let db = Db::create(&db_path, DbConfig::default()).unwrap();

    db.execute("CREATE TABLE y (id INT PRIMARY KEY);").unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, x INT);")
        .unwrap();
    db.execute(
        "ALTER TABLE t ADD CONSTRAINT fk_t_y FOREIGN KEY (x) REFERENCES y(id) ON DELETE RESTRICT ON UPDATE RESTRICT;",
    )
    .unwrap();

    db.execute("INSERT INTO y VALUES (1);").unwrap();
    db.execute("INSERT INTO t VALUES (10, 1);").unwrap();

    let update_err = db.execute("UPDATE y SET id = 2 WHERE id = 1;").unwrap_err();
    assert!(
        update_err
            .to_string()
            .to_lowercase()
            .contains("foreign key"),
        "got: {update_err}"
    );

    let delete_err = db.execute("DELETE FROM y WHERE id = 1;").unwrap_err();
    assert!(
        delete_err
            .to_string()
            .to_lowercase()
            .contains("foreign key"),
        "got: {delete_err}"
    );
}
