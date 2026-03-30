use decentdb::{Db, DbConfig};
use tempfile::TempDir;

#[test]
fn test_create_table_self_ref() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Db::create(&db_path, DbConfig::default()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, parent_id INT, FOREIGN KEY (parent_id) REFERENCES t(id));").unwrap();
}

#[test]
fn test_create_table_self_ref_is_case_insensitive() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("case-insensitive-self-ref.db");
    let db = Db::create(&db_path, DbConfig::default()).unwrap();

    db.execute(
        "CREATE TABLE Categories (
            Id INT PRIMARY KEY,
            ParentCategoryId INT,
            FOREIGN KEY (ParentCategoryId) REFERENCES categories(Id)
        );",
    )
    .unwrap();
}
