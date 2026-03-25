use decentdb::Db;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

fn unique_path() -> String {
    format!(
        "/tmp/test_join_{}.db",
        NEXT_ID.fetch_add(1, Ordering::SeqCst)
    )
}

#[test]
fn debug_ast() {
    let raw = libpg_query_sys::parse_statement("SELECT 1 WHERE 1 NOT IN (1, 3)").unwrap();
    println!("{:#?}", raw);
}

#[test]
fn test_join_execution() {
    let path = unique_path();
    let db = Db::create(&path, Default::default()).expect("create db");
    db.execute("CREATE TABLE a (id INT64 PRIMARY KEY)")
        .expect("create a");
    db.execute("CREATE TABLE b (id INT64 PRIMARY KEY)")
        .expect("create b");
    db.execute("INSERT INTO a VALUES (1), (2)")
        .expect("insert a");
    db.execute("INSERT INTO b VALUES (1), (3)")
        .expect("insert b");

    let queries = vec![
        ("CROSS JOIN", "SELECT * FROM a CROSS JOIN b"),
        ("RIGHT JOIN", "SELECT * FROM a RIGHT JOIN b ON a.id = b.id"),
        (
            "FULL OUTER JOIN",
            "SELECT * FROM a FULL OUTER JOIN b ON a.id = b.id",
        ),
        ("NATURAL JOIN", "SELECT * FROM a NATURAL JOIN b"),
        ("INNER JOIN", "SELECT * FROM a INNER JOIN b ON a.id = b.id"),
        ("LEFT JOIN", "SELECT * FROM a LEFT JOIN b ON a.id = b.id"),
    ];

    for (name, q) in queries {
        match db.execute(q) {
            Ok(_result) => println!("✓ Executed: {} - {}", name, q),
            Err(e) => println!("✗ Failed: {} - {} - {}", name, q, e),
        }
    }

    std::fs::remove_file(&path).ok();
}
