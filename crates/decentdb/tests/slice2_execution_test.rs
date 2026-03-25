use decentdb::{Db, DbConfig};

#[test]
fn test_limit_all() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE t (id INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();

    match db.execute("SELECT * FROM t LIMIT ALL") {
        Ok(_) => println!("LIMIT ALL: OK"),
        Err(e) => println!("LIMIT ALL: Error: {}", e),
    }
}

#[test]
fn test_offset_fetch() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE t (id INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    match db.execute("SELECT * FROM t ORDER BY id OFFSET 1 ROW FETCH NEXT 2 ROWS ONLY") {
        Ok(_) => println!("OFFSET ... FETCH: OK"),
        Err(e) => println!("OFFSET ... FETCH: Error: {}", e),
    }
}

#[test]
fn test_distinct_on() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE t (a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (1, 20), (2, 30)")
        .unwrap();

    match db.execute("SELECT DISTINCT ON (a) a, b FROM t ORDER BY a") {
        Ok(_) => println!("DISTINCT ON: OK"),
        Err(e) => println!("DISTINCT ON: Error: {}", e),
    }
}
