#[test]
fn debug_slice2_features() {
    // Test LIMIT ALL
    println!("\n=== LIMIT ALL ===");
    match libpg_query_sys::parse_statement("SELECT * FROM t LIMIT ALL") {
        Ok(parsed) => println!("{:#?}", parsed),
        Err(e) => println!("Error: {}", e),
    }

    // Test OFFSET ... FETCH
    println!("\n=== OFFSET ... FETCH ===");
    match libpg_query_sys::parse_statement("SELECT * FROM t OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY")
    {
        Ok(parsed) => println!("{:#?}", parsed),
        Err(e) => println!("Error: {}", e),
    }

    // Test DISTINCT ON
    println!("\n=== DISTINCT ON ===");
    match libpg_query_sys::parse_statement("SELECT DISTINCT ON (a) a, b FROM t ORDER BY a") {
        Ok(parsed) => println!("{:#?}", parsed),
        Err(e) => println!("Error: {}", e),
    }
}
