#[test]
fn debug_ast() {
    let raw = libpg_query_sys::parse_statement("SELECT 1 WHERE 1 NOT IN (1, 3)").unwrap();
    println!("{:#?}", raw);
}
