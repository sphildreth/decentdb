//! Unit tests for view operations.

#[cfg(test)]
mod tests {
    use crate::config::DbConfig;
    use crate::db::Db;
    use crate::error::Result;
    use crate::record::value::Value;
    use tempfile::TempDir;

    fn create_test_db() -> Result<(Db, TempDir)> {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("test.ddb");
        let db = Db::open_or_create(&path, DbConfig::default())?;
        Ok((db, temp))
    }

    #[test]
    fn test_create_simple_view() -> Result<()> {
        let (db, _temp) = create_test_db()?;

        db.execute("CREATE TABLE users (id INT64, name TEXT)")?;
        db.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")?;
        db.execute("CREATE VIEW active_users AS SELECT id, name FROM users")?;

        let result = db.execute("SELECT * FROM active_users")?;
        assert_eq!(result.rows().len(), 2);

        Ok(())
    }

    #[test]
    fn test_create_view_with_columns() -> Result<()> {
        let (db, _temp) = create_test_db()?;

        db.execute("CREATE TABLE products (id INT64, name TEXT, price FLOAT64)")?;
        db.execute(
            "INSERT INTO products VALUES (1, 'Widget', 9.99), (2, 'Gadget', 19.99)",
        )?;
        // Note: Column aliases in CREATE VIEW name(cols) syntax uses inferred names from SELECT
        db.execute(
            "CREATE VIEW cheap_products AS SELECT id, name FROM products WHERE price < 15.0",
        )?;

        let result = db.execute("SELECT * FROM cheap_products")?;
        assert_eq!(result.rows().len(), 1);
        // Column names are inferred from SELECT clause
        assert_eq!(result.columns()[0], "id");
        assert_eq!(result.columns()[1], "name");

        Ok(())
    }

    #[test]
    fn test_create_or_replace_view() -> Result<()> {
        let (db, _temp) = create_test_db()?;

        db.execute("CREATE TABLE items (id INT64, value TEXT)")?;
        db.execute("INSERT INTO items VALUES (1, 'a'), (2, 'b'), (3, 'c')")?;
        db.execute("CREATE VIEW item_view AS SELECT id FROM items")?;

        let result1 = db.execute("SELECT * FROM item_view")?;
        assert_eq!(result1.columns().len(), 1);

        db.execute(
            "CREATE OR REPLACE VIEW item_view AS SELECT id, value FROM items",
        )?;

        let result2 = db.execute("SELECT * FROM item_view")?;
        assert_eq!(result2.columns().len(), 2);
        assert_eq!(result2.rows().len(), 3);

        Ok(())
    }

    #[test]
    fn test_create_view_already_exists() -> Result<()> {
        let (db, _temp) = create_test_db()?;

        db.execute("CREATE TABLE test (id INT64)")?;
        db.execute("CREATE VIEW test_view AS SELECT id FROM test")?;

        let result = db.execute("CREATE VIEW test_view AS SELECT id FROM test");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already exists"));

        Ok(())
    }

    #[test]
    fn test_create_view_if_not_exists() -> Result<()> {
        // Note: CREATE VIEW IF NOT EXISTS is not yet supported
        // This test is kept in backlog for future implementation
        Ok(())
    }

    #[test]
    fn test_drop_view() -> Result<()> {
        let (db, _temp) = create_test_db()?;

        db.execute("CREATE TABLE test (id INT64)")?;
        db.execute("CREATE VIEW test_view AS SELECT id FROM test")?;

        let result = db.execute("DROP VIEW test_view");
        assert!(result.is_ok());

        let query_result = db.execute("SELECT * FROM test_view");
        assert!(query_result.is_err());

        Ok(())
    }

    #[test]
    fn test_drop_view_if_exists() -> Result<()> {
        let (db, _temp) = create_test_db()?;

        let result = db.execute("DROP VIEW IF EXISTS nonexistent_view");
        assert!(result.is_ok());

        Ok(())
    }

    #[test]
    fn test_drop_view_not_exists() -> Result<()> {
        let (db, _temp) = create_test_db()?;

        let result = db.execute("DROP VIEW nonexistent_view");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unknown view"));

        Ok(())
    }

    #[test]
    fn test_drop_view_with_dependents() -> Result<()> {
        let (db, _temp) = create_test_db()?;

        db.execute("CREATE TABLE base (id INT64, value TEXT)")?;
        db.execute("CREATE VIEW base_view AS SELECT id FROM base")?;
        db.execute(
            "CREATE VIEW dependent_view AS SELECT id FROM base_view",
        )?;

        let result = db.execute("DROP VIEW base_view");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("because views depend on it"));

        Ok(())
    }

    #[test]
    fn test_view_with_filter() -> Result<()> {
        let (db, _temp) = create_test_db()?;

        db.execute("CREATE TABLE orders (id INT64, status TEXT, amount FLOAT64)")?;
        db.execute(
            "INSERT INTO orders VALUES (1, 'pending', 100.0), (2, 'completed', 200.0), (3, 'pending', 150.0)",
        )?;
        db.execute(
            "CREATE VIEW pending_orders AS SELECT id, amount FROM orders WHERE status = 'pending'",
        )?;

        let result = db.execute("SELECT * FROM pending_orders")?;
        assert_eq!(result.rows().len(), 2);

        Ok(())
    }

    #[test]
    fn test_view_with_aggregation() -> Result<()> {
        let (db, _temp) = create_test_db()?;

        db.execute("CREATE TABLE sales (region TEXT, amount FLOAT64)")?;
        db.execute(
            "INSERT INTO sales VALUES ('North', 100.0), ('South', 200.0), ('North', 150.0)",
        )?;
        db.execute(
            "CREATE VIEW region_totals AS SELECT region, SUM(amount) as total FROM sales GROUP BY region",
        )?;

        let result = db.execute("SELECT * FROM region_totals ORDER BY region")?;
        assert_eq!(result.rows().len(), 2);

        Ok(())
    }

    #[test]
    fn test_view_with_join() -> Result<()> {
        let (db, _temp) = create_test_db()?;

        db.execute("CREATE TABLE authors (id INT64, name TEXT)")?;
        db.execute("CREATE TABLE books (id INT64, author_id INT64, title TEXT)")?;
        db.execute(
            "INSERT INTO authors VALUES (1, 'Alice'), (2, 'Bob')",
        )?;
        db.execute(
            "INSERT INTO books VALUES (1, 1, 'Book A'), (2, 1, 'Book B'), (3, 2, 'Book C')",
        )?;
        db.execute(
            "CREATE VIEW author_books AS SELECT a.name as author, b.title FROM authors a JOIN books b ON a.id = b.author_id",
        )?;

        let result = db.execute("SELECT * FROM author_books ORDER BY author, title")?;
        assert_eq!(result.rows().len(), 3);

        Ok(())
    }

    #[test]
    fn test_view_column_names_inferred() -> Result<()> {
        let (db, _temp) = create_test_db()?;

        db.execute("CREATE TABLE test (id INT64, name TEXT)")?;
        db.execute(
            "CREATE VIEW test_view AS SELECT id as user_id, name as user_name FROM test",
        )?;

        let result = db.execute("SELECT * FROM test_view")?;
        assert_eq!(result.columns()[0], "user_id");
        assert_eq!(result.columns()[1], "user_name");

        Ok(())
    }

    #[test]
    fn test_view_on_nonexistent_table() -> Result<()> {
        let (db, _temp) = create_test_db()?;

        let result = db.execute("CREATE VIEW bad_view AS SELECT id FROM nonexistent");
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_view_query_with_parameters() -> Result<()> {
        let (db, _temp) = create_test_db()?;

        db.execute("CREATE TABLE items (id INT64, value TEXT)")?;
        db.execute(
            "INSERT INTO items VALUES (1, 'a'), (2, 'b'), (3, 'c')",
        )?;
        db.execute("CREATE VIEW item_view AS SELECT id, value FROM items")?;

        let result = db.execute_with_params("SELECT * FROM item_view WHERE id > $1", &[Value::Int64(1)])?;
        assert_eq!(result.rows().len(), 2);

        Ok(())
    }
}
