//! Unit tests for bulk load operations.

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
    fn test_bulk_load_basic() -> Result<()> {
        let (db, _temp) = create_test_db()?;

        db.execute("CREATE TABLE users (id INT64, name TEXT, age INT64)")?;

        let rows: Vec<Vec<Value>> = vec![
            vec![
                Value::Int64(1),
                Value::Text("Alice".to_string()),
                Value::Int64(30),
            ],
            vec![
                Value::Int64(2),
                Value::Text("Bob".to_string()),
                Value::Int64(25),
            ],
            vec![
                Value::Int64(3),
                Value::Text("Charlie".to_string()),
                Value::Int64(35),
            ],
        ];

        let options = crate::BulkLoadOptions::default();
        let affected = db.bulk_load_rows("users", &["id", "name", "age"], &rows, options)?;
        assert_eq!(affected, 3);

        let result = db.execute("SELECT * FROM users ORDER BY id")?;
        assert_eq!(result.rows().len(), 3);
        assert_eq!(
            result.rows()[0].values(),
            &[
                Value::Int64(1),
                Value::Text("Alice".to_string()),
                Value::Int64(30)
            ]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[
                Value::Int64(2),
                Value::Text("Bob".to_string()),
                Value::Int64(25)
            ]
        );
        assert_eq!(
            result.rows()[2].values(),
            &[
                Value::Int64(3),
                Value::Text("Charlie".to_string()),
                Value::Int64(35)
            ]
        );

        Ok(())
    }

    #[test]
    fn test_bulk_load_empty() -> Result<()> {
        let (db, _temp) = create_test_db()?;

        db.execute("CREATE TABLE empty_table (id INT64, value TEXT)")?;

        let rows: Vec<Vec<Value>> = vec![];
        let options = crate::BulkLoadOptions::default();
        let affected = db.bulk_load_rows("empty_table", &["id", "value"], &rows, options)?;
        assert_eq!(affected, 0);

        let result = db.execute("SELECT COUNT(*) FROM empty_table")?;
        assert_eq!(result.rows()[0].values(), &[Value::Int64(0)]);

        Ok(())
    }

    #[test]
    fn test_bulk_load_partial_columns() -> Result<()> {
        let (db, _temp) = create_test_db()?;

        db.execute("CREATE TABLE partial (id INT64, name TEXT, value TEXT)")?;

        let rows: Vec<Vec<Value>> = vec![
            vec![Value::Int64(1), Value::Text("first".to_string())],
            vec![Value::Int64(2), Value::Text("second".to_string())],
        ];

        let options = crate::BulkLoadOptions::default();
        let affected = db.bulk_load_rows("partial", &["id", "name"], &rows, options)?;
        assert_eq!(affected, 2);

        let result = db.execute("SELECT * FROM partial ORDER BY id")?;
        assert_eq!(result.rows().len(), 2);
        assert!(matches!(result.rows()[0].values()[2], Value::Null));
        assert!(matches!(result.rows()[1].values()[2], Value::Null));

        Ok(())
    }

    #[test]
    fn test_bulk_load_invalid_table() -> Result<()> {
        let (db, _temp) = create_test_db()?;

        let rows: Vec<Vec<Value>> = vec![vec![Value::Int64(1)]];
        let options = crate::BulkLoadOptions::default();
        let result = db.bulk_load_rows("nonexistent", &["id"], &rows, options);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unknown table"));

        Ok(())
    }

    #[test]
    fn test_bulk_load_view_not_allowed() -> Result<()> {
        let (db, _temp) = create_test_db()?;

        db.execute("CREATE TABLE base (id INT64)")?;
        db.execute("CREATE VIEW base_view AS SELECT id FROM base")?;

        let rows: Vec<Vec<Value>> = vec![vec![Value::Int64(1)]];
        let options = crate::BulkLoadOptions::default();
        let result = db.bulk_load_rows("base_view", &["id"], &rows, options);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("bulk load targets must be base tables"));

        Ok(())
    }

    #[test]
    fn test_bulk_load_column_count_mismatch() -> Result<()> {
        let (db, _temp) = create_test_db()?;

        db.execute("CREATE TABLE test (id INT64, name TEXT)")?;

        let rows: Vec<Vec<Value>> = vec![vec![Value::Int64(1)]];
        let options = crate::BulkLoadOptions::default();
        let result = db.bulk_load_rows("test", &["id", "name"], &rows, options);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_bulk_load_type_coercion() -> Result<()> {
        let (db, _temp) = create_test_db()?;
        const V1: f64 = 314.0 / 100.0;
        const V2: f64 = 271.0 / 100.0;

        db.execute("CREATE TABLE typed (id INT64, value FLOAT64)")?;

        let rows: Vec<Vec<Value>> = vec![
            vec![Value::Int64(1), Value::Float64(V1)],
            vec![Value::Int64(2), Value::Float64(V2)],
        ];

        let options = crate::BulkLoadOptions::default();
        let affected = db.bulk_load_rows("typed", &["id", "value"], &rows, options)?;
        assert_eq!(affected, 2);

        let result = db.execute("SELECT * FROM typed ORDER BY id")?;
        assert_eq!(result.rows().len(), 2);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(1), Value::Float64(V1)]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[Value::Int64(2), Value::Float64(V2)]
        );

        Ok(())
    }

    #[test]
    fn test_bulk_load_with_nulls() -> Result<()> {
        let (db, _temp) = create_test_db()?;

        db.execute("CREATE TABLE nullable (id INT64, value TEXT)")?;

        let rows: Vec<Vec<Value>> = vec![
            vec![Value::Int64(1), Value::Text("has value".to_string())],
            vec![Value::Int64(2), Value::Null],
            vec![Value::Int64(3), Value::Text("another".to_string())],
        ];

        let options = crate::BulkLoadOptions::default();
        let affected = db.bulk_load_rows("nullable", &["id", "value"], &rows, options)?;
        assert_eq!(affected, 3);

        let result = db.execute("SELECT * FROM nullable ORDER BY id")?;
        assert_eq!(result.rows().len(), 3);
        assert!(matches!(result.rows()[1].values()[1], Value::Null));

        Ok(())
    }

    #[test]
    fn test_bulk_load_custom_batch_size() -> Result<()> {
        let (db, _temp) = create_test_db()?;

        db.execute("CREATE TABLE batched (id INT64, value TEXT)")?;

        let rows: Vec<Vec<Value>> = (1..=100)
            .map(|i| vec![Value::Int64(i), Value::Text(format!("value{}", i))])
            .collect();

        let options = crate::BulkLoadOptions {
            batch_size: 10,
            sync_interval: 100,
            disable_indexes: false,
            checkpoint_on_complete: true,
        };
        let affected = db.bulk_load_rows("batched", &["id", "value"], &rows, options)?;
        assert_eq!(affected, 100);

        let result = db.execute("SELECT COUNT(*) FROM batched")?;
        assert_eq!(result.rows()[0].values(), &[Value::Int64(100)]);

        Ok(())
    }

    #[test]
    fn test_bulk_load_invalid_options() -> Result<()> {
        let (db, _temp) = create_test_db()?;

        db.execute("CREATE TABLE test (id INT64)")?;

        let rows: Vec<Vec<Value>> = vec![vec![Value::Int64(1)]];

        let options = crate::BulkLoadOptions {
            batch_size: 0,
            sync_interval: 100,
            disable_indexes: false,
            checkpoint_on_complete: true,
        };
        let result = db.bulk_load_rows("test", &["id"], &rows, options);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("batch_size and sync_interval must be greater than zero"));

        let options2 = crate::BulkLoadOptions {
            batch_size: 10,
            sync_interval: 0,
            disable_indexes: false,
            checkpoint_on_complete: true,
        };
        let result2 = db.bulk_load_rows("test", &["id"], &rows, options2);
        assert!(result2.is_err());

        Ok(())
    }

    #[test]
    fn test_bulk_load_preserves_order() -> Result<()> {
        let (db, _temp) = create_test_db()?;

        db.execute("CREATE TABLE ordered (id INT64, seq INT64)")?;

        let rows: Vec<Vec<Value>> = (1..=50)
            .map(|i| vec![Value::Int64(i), Value::Int64(i)])
            .collect();

        let options = crate::BulkLoadOptions::default();
        db.bulk_load_rows("ordered", &["id", "seq"], &rows, options)?;

        let result = db.execute("SELECT seq FROM ordered ORDER BY id")?;
        for (i, row) in result.rows().iter().enumerate() {
            assert_eq!(row.values()[0], Value::Int64((i + 1) as i64));
        }

        Ok(())
    }

    #[test]
    fn test_bulk_load_large_dataset() -> Result<()> {
        let (db, _temp) = create_test_db()?;

        db.execute("CREATE TABLE large (id INT64, value TEXT, score FLOAT64)")?;

        let rows: Vec<Vec<Value>> = (1..=1000)
            .map(|i| {
                vec![
                    Value::Int64(i as i64),
                    Value::Text(format!("item{}", i)),
                    Value::Float64(i as f64 * 0.1),
                ]
            })
            .collect();

        let options = crate::BulkLoadOptions {
            batch_size: 100,
            sync_interval: 500,
            disable_indexes: false,
            checkpoint_on_complete: true,
        };
        let affected = db.bulk_load_rows("large", &["id", "value", "score"], &rows, options)?;
        assert_eq!(affected, 1000);

        let result = db.execute("SELECT COUNT(*) FROM large")?;
        assert_eq!(result.rows()[0].values(), &[Value::Int64(1000)]);

        let sum_result = db.execute("SELECT SUM(score) FROM large")?;
        let sum_value = sum_result.rows()[0].values()[0].clone();
        assert!(matches!(sum_value, Value::Float64(_)));

        Ok(())
    }
}
