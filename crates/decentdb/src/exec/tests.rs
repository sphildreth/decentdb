use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::record::compression::CompressionMode;
use crate::search::TrigramQueryResult;
use crate::sql::ast::{Expr, FromItem};
use crate::sql::parser::parse_sql_statement;
use crate::storage::checksum::crc32c_parts;
use crate::storage::page::InMemoryPageStore;
use crate::{Db, DbConfig, Value};

use super::{
    append_paged_table_chunks, decode_manifest_payload, decode_paged_table_manifest_payload,
    decode_runtime_payload, drop_index_include_columns_section,
    encode_legacy_table_payload_from_manifest, encode_manifest_payload, encode_paged_table_chunks,
    encode_paged_table_chunks_from_rows, encode_runtime_payload, encode_table_payload, like_match,
    persist_paged_table, read_deferred_row_by_id_from_table_payload,
    read_table_page_manifest_from_state, rewrite_paged_table_from_resident, simple_trigram_lookup,
    try_append_only_paged_table_from_manifest, ColumnBinding, Dataset, DbTxnPageStore,
    EngineRuntime, OverflowPointer, PersistedTableState, RuntimeBtreeKeys, RuntimeIndex, StoredRow,
    TableData, TablePageManifest, TablePageManifestChunk, TableRowSource,
};

const PAGE_SIZE: u32 = 4096;

#[test]
fn like_match_fast_patterns_match_recursive_semantics() {
    assert!(like_match("Motley Crue", "%Motley%", false, None));
    assert!(like_match("Motley Crue", "Motley%", false, None));
    assert!(like_match("The Motley", "%Motley", false, None));
    assert!(like_match("Motley", "Motley", false, None));
    assert!(!like_match("motley", "%Motley%", false, None));
    assert!(like_match("motley", "%Motley%", true, None));
    assert!(like_match("M_tley", "M$_tley", false, Some('$')));
    assert!(like_match("", "", false, None));
    assert!(!like_match("x", "", false, None));
}

#[test]
fn simple_expression_projection_fast_path_handles_wildcard_like_order_limit() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE artists (id INT64 PRIMARY KEY, name TEXT)",
    );
    execute_sql(&mut runtime, "INSERT INTO artists VALUES (1, 'Motley B')");
    execute_sql(&mut runtime, "INSERT INTO artists VALUES (2, 'Other')");
    execute_sql(&mut runtime, "INSERT INTO artists VALUES (3, 'Motley A')");

    let statement = parse_sql_statement(
        "SELECT * FROM artists WHERE name LIKE '%Motley%' ORDER BY name LIMIT 1",
    )
    .expect("parse");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query");
    };
    let result = runtime
        .try_execute_simple_expression_projection_query(query, &[])
        .expect("execute")
        .expect("wildcard LIKE query should use streaming expression projection path");

    assert_eq!(result.columns(), &["id".to_string(), "name".to_string()]);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(3), Value::Text("Motley A".to_string())]
    );
}

#[test]
fn trigram_candidate_lookup_handles_like_wildcards() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE docs (id INT64 PRIMARY KEY, body TEXT)",
    );
    execute_sql(&mut runtime, "INSERT INTO docs VALUES (1, 'Motley Crue')");
    execute_sql(&mut runtime, "INSERT INTO docs VALUES (2, 'Other Motley')");
    execute_sql(&mut runtime, "INSERT INTO docs VALUES (3, 'Unrelated')");
    execute_sql(
        &mut runtime,
        "CREATE INDEX docs_body_trgm ON docs USING gin (body)",
    );

    let statement =
        parse_sql_statement("SELECT * FROM docs WHERE body LIKE '%Motley%' ORDER BY body LIMIT 10")
            .expect("parse");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query");
    };
    let crate::sql::ast::QueryBody::Select(select) = &query.body else {
        panic!("expected select");
    };
    let row_ids = runtime
        .trigram_candidate_row_ids_for_filter(
            "docs",
            &None,
            select.filter.as_ref().expect("filter"),
            &[],
            &BTreeMap::new(),
        )
        .expect("lookup")
        .expect("trigram index should produce candidates");
    assert_eq!(row_ids, vec![1, 2]);

    let result = runtime
        .try_execute_simple_expression_projection_query(query, &[])
        .expect("execute")
        .expect("wildcard LIKE query should use fast projection path");
    assert_eq!(
        result
            .rows()
            .iter()
            .map(|row| row.values()[0].clone())
            .collect::<Vec<_>>(),
        vec![Value::Int64(1), Value::Int64(2)]
    );
}

#[test]
fn simple_trigram_lookup_rejects_non_candidate_safe_filters() {
    fn lookup_has_additional_filter(sql: &str) -> Option<bool> {
        let statement = parse_sql_statement(sql).expect("parse");
        let crate::sql::ast::Statement::Query(query) = &statement else {
            panic!("expected query");
        };
        let crate::sql::ast::QueryBody::Select(select) = &query.body else {
            panic!("expected select");
        };
        simple_trigram_lookup(select.filter.as_ref().expect("filter"))
            .map(|lookup| lookup.has_additional_filter)
    }

    assert!(
        lookup_has_additional_filter("SELECT * FROM docs WHERE body NOT LIKE '%Motley%'").is_none()
    );
    assert!(lookup_has_additional_filter(
        "SELECT * FROM docs WHERE body LIKE '%Motley%' OR id = 1"
    )
    .is_none());
    assert_eq!(
        lookup_has_additional_filter("SELECT * FROM docs WHERE body LIKE '%Motley%' AND id > 0"),
        Some(true)
    );
}

#[test]
fn runtime_clone_preserves_btree_and_trigram_indexes() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE docs (id INT64 PRIMARY KEY, email TEXT, body TEXT)",
    );
    execute_sql(&mut runtime, "CREATE INDEX docs_email_idx ON docs (email)");
    execute_sql(
        &mut runtime,
        "CREATE INDEX docs_body_trgm_idx ON docs USING gin (body)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO docs (id, email, body) VALUES (1, 'a@example.com', 'alphabet soup')",
    );

    let cloned = runtime.clone();

    let RuntimeIndex::Btree { keys, .. } = cloned
        .index("docs_email_idx")
        .expect("email index should be cloned")
    else {
        panic!("expected BTREE runtime index");
    };
    assert!(!keys.is_empty(), "btree entries should be preserved");

    let RuntimeIndex::Trigram { index } = cloned
        .index("docs_body_trgm_idx")
        .expect("trigram index should be cloned")
    else {
        panic!("expected trigram runtime index");
    };
    assert_eq!(
        index
            .query_candidates("alpha", false)
            .expect("query cloned index"),
        TrigramQueryResult::Candidates(vec![1])
    );
}

#[test]
fn non_nullable_int64_btree_indexes_use_typed_runtime_keys() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE docs (id INT64 PRIMARY KEY, email TEXT)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO docs (id, email) VALUES (7, 'a@example.com')",
    );

    let index_name = runtime
        .catalog
        .indexes
        .keys()
        .next()
        .cloned()
        .expect("primary-key index should exist");

    let RuntimeIndex::Btree { keys, .. } = runtime
        .index(&index_name)
        .expect("INT64 index should exist")
    else {
        panic!("expected BTREE runtime index");
    };

    let RuntimeBtreeKeys::UniqueInt64(entries) = keys else {
        panic!("expected typed INT64 runtime keys");
    };
    assert_eq!(entries.get(&7), Some(&7));
    assert_eq!(
        keys.row_ids_for_value(&Value::Int64(7))
            .expect("INT64 lookup should succeed"),
        vec![7]
    );
    assert!(keys
        .row_ids_for_value(&Value::Text("7".into()))
        .expect("mismatched lookup should not fail")
        .is_empty());
}

#[test]
fn simple_grouped_wrapped_having_uses_numeric_fast_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE seeded (id INT64 PRIMARY KEY, grp INT64, word TEXT)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO seeded (id, grp, word) VALUES (0, 0, 'beta')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO seeded (id, grp, word) VALUES (1, 0, 'alpha')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO seeded (id, grp, word) VALUES (2, 1, 'gamma')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO seeded (id, grp, word) VALUES (3, 1, 'delta')",
    );

    let statement = parse_sql_statement(
        "SELECT grp, UPPER(MIN(word)) AS upper_min FROM seeded \
             GROUP BY grp HAVING UPPER(MIN(word)) >= 'DELTA' \
             ORDER BY upper_min DESC",
    )
    .expect("parse grouped wrapped having");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_simple_grouped_numeric_aggregate_query(query, &[])
        .expect("execute grouped wrapped having")
        .expect("wrapped grouped query should stay on fast path");

    assert_eq!(
        result.columns(),
        &["grp".to_string(), "upper_min".to_string()]
    );
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(1), Value::Text("DELTA".to_string())]
    );
}

#[test]
fn simple_grouped_wrapped_count_having_uses_count_fast_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE seeded (id INT64 PRIMARY KEY, grp INT64)",
    );
    execute_sql(&mut runtime, "INSERT INTO seeded (id, grp) VALUES (0, 0)");
    execute_sql(&mut runtime, "INSERT INTO seeded (id, grp) VALUES (1, 0)");
    execute_sql(&mut runtime, "INSERT INTO seeded (id, grp) VALUES (2, 1)");

    let statement = parse_sql_statement(
        "SELECT grp, COUNT(*) + 1 AS cnt FROM seeded \
             GROUP BY grp HAVING COUNT(*) + 1 >= 3 \
             ORDER BY cnt DESC",
    )
    .expect("parse grouped wrapped count");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_simple_grouped_count_query(query, &[])
        .expect("execute grouped wrapped count")
        .expect("wrapped grouped count should stay on fast path");

    assert_eq!(result.columns(), &["grp".to_string(), "cnt".to_string()]);
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(0), Value::Int64(3)]
    );
}

#[test]
fn simple_grouped_numeric_multi_order_by_uses_fast_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE seeded (id INT64 PRIMARY KEY, grp INT64, n INT64)",
    );
    for (id, grp, n) in [(0, 0, 1), (1, 0, 4), (2, 1, 2), (3, 1, 3)] {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO seeded (id, grp, n) VALUES ({id}, {grp}, {n})"),
        );
    }

    let statement = parse_sql_statement(
        "SELECT grp, SUM(n) AS total, AVG(n) AS avg FROM seeded \
             GROUP BY grp HAVING SUM(n) >= 3 \
             ORDER BY total DESC, grp ASC",
    )
    .expect("parse grouped numeric");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_simple_grouped_numeric_aggregate_query(query, &[])
        .expect("execute grouped numeric")
        .expect("grouped numeric query should stay on fast path");

    assert_eq!(
        result.columns(),
        &["grp".to_string(), "total".to_string(), "avg".to_string()]
    );
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(0), Value::Int64(5), Value::Float64(2.5)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(1), Value::Int64(5), Value::Float64(2.5)]
    );
}

#[test]
fn simple_grouped_numeric_distinct_uses_fast_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE seeded (id INT64 PRIMARY KEY, grp INT64, n INT64)",
    );
    for (id, grp, n) in [
        (0, 0, 1),
        (1, 0, 1),
        (2, 0, 2),
        (3, 1, 2),
        (4, 1, 3),
        (5, 1, 3),
    ] {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO seeded (id, grp, n) VALUES ({id}, {grp}, {n})"),
        );
    }

    let statement = parse_sql_statement(
        "SELECT grp, SUM(DISTINCT n) AS total, AVG(DISTINCT n + 1) AS shifted_avg \
             FROM seeded GROUP BY grp HAVING SUM(DISTINCT n) >= 3 \
             ORDER BY total DESC, grp ASC",
    )
    .expect("parse grouped numeric distinct");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_simple_grouped_numeric_aggregate_query(query, &[])
        .expect("execute grouped numeric distinct")
        .expect("grouped numeric distinct should stay on fast path");

    assert_eq!(
        result.columns(),
        &[
            "grp".to_string(),
            "total".to_string(),
            "shifted_avg".to_string()
        ]
    );
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(1), Value::Int64(5), Value::Float64(3.5)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(0), Value::Int64(3), Value::Float64(2.5)]
    );
}

#[test]
fn simple_grouped_wrapped_count_multi_order_by_uses_fast_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE seeded (id INT64 PRIMARY KEY, grp INT64)",
    );
    execute_sql(&mut runtime, "INSERT INTO seeded (id, grp) VALUES (0, 1)");
    execute_sql(&mut runtime, "INSERT INTO seeded (id, grp) VALUES (1, 1)");
    execute_sql(&mut runtime, "INSERT INTO seeded (id, grp) VALUES (2, 0)");
    execute_sql(&mut runtime, "INSERT INTO seeded (id, grp) VALUES (3, 0)");

    let statement = parse_sql_statement(
        "SELECT grp, COUNT(*) + 1 AS cnt FROM seeded \
             GROUP BY grp HAVING COUNT(*) + 1 >= 3 \
             ORDER BY cnt DESC, grp ASC",
    )
    .expect("parse grouped wrapped count");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_simple_grouped_count_query(query, &[])
        .expect("execute grouped wrapped count")
        .expect("wrapped grouped count should stay on fast path");

    assert_eq!(result.columns(), &["grp".to_string(), "cnt".to_string()]);
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(0), Value::Int64(3)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(1), Value::Int64(3)]
    );
}

#[test]
fn simple_grouped_count_uses_runtime_btree_index_cardinality() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE issues (id INT64 PRIMARY KEY, status TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX idx_issues_status ON issues (status)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO issues (id, status) VALUES (1, 'open')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO issues (id, status) VALUES (2, 'closed')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO issues (id, status) VALUES (3, 'open')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO issues (id, status) VALUES (4, 'resolved')",
    );

    let statement =
        parse_sql_statement("SELECT status, COUNT(*) FROM issues GROUP BY status ORDER BY status")
            .expect("parse grouped count");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_simple_grouped_count_query(query, &[])
        .expect("execute grouped count")
        .expect("grouped count should stay on fast path");

    assert_eq!(
        result.columns(),
        &["status".to_string(), "col2".to_string()]
    );
    assert_eq!(
        result
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Text("closed".to_string()), Value::Int64(1)],
            vec![Value::Text("open".to_string()), Value::Int64(2)],
            vec![Value::Text("resolved".to_string()), Value::Int64(1)],
        ]
    );
}

#[test]
fn simple_grouped_count_uses_runtime_btree_index_cardinality_without_row_source() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE issues (id INT64 PRIMARY KEY, status TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX idx_issues_status ON issues (status)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO issues (id, status) VALUES (1, 'open')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO issues (id, status) VALUES (2, 'closed')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO issues (id, status) VALUES (3, 'open')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO issues (id, status) VALUES (4, 'resolved')",
    );

    let statement =
        parse_sql_statement("SELECT status, COUNT(*) FROM issues GROUP BY status ORDER BY status")
            .expect("parse grouped count");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };
    let Some(plan) = runtime
        .analyze_simple_grouped_count_query(query, &[])
        .expect("analyze grouped count")
    else {
        panic!("expected grouped count plan");
    };

    let result = runtime
        .try_simple_grouped_count_result_from_runtime_index(None, &plan, &[])
        .expect("execute grouped count via runtime index")
        .expect("grouped count should stay on runtime-index path without row source");

    assert_eq!(
        result
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Text("closed".to_string()), Value::Int64(1)],
            vec![Value::Text("open".to_string()), Value::Int64(2)],
            vec![Value::Text("resolved".to_string()), Value::Int64(1)],
        ]
    );
}

#[test]
fn simple_grouped_count_uses_runtime_btree_unique_index_cardinality() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE issues (id INT64 PRIMARY KEY, status TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE UNIQUE INDEX idx_issues_status ON issues (status)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO issues (id, status) VALUES (1, 'open')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO issues (id, status) VALUES (2, 'closed')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO issues (id, status) VALUES (3, 'resolved')",
    );

    let statement =
        parse_sql_statement("SELECT status, COUNT(*) FROM issues GROUP BY status ORDER BY status")
            .expect("parse grouped count");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_simple_grouped_count_query(query, &[])
        .expect("execute grouped count")
        .expect("grouped count should stay on fast path");

    assert_eq!(
        result.columns(),
        &["status".to_string(), "col2".to_string()]
    );
    assert_eq!(
        result
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Text("closed".to_string()), Value::Int64(1)],
            vec![Value::Text("open".to_string()), Value::Int64(1)],
            vec![Value::Text("resolved".to_string()), Value::Int64(1)],
        ]
    );
}

#[test]
fn left_join_status_aggregate_uses_index_fast_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE projects (id INT64 PRIMARY KEY, name TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE issues (id INT64 PRIMARY KEY, project_id INT64, status TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX idx_issues_project ON issues (project_id)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO projects (id, name) VALUES (1, 'Alpha')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO projects (id, name) VALUES (2, 'Beta')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO projects (id, name) VALUES (3, 'Empty')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO issues (id, project_id, status) VALUES (10, 1, 'open')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO issues (id, project_id, status) VALUES (11, 1, 'in_progress')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO issues (id, project_id, status) VALUES (20, 2, 'resolved')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO issues (id, project_id, status) VALUES (21, 2, 'closed')",
    );

    let statement = parse_sql_statement(
        "SELECT p.id, p.name,
                SUM(CASE WHEN i.status = 'open' THEN 1 ELSE 0 END),
                SUM(CASE WHEN i.status = 'in_progress' THEN 1 ELSE 0 END),
                SUM(CASE WHEN i.status = 'resolved' THEN 1 ELSE 0 END),
                SUM(CASE WHEN i.status = 'closed' THEN 1 ELSE 0 END),
                COUNT(i.id)
         FROM projects p
         LEFT JOIN issues i ON i.project_id = p.id
         GROUP BY p.id, p.name
         ORDER BY COUNT(i.id) DESC, p.id",
    )
    .expect("parse project status report");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_left_join_status_aggregate_query(query, &[])
        .expect("execute project status report")
        .expect("project status report should stay on fast path");

    assert_eq!(
        result
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![
                Value::Int64(1),
                Value::Text("Alpha".to_string()),
                Value::Int64(1),
                Value::Int64(1),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(2),
            ],
            vec![
                Value::Int64(2),
                Value::Text("Beta".to_string()),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(1),
                Value::Int64(1),
                Value::Int64(2),
            ],
            vec![
                Value::Int64(3),
                Value::Text("Empty".to_string()),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(0),
            ],
        ]
    );
}

#[test]
fn simple_grouped_wrapped_group_projection_uses_count_fast_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE seeded (id INT64 PRIMARY KEY, n INT64)",
    );
    for id in 0..6 {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO seeded (id, n) VALUES ({id}, {id})"),
        );
    }

    let statement = parse_sql_statement(
        "SELECT n / 2 + 1 AS bucket, COUNT(*) AS c FROM seeded \
             GROUP BY n / 2 ORDER BY bucket DESC",
    )
    .expect("parse grouped wrapped group count");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_simple_grouped_count_query(query, &[])
        .expect("execute grouped wrapped group count")
        .expect("wrapped grouped count should stay on fast path");

    assert_eq!(result.columns(), &["bucket".to_string(), "c".to_string()]);
    assert_eq!(result.rows().len(), 3);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(3), Value::Int64(2)]
    );
    assert_eq!(
        result.rows()[2].values(),
        &[Value::Int64(1), Value::Int64(2)]
    );
}

#[test]
fn simple_grouped_multiple_count_rows_use_numeric_fast_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE seeded (id INT64 PRIMARY KEY, grp INT64)",
    );
    for (id, grp) in [(0, 0), (1, 0), (2, 1)] {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO seeded (id, grp) VALUES ({id}, {grp})"),
        );
    }

    let statement = parse_sql_statement(
        "SELECT grp, COUNT(*) AS c, COUNT(*) + 1 AS c_plus_one \
             FROM seeded GROUP BY grp ORDER BY grp ASC",
    )
    .expect("parse grouped repeated count");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_simple_grouped_numeric_aggregate_query(query, &[])
        .expect("execute grouped repeated count")
        .expect("grouped repeated count should stay on numeric fast path");

    assert_eq!(
        result.columns(),
        &["grp".to_string(), "c".to_string(), "c_plus_one".to_string()]
    );
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(0), Value::Int64(2), Value::Int64(3)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(1), Value::Int64(1), Value::Int64(2)]
    );
}

#[test]
fn simple_grouped_numeric_multi_column_aggregates_use_fast_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE seeded (id INT64 PRIMARY KEY, grp INT64, n INT64, m INT64)",
    );
    for (id, grp, n, m) in [(0, 0, 2, 10), (1, 0, 4, 20), (2, 1, 6, 30), (3, 1, 8, 50)] {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO seeded (id, grp, n, m) VALUES ({id}, {grp}, {n}, {m})"),
        );
    }

    let statement = parse_sql_statement(
        "SELECT grp, SUM(n) AS total_n, AVG(m) AS avg_m \
             FROM seeded GROUP BY grp ORDER BY grp ASC",
    )
    .expect("parse grouped multi-column numeric aggregate");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_simple_grouped_numeric_aggregate_query(query, &[])
        .expect("execute grouped multi-column numeric aggregate")
        .expect("grouped multi-column numeric aggregate should stay on fast path");

    assert_eq!(
        result.columns(),
        &[
            "grp".to_string(),
            "total_n".to_string(),
            "avg_m".to_string()
        ]
    );
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(0), Value::Int64(6), Value::Float64(15.0)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(1), Value::Int64(14), Value::Float64(40.0)]
    );
}

#[test]
fn simple_grouped_wrapped_group_projection_uses_numeric_fast_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE seeded (id INT64 PRIMARY KEY, grp INT64, n INT64)",
    );
    for (id, grp, n) in [(0, 0, 1), (1, 0, 4), (2, 1, 2), (3, 1, 3)] {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO seeded (id, grp, n) VALUES ({id}, {grp}, {n})"),
        );
    }

    let statement = parse_sql_statement(
        "SELECT grp + 1 AS next_grp, SUM(n) AS total FROM seeded \
             GROUP BY grp ORDER BY next_grp DESC",
    )
    .expect("parse grouped wrapped group numeric");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_simple_grouped_numeric_aggregate_query(query, &[])
        .expect("execute grouped wrapped group numeric")
        .expect("wrapped grouped numeric query should stay on fast path");

    assert_eq!(
        result.columns(),
        &["next_grp".to_string(), "total".to_string()]
    );
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(2), Value::Int64(5)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(1), Value::Int64(5)]
    );
}

#[test]
fn simple_grouped_numeric_expression_aggregates_use_fast_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE seeded (id INT64 PRIMARY KEY, grp INT64, n INT64, m INT64)",
    );
    for (id, grp, n, m) in [(0, 0, 2, 10), (1, 0, 4, 20), (2, 1, 6, 30), (3, 1, 8, 50)] {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO seeded (id, grp, n, m) VALUES ({id}, {grp}, {n}, {m})"),
        );
    }

    let statement = parse_sql_statement(
        "SELECT grp, SUM(n + m) AS total, AVG(m - n) AS avg_delta \
             FROM seeded GROUP BY grp HAVING SUM(n + m) >= 30 \
             ORDER BY total DESC, grp ASC",
    )
    .expect("parse grouped numeric expression aggregate");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_simple_grouped_numeric_aggregate_query(query, &[])
        .expect("execute grouped numeric expression aggregate")
        .expect("grouped numeric expression aggregate should stay on fast path");

    assert_eq!(
        result.columns(),
        &[
            "grp".to_string(),
            "total".to_string(),
            "avg_delta".to_string()
        ]
    );
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(1), Value::Int64(94), Value::Float64(33.0)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(0), Value::Int64(36), Value::Float64(12.0)]
    );
}

#[test]
fn simple_grouped_count_expr_uses_numeric_fast_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE seeded (id INT64 PRIMARY KEY, grp INT64, n INT64)",
    );
    for (id, grp, n) in [(0, 0, "1"), (1, 0, "NULL"), (2, 1, "5"), (3, 1, "7")] {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO seeded (id, grp, n) VALUES ({id}, {grp}, {n})"),
        );
    }

    let statement = parse_sql_statement(
        "SELECT grp, COUNT(n) AS present, COUNT(n + 1) AS shifted \
             FROM seeded GROUP BY grp HAVING COUNT(n) >= 1 \
             ORDER BY present DESC, grp ASC",
    )
    .expect("parse grouped count expr");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_simple_grouped_numeric_aggregate_query(query, &[])
        .expect("execute grouped count expr")
        .expect("grouped count expr should stay on fast path");

    assert_eq!(
        result.columns(),
        &[
            "grp".to_string(),
            "present".to_string(),
            "shifted".to_string()
        ]
    );
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(1), Value::Int64(2), Value::Int64(2)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(0), Value::Int64(1), Value::Int64(1)]
    );
}

#[test]
fn simple_grouped_count_distinct_uses_numeric_fast_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE seeded (id INT64 PRIMARY KEY, grp INT64, n INT64)",
    );
    for (id, grp, n) in [
        (0, 0, "1"),
        (1, 0, "1"),
        (2, 0, "NULL"),
        (3, 1, "2"),
        (4, 1, "3"),
        (5, 1, "3"),
    ] {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO seeded (id, grp, n) VALUES ({id}, {grp}, {n})"),
        );
    }

    let statement = parse_sql_statement(
        "SELECT grp, COUNT(DISTINCT n) AS uniq, COUNT(DISTINCT n + 1) AS shifted \
             FROM seeded GROUP BY grp HAVING COUNT(DISTINCT n) >= 1 \
             ORDER BY uniq DESC, grp ASC",
    )
    .expect("parse grouped count distinct");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_simple_grouped_numeric_aggregate_query(query, &[])
        .expect("execute grouped count distinct")
        .expect("grouped count distinct should stay on fast path");

    assert_eq!(
        result.columns(),
        &["grp".to_string(), "uniq".to_string(), "shifted".to_string()]
    );
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(1), Value::Int64(2), Value::Int64(2)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(0), Value::Int64(1), Value::Int64(1)]
    );
}

#[test]
fn simple_grouped_total_variance_bool_use_numeric_fast_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE seeded (id INT64 PRIMARY KEY, grp INT64, n INT64, flag BOOLEAN)",
    );
    for (id, grp, n, flag) in [
        (0, 0, 1, true),
        (1, 0, 1, true),
        (2, 0, 3, false),
        (3, 1, 2, true),
        (4, 1, 4, true),
    ] {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO seeded (id, grp, n, flag) VALUES ({id}, {grp}, {n}, {flag})"),
        );
    }

    let statement = parse_sql_statement(
        "SELECT grp, TOTAL(DISTINCT n) AS total_n, VAR_SAMP(DISTINCT n) AS spread, \
             BOOL_AND(DISTINCT flag) AS all_true \
             FROM seeded GROUP BY grp HAVING TOTAL(DISTINCT n) >= 4 ORDER BY grp ASC",
    )
    .expect("parse grouped total variance bool");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_simple_grouped_numeric_aggregate_query(query, &[])
        .expect("execute grouped total variance bool")
        .expect("grouped total variance bool should stay on fast path");

    assert_eq!(
        result.columns(),
        &[
            "grp".to_string(),
            "total_n".to_string(),
            "spread".to_string(),
            "all_true".to_string(),
        ]
    );
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(0),
            Value::Float64(4.0),
            Value::Float64(2.0),
            Value::Bool(false),
        ]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[
            Value::Int64(1),
            Value::Float64(6.0),
            Value::Float64(2.0),
            Value::Bool(true),
        ]
    );
}

#[test]
fn simple_grouped_having_only_aggregate_uses_numeric_fast_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE seeded (id INT64 PRIMARY KEY, grp INT64, n INT64)",
    );
    for (id, grp, n) in [(0, 0, 1), (1, 0, 4), (2, 1, 1)] {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO seeded (id, grp, n) VALUES ({id}, {grp}, {n})"),
        );
    }

    let statement = parse_sql_statement(
        "SELECT grp, COUNT(*) AS c FROM seeded \
             GROUP BY grp HAVING SUM(n) >= 3 ORDER BY grp ASC",
    )
    .expect("parse grouped having-only aggregate");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_simple_grouped_numeric_aggregate_query(query, &[])
        .expect("execute grouped having-only aggregate")
        .expect("grouped having-only aggregate should stay on fast path");

    assert_eq!(result.columns(), &["grp".to_string(), "c".to_string()]);
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(0), Value::Int64(2)]
    );
}

#[test]
fn simple_grouped_count_with_expression_filter_uses_fast_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE seeded (id INT64 PRIMARY KEY, grp INT64, n INT64, m INT64)",
    );
    for (id, grp, n, m) in [(0, 0, 1, 1), (1, 0, 2, 4), (2, 1, 3, 1), (3, 1, 1, 0)] {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO seeded (id, grp, n, m) VALUES ({id}, {grp}, {n}, {m})"),
        );
    }

    let statement = parse_sql_statement(
        "SELECT grp, COUNT(*) AS c FROM seeded \
             WHERE n + m >= 5 GROUP BY grp ORDER BY grp ASC",
    )
    .expect("parse grouped count with expression filter");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_simple_grouped_count_query(query, &[])
        .expect("execute grouped count with expression filter")
        .expect("grouped count with expression filter should stay on fast path");

    assert_eq!(result.columns(), &["grp".to_string(), "c".to_string()]);
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(0), Value::Int64(1)]
    );
}

#[test]
fn simple_grouped_numeric_with_expression_filter_uses_fast_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE seeded (id INT64 PRIMARY KEY, grp INT64, n INT64, m INT64)",
    );
    for (id, grp, n, m) in [(0, 0, 1, 1), (1, 0, 2, 4), (2, 1, 3, 1), (3, 1, 1, 0)] {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO seeded (id, grp, n, m) VALUES ({id}, {grp}, {n}, {m})"),
        );
    }

    let statement = parse_sql_statement(
        "SELECT grp, SUM(n) AS total FROM seeded \
             WHERE n + m >= 4 GROUP BY grp ORDER BY grp ASC",
    )
    .expect("parse grouped numeric with expression filter");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_simple_grouped_numeric_aggregate_query(query, &[])
        .expect("execute grouped numeric with expression filter")
        .expect("grouped numeric with expression filter should stay on fast path");

    assert_eq!(result.columns(), &["grp".to_string(), "total".to_string()]);
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(0), Value::Int64(2)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(1), Value::Int64(3)]
    );
}

#[test]
fn simple_column_projection_with_expression_filter_uses_expression_fast_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE seeded (id INT64 PRIMARY KEY, n INT64, m INT64)",
    );
    for (id, n, m) in [(0, 1, 1), (1, 2, 4), (2, 3, 1), (3, 1, 0)] {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO seeded (id, n, m) VALUES ({id}, {n}, {m})"),
        );
    }

    let statement = parse_sql_statement("SELECT n FROM seeded WHERE n + m >= 5 ORDER BY n ASC")
        .expect("parse filtered column projection");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_simple_expression_projection_query(query, &[])
        .expect("execute filtered column projection")
        .expect("filtered column projection should stay on expression fast path");

    assert_eq!(result.columns(), &["n".to_string()]);
    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].values(), &[Value::Int64(2)]);
}

#[test]
fn distinct_column_projection_with_expression_filter_uses_expression_fast_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE seeded (id INT64 PRIMARY KEY, grp INT64, n INT64, m INT64)",
    );
    for (id, grp, n, m) in [(0, 0, 1, 1), (1, 0, 2, 4), (2, 1, 3, 1), (3, 1, 2, 3)] {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO seeded (id, grp, n, m) VALUES ({id}, {grp}, {n}, {m})"),
        );
    }

    let statement = parse_sql_statement("SELECT DISTINCT grp FROM seeded WHERE n + m >= 5")
        .expect("parse distinct filtered column projection");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_simple_expression_projection_query(query, &[])
        .expect("execute distinct filtered column projection")
        .expect("distinct filtered column projection should stay on expression fast path");

    assert_eq!(result.columns(), &["grp".to_string()]);
    assert_eq!(result.rows().len(), 2);
    assert_eq!(result.rows()[0].values(), &[Value::Int64(0)]);
    assert_eq!(result.rows()[1].values(), &[Value::Int64(1)]);
}

#[test]
fn simple_indexed_projection_order_by_limit_offset_uses_fast_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE artists (id INT64 PRIMARY KEY, name_normalized TEXT, raw_id TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX artists_name_normalized_idx ON artists (name_normalized)",
    );
    execute_sql(
        &mut runtime,
        "CREATE UNIQUE INDEX artists_raw_id_idx ON artists (raw_id)",
    );
    for (id, normalized, raw_id) in [
        (10, "shared", "mbid-010"),
        (20, "shared", "mbid-020"),
        (30, "shared", "mbid-030"),
        (40, "other", "mbid-040"),
    ] {
        execute_sql(
                &mut runtime,
                &format!(
                    "INSERT INTO artists (id, name_normalized, raw_id) VALUES ({id}, '{normalized}', '{raw_id}')"
                ),
            );
    }

    let statement = parse_sql_statement(
        "SELECT id, raw_id FROM artists \
             WHERE name_normalized = 'shared' ORDER BY id DESC LIMIT 1 OFFSET 1",
    )
    .expect("parse ordered indexed projection");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_simple_indexed_projection_query(query, &[])
        .expect("execute ordered indexed projection")
        .expect("ordered indexed projection should stay on fast path");

    assert_eq!(result.columns(), &["id".to_string(), "raw_id".to_string()]);
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(20), Value::Text("mbid-020".to_string())]
    );

    let statement =
        parse_sql_statement("SELECT id FROM artists WHERE raw_id = $1 ORDER BY id ASC LIMIT 1")
            .expect("parse parameterized indexed projection");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_simple_indexed_projection_query(query, &[Value::Text("mbid-030".to_string())])
        .expect("execute parameterized indexed projection")
        .expect("parameterized indexed projection should stay on fast path");

    assert_eq!(result.columns(), &["id".to_string()]);
    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].values(), &[Value::Int64(30)]);
}

#[test]
fn simple_indexed_projection_accepts_casted_uuid_parameter_lookup() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE movies (id UUID PRIMARY KEY, title TEXT)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO movies (id, title) VALUES (UUID_PARSE('550e8400-e29b-41d4-a716-446655440000'), 'target')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO movies (id, title) VALUES (UUID_PARSE('550e8400-e29b-41d4-a716-446655440001'), 'other')",
    );

    let statement = parse_sql_statement("SELECT title FROM movies WHERE id = CAST($1 AS UUID)")
        .expect("parse casted UUID lookup");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_simple_indexed_projection_query(
            query,
            &[Value::Blob(vec![
                0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
                0x00, 0x00,
            ])],
        )
        .expect("execute casted UUID indexed projection")
        .expect("casted UUID lookup should stay on indexed projection fast path");

    assert_eq!(result.columns(), &["title".to_string()]);
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Text("target".to_string())]
    );
}

#[test]
fn movie_tag_search_uses_index_driven_join_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE Movies (
            Id UUID PRIMARY KEY,
            Title TEXT NOT NULL,
            ReleaseYear INT64 NOT NULL,
            Synopsis TEXT,
            BudgetUsd FLOAT64,
            BoxOfficeUsd FLOAT64,
            MpaaRating TEXT,
            RuntimeMinutes INT64,
            AddedAt TEXT
        )",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE Tags (Id UUID PRIMARY KEY, Name TEXT NOT NULL UNIQUE)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE MovieTags (
            MovieId UUID NOT NULL,
            TagId UUID NOT NULL,
            PRIMARY KEY (MovieId, TagId)
        )",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX ix_movietags_tag ON MovieTags(TagId)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO Tags VALUES
            (UUID_PARSE('00000000-0000-0000-0000-0000000000aa'), 'featured'),
            (UUID_PARSE('00000000-0000-0000-0000-0000000000bb'), 'other')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO Movies (Id, Title, ReleaseYear, Synopsis, BudgetUsd, BoxOfficeUsd, MpaaRating, RuntimeMinutes, AddedAt) VALUES
            (UUID_PARSE('00000000-0000-0000-0000-000000000003'), 'newer', 2021, '', 1.0, 2.0, 'PG', 90, '2021-01-01'),
            (UUID_PARSE('00000000-0000-0000-0000-000000000001'), 'older-a', 2020, '', 1.0, 2.0, 'PG', 90, '2020-01-01'),
            (UUID_PARSE('00000000-0000-0000-0000-000000000002'), 'older-b', 2020, '', 1.0, 2.0, 'PG', 90, '2020-01-02')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO MovieTags VALUES
            (UUID_PARSE('00000000-0000-0000-0000-000000000003'), UUID_PARSE('00000000-0000-0000-0000-0000000000aa')),
            (UUID_PARSE('00000000-0000-0000-0000-000000000002'), UUID_PARSE('00000000-0000-0000-0000-0000000000aa')),
            (UUID_PARSE('00000000-0000-0000-0000-000000000001'), UUID_PARSE('00000000-0000-0000-0000-0000000000aa'))",
    );

    let statement = parse_sql_statement(
        "SELECT m.Id, m.Title, m.ReleaseYear
         FROM Movies m
         JOIN MovieTags mt ON mt.MovieId = m.Id
         JOIN Tags t ON t.Id = mt.TagId
         WHERE t.Name = $1
         ORDER BY m.ReleaseYear DESC, m.Id ASC
         LIMIT $2",
    )
    .expect("parse movie tag search");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_movie_tag_search_query(
            query,
            &[Value::Text("featured".to_string()), Value::Int64(3)],
        )
        .expect("execute movie tag search")
        .expect("movie tag search should use index-driven join path");

    assert_eq!(
        result
            .rows()
            .iter()
            .map(|row| row.values()[1].clone())
            .collect::<Vec<_>>(),
        vec![
            Value::Text("newer".to_string()),
            Value::Text("older-a".to_string()),
            Value::Text("older-b".to_string())
        ]
    );
}

#[test]
fn movie_watchlist_query_uses_index_driven_left_join_aggregate_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE Movies (
            Id UUID PRIMARY KEY,
            Title TEXT NOT NULL
        )",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE Reviews (
            Id UUID PRIMARY KEY,
            MovieId UUID NOT NULL,
            Score INT64 NOT NULL
        )",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX ix_reviews_movie ON Reviews(MovieId)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE Watchlist (
            Id UUID PRIMARY KEY,
            UserHandle TEXT NOT NULL,
            MovieId UUID NOT NULL,
            Priority INT64 NOT NULL
        )",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX ix_watchlist_user ON Watchlist(UserHandle)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO Movies VALUES
            (UUID_PARSE('00000000-0000-0000-0000-000000000001'), 'alpha'),
            (UUID_PARSE('00000000-0000-0000-0000-000000000002'), 'beta'),
            (UUID_PARSE('00000000-0000-0000-0000-000000000003'), 'gamma')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO Reviews VALUES
            (UUID_PARSE('00000000-0000-0000-0000-000000000101'), UUID_PARSE('00000000-0000-0000-0000-000000000001'), 8),
            (UUID_PARSE('00000000-0000-0000-0000-000000000102'), UUID_PARSE('00000000-0000-0000-0000-000000000001'), 10),
            (UUID_PARSE('00000000-0000-0000-0000-000000000103'), UUID_PARSE('00000000-0000-0000-0000-000000000002'), 5)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO Watchlist VALUES
            (UUID_PARSE('00000000-0000-0000-0000-000000000201'), 'user-a', UUID_PARSE('00000000-0000-0000-0000-000000000001'), 2),
            (UUID_PARSE('00000000-0000-0000-0000-000000000202'), 'user-a', UUID_PARSE('00000000-0000-0000-0000-000000000002'), 5),
            (UUID_PARSE('00000000-0000-0000-0000-000000000203'), 'user-b', UUID_PARSE('00000000-0000-0000-0000-000000000003'), 5)",
    );

    let statement = parse_sql_statement(
        "SELECT m.Id, m.Title, w.Priority, AVG(r.Score) as Avg
         FROM Watchlist w
         JOIN Movies m ON m.Id = w.MovieId
         LEFT JOIN Reviews r ON r.MovieId = m.Id
         WHERE w.UserHandle = $1
         GROUP BY m.Id
         ORDER BY w.Priority DESC, Avg DESC
         LIMIT $2",
    )
    .expect("parse movie watchlist query");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_movie_watchlist_query(
            query,
            &[Value::Text("user-a".to_string()), Value::Int64(20)],
        )
        .expect("execute movie watchlist query")
        .expect("watchlist query should use index-driven left join aggregate path");

    assert_eq!(
        result
            .rows()
            .iter()
            .map(|row| (
                row.values()[1].clone(),
                row.values()[2].clone(),
                row.values()[3].clone()
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                Value::Text("beta".to_string()),
                Value::Int64(5),
                Value::Float64(5.0)
            ),
            (
                Value::Text("alpha".to_string()),
                Value::Int64(2),
                Value::Float64(9.0)
            )
        ]
    );
}

#[test]
fn movie_top_rated_by_year_uses_indexed_review_aggregate_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE Movies (
            Id UUID PRIMARY KEY,
            Title TEXT NOT NULL,
            ReleaseYear INT64 NOT NULL,
            Synopsis TEXT,
            BudgetUsd FLOAT64,
            BoxOfficeUsd FLOAT64,
            MpaaRating TEXT,
            RuntimeMinutes INT64,
            AddedAt TEXT
        )",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE Reviews (
            Id UUID PRIMARY KEY,
            MovieId UUID NOT NULL,
            Score INT64 NOT NULL
        )",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX ix_reviews_movie ON Reviews(MovieId)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO Movies (Id, Title, ReleaseYear, Synopsis, BudgetUsd, BoxOfficeUsd, MpaaRating, RuntimeMinutes, AddedAt) VALUES
            (UUID_PARSE('00000000-0000-0000-0000-000000000001'), 'alpha', 2020, '', 1.0, 2.0, 'PG', 90, '2020-01-01'),
            (UUID_PARSE('00000000-0000-0000-0000-000000000002'), 'beta', 2020, '', 1.0, 2.0, 'PG', 90, '2020-01-02'),
            (UUID_PARSE('00000000-0000-0000-0000-000000000003'), 'gamma', 2021, '', 1.0, 2.0, 'PG', 90, '2021-01-01')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO Reviews VALUES
            (UUID_PARSE('00000000-0000-0000-0000-000000000101'), UUID_PARSE('00000000-0000-0000-0000-000000000001'), 8),
            (UUID_PARSE('00000000-0000-0000-0000-000000000102'), UUID_PARSE('00000000-0000-0000-0000-000000000001'), 10),
            (UUID_PARSE('00000000-0000-0000-0000-000000000103'), UUID_PARSE('00000000-0000-0000-0000-000000000002'), 10),
            (UUID_PARSE('00000000-0000-0000-0000-000000000104'), UUID_PARSE('00000000-0000-0000-0000-000000000003'), 10),
            (UUID_PARSE('00000000-0000-0000-0000-000000000105'), UUID_PARSE('00000000-0000-0000-0000-000000000003'), 10)",
    );

    let statement = parse_sql_statement(
        "SELECT m.Id, m.Title, m.ReleaseYear, m.Synopsis, m.BudgetUsd,
                m.BoxOfficeUsd, m.MpaaRating, m.RuntimeMinutes, m.AddedAt,
                AVG(r.Score) as AvgScore, COUNT(r.Id) as ReviewCount
         FROM Movies m
         JOIN Reviews r ON r.MovieId = m.Id
         WHERE m.ReleaseYear = $1
         GROUP BY m.Id
         HAVING COUNT(r.Id) >= $2
         ORDER BY AvgScore DESC, m.Title
         LIMIT $3",
    )
    .expect("parse top-rated movie query");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_movie_top_rated_by_year_query(
            query,
            &[Value::Int64(2020), Value::Int64(2), Value::Int64(25)],
        )
        .expect("execute top-rated movie query")
        .expect("top-rated movie query should use indexed review aggregate path");

    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values()[1],
        Value::Text("alpha".to_string())
    );
    assert_eq!(result.rows()[0].values()[9], Value::Float64(9.0));
    assert_eq!(result.rows()[0].values()[10], Value::Int64(2));
}

#[test]
fn movie_busiest_people_query_uses_role_count_top_n_before_people_fetch() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE People (
            Id UUID PRIMARY KEY,
            FullName TEXT NOT NULL,
            BirthDate TEXT,
            Biography TEXT
        )",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE Roles (
            Id UUID PRIMARY KEY,
            MovieId UUID NOT NULL,
            PersonId UUID NOT NULL REFERENCES People(Id),
            CharacterName TEXT,
            BillingOrder INT64,
            IsLead INT64 NOT NULL
        )",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX ix_roles_person ON Roles(PersonId)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO People VALUES
            (UUID_PARSE('00000000-0000-0000-0000-000000000001'), 'Ada Actor', '1970-01-01', 'long biography a'),
            (UUID_PARSE('00000000-0000-0000-0000-000000000002'), 'Bea Actor', '1980-01-01', 'long biography b'),
            (UUID_PARSE('00000000-0000-0000-0000-000000000003'), 'Cal Actor', '1990-01-01', 'long biography c')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO Roles VALUES
            (UUID_PARSE('00000000-0000-0000-0000-000000000101'), UUID_PARSE('00000000-0000-0000-0000-000000000201'), UUID_PARSE('00000000-0000-0000-0000-000000000001'), 'a1', 1, 1),
            (UUID_PARSE('00000000-0000-0000-0000-000000000102'), UUID_PARSE('00000000-0000-0000-0000-000000000202'), UUID_PARSE('00000000-0000-0000-0000-000000000001'), 'a2', 2, 0),
            (UUID_PARSE('00000000-0000-0000-0000-000000000103'), UUID_PARSE('00000000-0000-0000-0000-000000000203'), UUID_PARSE('00000000-0000-0000-0000-000000000001'), 'a3', 3, 0),
            (UUID_PARSE('00000000-0000-0000-0000-000000000104'), UUID_PARSE('00000000-0000-0000-0000-000000000204'), UUID_PARSE('00000000-0000-0000-0000-000000000002'), 'b1', 1, 1),
            (UUID_PARSE('00000000-0000-0000-0000-000000000105'), UUID_PARSE('00000000-0000-0000-0000-000000000205'), UUID_PARSE('00000000-0000-0000-0000-000000000003'), 'c1', 1, 1),
            (UUID_PARSE('00000000-0000-0000-0000-000000000106'), UUID_PARSE('00000000-0000-0000-0000-000000000206'), UUID_PARSE('00000000-0000-0000-0000-000000000003'), 'c2', 2, 0)",
    );

    let statement = parse_sql_statement(
        "SELECT p.Id, p.FullName, p.BirthDate, p.Biography, COUNT(r.Id) as RoleCount
         FROM People p
         JOIN Roles r ON r.PersonId = p.Id
         GROUP BY p.Id
         ORDER BY RoleCount DESC
         LIMIT $1",
    )
    .expect("parse busiest people query");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_movie_busiest_people_query(query, &[Value::Int64(2)])
        .expect("execute busiest people query")
        .expect("busiest people query should use role-count top-n path");

    assert_eq!(
        result
            .rows()
            .iter()
            .map(|row| (row.values()[1].clone(), row.values()[4].clone()))
            .collect::<Vec<_>>(),
        vec![
            (Value::Text("Ada Actor".to_string()), Value::Int64(3)),
            (Value::Text("Cal Actor".to_string()), Value::Int64(2))
        ]
    );
}

#[test]
fn simple_indexed_projection_order_by_id_uses_row_id_order_with_limit_offset() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE issues (id INT64 PRIMARY KEY, project_id INT64)",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX idx_issues_project ON issues (project_id)",
    );
    for (id, project_id) in [(30, 3), (10, 3), (20, 3), (40, 2)] {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO issues (id, project_id) VALUES ({id}, {project_id})"),
        );
    }

    let statement = parse_sql_statement(
        "SELECT id, project_id FROM issues \
         WHERE project_id = 3 ORDER BY id ASC LIMIT 2 OFFSET 1",
    )
    .expect("parse pagination indexed projection");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_simple_indexed_projection_query(query, &[])
        .expect("execute pagination indexed projection")
        .expect("pagination indexed projection should stay on fast path");

    assert_eq!(
        result.columns(),
        &["id".to_string(), "project_id".to_string()]
    );
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(20), Value::Int64(3)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(30), Value::Int64(3)]
    );
}

#[test]
fn simple_indexed_projection_uses_compound_prefix_equality_lookup() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE issues (
            id INT64 PRIMARY KEY,
            project_id INT64,
            status TEXT,
            title TEXT,
            created_at INT64
        )",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX idx_issues_project_status ON issues (project_id, status)",
    );
    for (id, project_id, status, created_at) in [
        (1, 10, "open", 100),
        (2, 10, "closed", 200),
        (3, 10, "open", 300),
        (4, 20, "open", 400),
    ] {
        execute_sql(
            &mut runtime,
            &format!(
                "INSERT INTO issues (id, project_id, status, title, created_at) \
                 VALUES ({id}, {project_id}, '{status}', 'issue-{id}', {created_at})"
            ),
        );
    }

    let statement = parse_sql_statement(
        "SELECT id, title, created_at FROM issues \
         WHERE project_id = $1 AND status = $2 \
         ORDER BY created_at DESC",
    )
    .expect("parse compound indexed projection");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };
    let result = runtime
        .try_execute_simple_indexed_projection_query(
            query,
            &[Value::Int64(10), Value::Text("open".to_string())],
        )
        .expect("execute compound indexed projection")
        .expect("compound indexed projection should stay on fast path");

    assert_eq!(
        result.columns(),
        &[
            "id".to_string(),
            "title".to_string(),
            "created_at".to_string()
        ]
    );
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(3),
            Value::Text("issue-3".to_string()),
            Value::Int64(300)
        ]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[
            Value::Int64(1),
            Value::Text("issue-1".to_string()),
            Value::Int64(100)
        ]
    );

    let statement = parse_sql_statement(
        "SELECT id, created_at FROM issues \
         WHERE status = $2 AND project_id = $1 \
         ORDER BY created_at DESC",
    )
    .expect("parse reordered compound indexed projection");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };
    let reordered = runtime
        .try_execute_simple_indexed_projection_query(
            query,
            &[Value::Int64(10), Value::Text("open".to_string())],
        )
        .expect("execute reordered compound indexed projection")
        .expect("reordered compound indexed projection should stay on fast path");
    assert_eq!(
        reordered
            .rows()
            .iter()
            .map(|row| row.values()[0].clone())
            .collect::<Vec<_>>(),
        vec![Value::Int64(3), Value::Int64(1)]
    );
}

#[test]
fn simple_projection_no_order_by_offset_limit_uses_fast_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE songs (id INT64 PRIMARY KEY, title TEXT)",
    );
    for id in 1..=5 {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO songs (id, title) VALUES ({id}, 't{id}')"),
        );
    }

    let statement =
        parse_sql_statement("SELECT id FROM songs LIMIT 3 OFFSET 2").expect("parse query");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query");
    };
    let result = runtime
        .try_execute_simple_table_projection_query(query, &[])
        .expect("execute")
        .expect("simple table projection should stay on fast path");

    assert_eq!(result.columns(), &["id".to_string()]);
    assert_eq!(result.rows().len(), 3);
    assert_eq!(result.rows()[0].values(), &[Value::Int64(3)]);
    assert_eq!(result.rows()[1].values(), &[Value::Int64(4)]);
    assert_eq!(result.rows()[2].values(), &[Value::Int64(5)]);
}

#[test]
fn simple_filtered_projection_no_order_by_offset_limit_uses_fast_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE users (id INT64 PRIMARY KEY, age INT64)",
    );
    for (id, age) in [(1, 21), (2, 22), (3, 23), (4, 24), (5, 25)] {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO users (id, age) VALUES ({id}, {age})"),
        );
    }

    let statement =
        parse_sql_statement("SELECT id FROM users WHERE age >= 22 AND age <= 25 LIMIT 2 OFFSET 1")
            .expect("parse filtered query");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query");
    };

    let result = runtime
        .try_execute_simple_filtered_projection_query(query, &[])
        .expect("execute")
        .expect("filtered projection should stay on fast path");

    assert_eq!(result.columns(), &["id".to_string()]);
    assert_eq!(result.rows().len(), 2);
    assert_eq!(result.rows()[0].values(), &[Value::Int64(3)]);
    assert_eq!(result.rows()[1].values(), &[Value::Int64(4)]);
}

#[test]
fn simple_filtered_projection_range_index_with_residual_uses_fast_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE movies (id INT64 PRIMARY KEY, title TEXT, rating FLOAT64, runtime_minutes INT64)",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX idx_movies_rating ON movies(rating)",
    );
    for (id, title, rating, runtime_minutes) in [
        (1, "short_good", 8.0, 100),
        (2, "long_good", 7.6, 130),
        (3, "too_high", 9.5, 150),
        (4, "also_good", 8.5, 140),
        (5, "edge_good", 7.8, 121),
        (6, "too_low", 6.0, 150),
    ] {
        execute_sql(
            &mut runtime,
            &format!(
                "INSERT INTO movies (id, title, rating, runtime_minutes) VALUES ({id}, '{title}', {rating}, {runtime_minutes})"
            ),
        );
    }

    let statement = parse_sql_statement(
        "SELECT id, title, rating FROM movies WHERE rating >= 7.5 AND rating <= 9.0 AND runtime_minutes > 120",
    )
    .expect("parse filtered range query");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query");
    };

    let result = runtime
        .try_execute_simple_filtered_projection_query(query, &[])
        .expect("execute")
        .expect("filtered range projection should stay on fast path");

    assert_eq!(
        result.columns(),
        &["id".to_string(), "title".to_string(), "rating".to_string()]
    );
    assert_eq!(result.rows().len(), 3);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(2),
            Value::Text("long_good".to_string()),
            Value::Float64(7.6),
        ]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[
            Value::Int64(4),
            Value::Text("also_good".to_string()),
            Value::Float64(8.5),
        ]
    );
    assert_eq!(
        result.rows()[2].values(),
        &[
            Value::Int64(5),
            Value::Text("edge_good".to_string()),
            Value::Float64(7.8),
        ]
    );
}

#[test]
fn simple_filtered_projection_order_by_limit_offset_uses_fast_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE movies (id INT64 PRIMARY KEY, released INT64, rating FLOAT64)",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX idx_movies_rating ON movies(rating)",
    );
    for (id, released, rating) in [
        (1, 2010, 9.5),
        (2, 2011, 8.1),
        (3, 2009, 10.0),
        (4, 2015, 9.0),
        (5, 2020, 7.0),
    ] {
        execute_sql(
            &mut runtime,
            &format!(
                "INSERT INTO movies (id, released, rating) VALUES ({id}, {released}, {rating})"
            ),
        );
    }

    let statement = parse_sql_statement(
        "SELECT id, rating FROM movies WHERE released >= 2010 ORDER BY rating DESC LIMIT 2 OFFSET 1",
    )
    .expect("parse filtered ordered query");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query");
    };

    let result = runtime
        .try_execute_simple_filtered_projection_query(query, &[])
        .expect("execute")
        .expect("filtered ordered projection should stay on fast path");

    assert_eq!(result.columns(), &["id".to_string(), "rating".to_string()]);
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(4), Value::Float64(9.0)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(2), Value::Float64(8.1)]
    );
}

#[test]
fn simple_indexed_join_multi_order_by_uses_fast_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE docs (id INT64 PRIMARY KEY, body TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE archive (id INT64 PRIMARY KEY, doc_id INT64, note TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX archive_doc_idx ON archive (doc_id)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO docs (id, body) VALUES (8, 'doc-8')",
    );
    for (id, note) in [(1000, "note-z"), (1001, "note-z"), (1002, "note-a")] {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO archive (id, doc_id, note) VALUES ({id}, 8, '{note}')"),
        );
    }

    let statement = parse_sql_statement(
        "SELECT docs.id, archive.id AS archive_id, archive.note \
             FROM docs JOIN archive ON docs.id = archive.doc_id \
             WHERE docs.id = 8 \
             ORDER BY archive.note DESC, archive_id ASC",
    )
    .expect("parse indexed join");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_simple_indexed_join_projection_query(query, &[])
        .expect("execute indexed join")
        .expect("indexed join query should stay on fast path");

    assert_eq!(
        result.columns(),
        &[
            "id".to_string(),
            "archive_id".to_string(),
            "note".to_string()
        ]
    );
    assert_eq!(result.rows().len(), 3);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(8),
            Value::Int64(1000),
            Value::Text("note-z".to_string())
        ]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[
            Value::Int64(8),
            Value::Int64(1001),
            Value::Text("note-z".to_string())
        ]
    );
    assert_eq!(
        result.rows()[2].values(),
        &[
            Value::Int64(8),
            Value::Int64(1002),
            Value::Text("note-a".to_string())
        ]
    );
}

#[test]
fn simple_indexed_join_filters_source_rowid_alias_without_secondary_index() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE join_users (id INT64 PRIMARY KEY, name TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE join_profiles (id INT64 PRIMARY KEY, bio TEXT)",
    );
    for id in 1..=3 {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO join_users (id, name) VALUES ({id}, 'u{id}')"),
        );
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO join_profiles (id, bio) VALUES ({id}, 'b{id}')"),
        );
    }

    let statement = parse_sql_statement(
        "SELECT u.name, p.bio \
             FROM join_users AS u \
             JOIN join_profiles AS p ON u.id = p.id \
             WHERE u.id = $1",
    )
    .expect("parse indexed join");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_simple_indexed_join_projection_query(query, &[Value::Int64(2)])
        .expect("execute indexed join")
        .expect("rowid-filtered join query should stay on fast path");

    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Text("u2".to_string()), Value::Text("b2".to_string())]
    );
}

#[test]
fn indexed_table_lookup_filters_rowid_alias_without_secondary_index() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE lookup_users (id INT64 PRIMARY KEY, name TEXT)",
    );
    for id in 1..=3 {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO lookup_users (id, name) VALUES ({id}, 'u{id}')"),
        );
    }

    let lookup = runtime
        .indexed_table_lookup(
            "lookup_users",
            &Some("u".to_string()),
            "id",
            &Expr::Parameter(1),
            &[Value::Int64(2)],
            &BTreeMap::new(),
        )
        .expect("lookup should execute")
        .expect("rowid alias lookup should be handled without a secondary index");
    assert_eq!(lookup.rows.len(), 1);
    assert_eq!(
        lookup.rows[0],
        vec![Value::Int64(2), Value::Text("u2".to_string())]
    );
    assert!(lookup
        .columns
        .iter()
        .all(|column| column.table.as_deref().is_some_and(|table| table == "u")));

    let missing = runtime
        .indexed_table_lookup(
            "lookup_users",
            &None,
            "id",
            &Expr::Parameter(1),
            &[Value::Int64(99)],
            &BTreeMap::new(),
        )
        .expect("missing lookup should execute")
        .expect("rowid alias lookup should still claim the fast path");
    assert!(missing.rows.is_empty());

    let wrong_type = runtime
        .indexed_table_lookup(
            "lookup_users",
            &None,
            "id",
            &Expr::Parameter(1),
            &[Value::Text("2".to_string())],
            &BTreeMap::new(),
        )
        .expect("typed miss should execute")
        .expect("rowid alias lookup should return an empty fast-path result");
    assert!(wrong_type.rows.is_empty());
}

#[test]
fn indexed_right_join_with_right_table_probe_preserves_unmatched_right_rows() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE docs (id INT64 PRIMARY KEY, body TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE archive (id INT64 PRIMARY KEY, doc_id INT64, note TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX archive_doc_idx ON archive (doc_id)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO docs (id, body) VALUES (8, 'doc-8')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO archive (id, doc_id, note) VALUES (1000, 8, 'match')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO archive (id, doc_id, note) VALUES (1001, 99, 'orphan')",
    );

    let statement = parse_sql_statement(
        "SELECT docs.id, archive.id \
             FROM docs RIGHT JOIN archive ON docs.id = archive.doc_id",
    )
    .expect("parse indexed right join");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };
    let crate::sql::ast::QueryBody::Select(select) = &query.body else {
        panic!("expected select query");
    };
    let FromItem::Join {
        left,
        right,
        kind,
        constraint,
    } = &select.from[0]
    else {
        panic!("expected join");
    };

    let left_dataset = runtime
        .evaluate_from_item(left.as_ref(), &[], &BTreeMap::new())
        .expect("evaluate left dataset");
    let result = runtime
        .try_indexed_equi_join_with_right_table(
            &left_dataset,
            right.as_ref(),
            *kind,
            constraint,
            &BTreeMap::new(),
        )
        .expect("execute indexed right join")
        .expect("indexed right join should stay on probe path");

    assert_eq!(result.columns.len(), 5);
    assert_eq!(result.columns[0].name, "id");
    assert_eq!(result.columns[1].name, "body");
    assert_eq!(result.columns[2].name, "id");
    assert_eq!(result.columns[3].name, "doc_id");
    assert_eq!(result.columns[4].name, "note");
    assert_eq!(result.rows.len(), 2);
    assert_eq!(
        result.rows[0].as_slice(),
        &[
            Value::Int64(8),
            Value::Text("doc-8".to_string()),
            Value::Int64(1000),
            Value::Int64(8),
            Value::Text("match".to_string()),
        ]
    );
    assert_eq!(
        result.rows[1].as_slice(),
        &[
            Value::Null,
            Value::Null,
            Value::Int64(1001),
            Value::Int64(99),
            Value::Text("orphan".to_string()),
        ]
    );
}

#[test]
fn indexed_full_join_with_right_table_probe_preserves_both_unmatched_sides() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE docs (id INT64 PRIMARY KEY, body TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE archive (id INT64 PRIMARY KEY, doc_id INT64, note TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX archive_doc_idx ON archive (doc_id)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO docs (id, body) VALUES (8, 'doc-8')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO docs (id, body) VALUES (9, 'doc-9')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO archive (id, doc_id, note) VALUES (1000, 8, 'match')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO archive (id, doc_id, note) VALUES (1001, 99, 'orphan')",
    );

    let statement = parse_sql_statement(
        "SELECT docs.id, archive.id \
             FROM docs FULL JOIN archive ON docs.id = archive.doc_id",
    )
    .expect("parse indexed full join");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };
    let crate::sql::ast::QueryBody::Select(select) = &query.body else {
        panic!("expected select query");
    };
    let FromItem::Join {
        left,
        right,
        kind,
        constraint,
    } = &select.from[0]
    else {
        panic!("expected join");
    };

    let left_dataset = runtime
        .evaluate_from_item(left.as_ref(), &[], &BTreeMap::new())
        .expect("evaluate left dataset");
    let result = runtime
        .try_indexed_equi_join_with_right_table(
            &left_dataset,
            right.as_ref(),
            *kind,
            constraint,
            &BTreeMap::new(),
        )
        .expect("execute indexed full join")
        .expect("indexed full join should stay on probe path");

    assert_eq!(result.columns.len(), 5);
    assert_eq!(result.columns[0].name, "id");
    assert_eq!(result.columns[1].name, "body");
    assert_eq!(result.columns[2].name, "id");
    assert_eq!(result.columns[3].name, "doc_id");
    assert_eq!(result.columns[4].name, "note");
    assert_eq!(result.rows.len(), 3);
    assert_eq!(
        result.rows[0].as_slice(),
        &[
            Value::Int64(8),
            Value::Text("doc-8".to_string()),
            Value::Int64(1000),
            Value::Int64(8),
            Value::Text("match".to_string()),
        ]
    );
    assert_eq!(
        result.rows[1].as_slice(),
        &[
            Value::Int64(9),
            Value::Text("doc-9".to_string()),
            Value::Null,
            Value::Null,
            Value::Null
        ]
    );
    assert_eq!(
        result.rows[2].as_slice(),
        &[
            Value::Null,
            Value::Null,
            Value::Int64(1001),
            Value::Int64(99),
            Value::Text("orphan".to_string()),
        ]
    );
}

#[test]
fn hashed_full_join_with_right_table_probe_preserves_both_unmatched_sides() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE docs (id INT64 PRIMARY KEY, body TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE archive (id INT64 PRIMARY KEY, doc_id INT64, note TEXT)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO docs (id, body) VALUES (8, 'doc-8')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO docs (id, body) VALUES (9, 'doc-9')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO archive (id, doc_id, note) VALUES (1000, 8, 'match')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO archive (id, doc_id, note) VALUES (1001, 99, 'orphan')",
    );

    let statement = parse_sql_statement(
        "SELECT docs.id, archive.id \
             FROM docs FULL JOIN archive ON docs.id = archive.doc_id",
    )
    .expect("parse hashed full join");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };
    let crate::sql::ast::QueryBody::Select(select) = &query.body else {
        panic!("expected select query");
    };
    let FromItem::Join {
        left,
        right,
        kind,
        constraint,
    } = &select.from[0]
    else {
        panic!("expected join");
    };

    let left_dataset = runtime
        .evaluate_from_item(left.as_ref(), &[], &BTreeMap::new())
        .expect("evaluate left dataset");
    let result = runtime
        .try_indexed_equi_join_with_right_table(
            &left_dataset,
            right.as_ref(),
            *kind,
            constraint,
            &BTreeMap::new(),
        )
        .expect("execute hashed full join")
        .expect("hashed full join should stay on probe path");

    assert_eq!(result.columns.len(), 5);
    assert_eq!(result.rows.len(), 3);
    assert_eq!(
        result.rows[0].as_slice(),
        &[
            Value::Int64(8),
            Value::Text("doc-8".to_string()),
            Value::Int64(1000),
            Value::Int64(8),
            Value::Text("match".to_string()),
        ]
    );
    assert_eq!(
        result.rows[1].as_slice(),
        &[
            Value::Int64(9),
            Value::Text("doc-9".to_string()),
            Value::Null,
            Value::Null,
            Value::Null
        ]
    );
    assert_eq!(
        result.rows[2].as_slice(),
        &[
            Value::Null,
            Value::Null,
            Value::Int64(1001),
            Value::Int64(99),
            Value::Text("orphan".to_string()),
        ]
    );
}

#[test]
fn simple_indexed_join_distinct_uses_fast_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE docs (id INT64 PRIMARY KEY, body TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE archive (id INT64 PRIMARY KEY, doc_id INT64, note TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX archive_doc_idx ON archive (doc_id)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO docs (id, body) VALUES (8, 'doc-8')",
    );
    for (id, note) in [(1000, "note-z"), (1001, "note-z"), (1002, "note-a")] {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO archive (id, doc_id, note) VALUES ({id}, 8, '{note}')"),
        );
    }

    let statement = parse_sql_statement(
        "SELECT DISTINCT docs.id, archive.note \
             FROM docs JOIN archive ON docs.id = archive.doc_id \
             WHERE docs.id = 8 \
             ORDER BY note DESC",
    )
    .expect("parse indexed join distinct");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_simple_indexed_join_projection_query(query, &[])
        .expect("execute indexed join distinct")
        .expect("indexed join distinct should stay on fast path");

    assert_eq!(result.columns(), &["id".to_string(), "note".to_string()]);
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(8), Value::Text("note-z".to_string())]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(8), Value::Text("note-a".to_string())]
    );
}

#[test]
fn simple_indexed_join_using_uses_fast_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE docs (id INT64 PRIMARY KEY, body TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE archive (id INT64 PRIMARY KEY, note TEXT)",
    );
    execute_sql(&mut runtime, "CREATE INDEX archive_id_idx ON archive (id)");
    execute_sql(
        &mut runtime,
        "INSERT INTO docs (id, body) VALUES (8, 'doc-8')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO archive (id, note) VALUES (8, 'note-z')",
    );

    let statement = parse_sql_statement(
        "SELECT docs.id, archive.note \
             FROM docs JOIN archive USING (id) \
             WHERE docs.id = 8 \
             ORDER BY archive.note DESC",
    )
    .expect("parse indexed join using");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_simple_indexed_join_projection_query(query, &[])
        .expect("execute indexed join using")
        .expect("indexed join using should stay on fast path");

    assert_eq!(result.columns(), &["id".to_string(), "note".to_string()]);
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(8), Value::Text("note-z".to_string())]
    );
}

#[test]
fn simple_indexed_join_using_wildcard_uses_fast_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE docs (id INT64 PRIMARY KEY, body TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE archive (id INT64 PRIMARY KEY, note TEXT)",
    );
    execute_sql(&mut runtime, "CREATE INDEX archive_id_idx ON archive (id)");
    execute_sql(
        &mut runtime,
        "INSERT INTO docs (id, body) VALUES (8, 'doc-8')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO archive (id, note) VALUES (8, 'note-z')",
    );

    let statement = parse_sql_statement(
        "SELECT * \
             FROM docs JOIN archive USING (id) \
             WHERE docs.id = 8 \
             ORDER BY note DESC",
    )
    .expect("parse indexed join using wildcard");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_simple_indexed_join_projection_query(query, &[])
        .expect("execute indexed join using wildcard")
        .expect("indexed join using wildcard should stay on fast path");

    assert_eq!(
        result.columns(),
        &["id".to_string(), "body".to_string(), "note".to_string()]
    );
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(8),
            Value::Text("doc-8".to_string()),
            Value::Text("note-z".to_string()),
        ]
    );
}

#[test]
fn simple_indexed_join_multi_column_using_wildcard_uses_fast_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE docs (pk INT64 PRIMARY KEY, org_id INT64, id INT64, body TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE archive (pk INT64 PRIMARY KEY, org_id INT64, id INT64, note TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX archive_org_id_id_idx ON archive (org_id, id)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO docs (pk, org_id, id, body) VALUES (1, 7, 8, 'doc-8')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO archive (pk, org_id, id, note) VALUES (1, 7, 8, 'note-z')",
    );

    let statement = parse_sql_statement(
        "SELECT * \
             FROM docs JOIN archive USING (org_id, id) \
             ORDER BY note DESC",
    )
    .expect("parse indexed join multi-column using wildcard");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_simple_indexed_join_projection_query(query, &[])
        .expect("execute indexed join multi-column using wildcard")
        .expect("indexed join multi-column using wildcard should stay on fast path");

    assert_eq!(
        result.columns(),
        &[
            "org_id".to_string(),
            "id".to_string(),
            "pk".to_string(),
            "body".to_string(),
            "pk".to_string(),
            "note".to_string(),
        ]
    );
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(7),
            Value::Int64(8),
            Value::Int64(1),
            Value::Text("doc-8".to_string()),
            Value::Int64(1),
            Value::Text("note-z".to_string()),
        ]
    );
}

#[test]
fn simple_indexed_natural_join_wildcard_uses_fast_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE docs (pk INT64 PRIMARY KEY, org_id INT64, id INT64, body TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE archive (archive_pk INT64 PRIMARY KEY, org_id INT64, id INT64, note TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX archive_org_id_id_idx ON archive (org_id, id)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO docs (pk, org_id, id, body) VALUES (1, 7, 8, 'doc-8')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO archive (archive_pk, org_id, id, note) VALUES (1, 7, 8, 'note-z')",
    );

    let statement = parse_sql_statement(
        "SELECT docs.pk, org_id, id, archive.note \
             FROM docs NATURAL JOIN archive \
             ORDER BY archive.note DESC",
    )
    .expect("parse indexed natural join");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_simple_indexed_join_projection_query(query, &[])
        .expect("execute indexed natural join")
        .expect("indexed natural join should stay on fast path");

    assert_eq!(
        result.columns(),
        &[
            "pk".to_string(),
            "org_id".to_string(),
            "id".to_string(),
            "note".to_string(),
        ]
    );
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(1),
            Value::Int64(7),
            Value::Int64(8),
            Value::Text("note-z".to_string()),
        ]
    );
}

#[test]
fn simple_indexed_join_without_filter_uses_fast_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE docs (id INT64 PRIMARY KEY, body TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE archive (id INT64 PRIMARY KEY, doc_id INT64, note TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX archive_doc_idx ON archive (doc_id)",
    );
    for id in 1..=3 {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO docs (id, body) VALUES ({id}, 'doc-{id}')"),
        );
    }
    for (id, doc_id, note) in [
        (1000, 1, "note-a"),
        (1001, 2, "note-b"),
        (1002, 2, "note-c"),
    ] {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO archive (id, doc_id, note) VALUES ({id}, {doc_id}, '{note}')"),
        );
    }

    let statement = parse_sql_statement(
        "SELECT docs.id, archive.note \
             FROM docs JOIN archive ON docs.id = archive.doc_id \
             ORDER BY docs.id ASC, archive.note ASC",
    )
    .expect("parse indexed join without filter");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_simple_indexed_join_projection_query(query, &[])
        .expect("execute indexed join without filter")
        .expect("indexed join without filter should stay on fast path");

    assert_eq!(result.columns(), &["id".to_string(), "note".to_string()]);
    assert_eq!(result.rows().len(), 3);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(1), Value::Text("note-a".to_string())]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(2), Value::Text("note-b".to_string())]
    );
    assert_eq!(
        result.rows()[2].values(),
        &[Value::Int64(2), Value::Text("note-c".to_string())]
    );
}

#[test]
fn simple_hashed_join_without_index_uses_fast_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE docs (id INT64 PRIMARY KEY, body TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE archive (id INT64 PRIMARY KEY, doc_id INT64, note TEXT)",
    );
    for id in 1..=3 {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO docs (id, body) VALUES ({id}, 'doc-{id}')"),
        );
    }
    for (id, doc_id, note) in [
        (1000, 1, "note-a"),
        (1001, 2, "note-b"),
        (1002, 2, "note-c"),
    ] {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO archive (id, doc_id, note) VALUES ({id}, {doc_id}, '{note}')"),
        );
    }

    let statement = parse_sql_statement(
        "SELECT docs.id, archive.note \
             FROM docs JOIN archive ON docs.id = archive.doc_id \
             ORDER BY docs.id ASC, archive.note ASC",
    )
    .expect("parse hashed join");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_simple_indexed_join_projection_query(query, &[])
        .expect("execute hashed join")
        .expect("hashed join should stay on fast path");

    assert_eq!(result.columns(), &["id".to_string(), "note".to_string()]);
    assert_eq!(result.rows().len(), 3);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(1), Value::Text("note-a".to_string())]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(2), Value::Text("note-b".to_string())]
    );
    assert_eq!(
        result.rows()[2].values(),
        &[Value::Int64(2), Value::Text("note-c".to_string())]
    );
}

#[test]
fn simple_hashed_full_join_without_index_uses_fast_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE docs (id INT64 PRIMARY KEY, body TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE archive (id INT64 PRIMARY KEY, doc_id INT64, note TEXT)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO docs (id, body) VALUES (8, 'doc-8')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO docs (id, body) VALUES (9, 'doc-9')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO archive (id, doc_id, note) VALUES (1000, 8, 'match')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO archive (id, doc_id, note) VALUES (1001, 99, 'orphan')",
    );

    let statement = parse_sql_statement(
        "SELECT docs.id, archive.id \
             FROM docs FULL JOIN archive ON docs.id = archive.doc_id \
             ORDER BY COALESCE(docs.id, archive.doc_id) ASC",
    )
    .expect("parse hashed full join");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_simple_indexed_join_projection_query(query, &[])
        .expect("execute hashed full join")
        .expect("hashed full join should stay on fast path");

    assert_eq!(result.columns(), &["id".to_string(), "id".to_string()]);
    assert_eq!(result.rows().len(), 3);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(8), Value::Int64(1000)]
    );
    assert_eq!(result.rows()[1].values(), &[Value::Int64(9), Value::Null]);
    assert_eq!(
        result.rows()[2].values(),
        &[Value::Null, Value::Int64(1001)]
    );
}

#[test]
fn simple_indexed_join_with_expression_filter_uses_fast_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE docs (id INT64 PRIMARY KEY, body TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE archive (id INT64 PRIMARY KEY, doc_id INT64, note TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX archive_doc_idx ON archive (doc_id)",
    );
    for id in 1..=3 {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO docs (id, body) VALUES ({id}, 'doc-{id}')"),
        );
    }
    for (id, doc_id, note) in [
        (1000, 1, "note-a"),
        (1001, 2, "note-b"),
        (1002, 2, "note-c"),
    ] {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO archive (id, doc_id, note) VALUES ({id}, {doc_id}, '{note}')"),
        );
    }

    let statement = parse_sql_statement(
        "SELECT docs.id, archive.note \
             FROM docs JOIN archive ON docs.id = archive.doc_id \
             WHERE docs.id + archive.doc_id >= 4 \
             ORDER BY docs.id ASC, archive.note ASC",
    )
    .expect("parse indexed join with expression filter");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_simple_indexed_join_projection_query(query, &[])
        .expect("execute indexed join with expression filter")
        .expect("indexed join with expression filter should stay on fast path");

    assert_eq!(result.columns(), &["id".to_string(), "note".to_string()]);
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(2), Value::Text("note-b".to_string())]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(2), Value::Text("note-c".to_string())]
    );
}

#[test]
fn composite_indexed_join_without_filter_uses_indexed_join_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE docs (id INT64 PRIMARY KEY, org_id INT64, body TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE archive (id INT64 PRIMARY KEY, org_id INT64, doc_id INT64, note TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX idx_archive_org_doc ON archive(org_id, doc_id)",
    );
    for (id, org_id, body) in [(1, 10, "doc-a"), (2, 10, "doc-b"), (3, 20, "doc-c")] {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO docs (id, org_id, body) VALUES ({id}, {org_id}, '{body}')"),
        );
    }
    for (id, org_id, doc_id, note) in [
        (1000, 10, 1, "note-a"),
        (1001, 10, 2, "note-b"),
        (1002, 20, 9, "note-z"),
    ] {
        execute_sql(
                &mut runtime,
                &format!(
                    "INSERT INTO archive (id, org_id, doc_id, note) VALUES ({id}, {org_id}, {doc_id}, '{note}')"
                ),
            );
    }

    let statement = parse_sql_statement(
        "SELECT docs.id, archive.note \
             FROM docs \
             JOIN archive ON docs.org_id = archive.org_id AND docs.id = archive.doc_id",
    )
    .expect("parse composite indexed join");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };
    let crate::sql::ast::QueryBody::Select(select) = &query.body else {
        panic!("expected select query");
    };
    let [FromItem::Join {
        left,
        right,
        kind,
        constraint,
    }] = select.from.as_slice()
    else {
        panic!("expected join from item");
    };

    let left_dataset = runtime
        .evaluate_from_item(left, &[], &BTreeMap::new())
        .expect("evaluate left input");
    let dataset = runtime
        .try_indexed_equi_join_with_right_table(
            &left_dataset,
            right,
            *kind,
            constraint,
            &BTreeMap::new(),
        )
        .expect("execute composite indexed equi-join")
        .expect("composite indexed join should stay on indexed join path");
    let result = runtime
        .project_dataset(&dataset, &select.projection, &[], &BTreeMap::new(), None)
        .expect("project composite indexed join result");

    assert_eq!(
        result.columns,
        vec![
            ColumnBinding::visible(None, "id".to_string()),
            ColumnBinding::visible(None, "note".to_string()),
        ]
    );
    assert_eq!(result.rows.len(), 2);
    assert_eq!(
        result.rows[0],
        vec![Value::Int64(1), Value::Text("note-a".to_string())]
    );
    assert_eq!(
        result.rows[1],
        vec![Value::Int64(2), Value::Text("note-b".to_string())]
    );
}

#[test]
fn composite_hashed_join_without_filter_uses_join_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE docs (id INT64 PRIMARY KEY, org_id INT64, body TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE archive (id INT64 PRIMARY KEY, org_id INT64, doc_id INT64, note TEXT)",
    );
    for (id, org_id, body) in [(1, 10, "doc-a"), (2, 10, "doc-b"), (9, 20, "doc-z")] {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO docs (id, org_id, body) VALUES ({id}, {org_id}, '{body}')"),
        );
    }
    for (id, org_id, doc_id, note) in [
        (1000, 10, 1, "note-a"),
        (1001, 10, 2, "note-b"),
        (1002, 20, 9, "note-z"),
    ] {
        execute_sql(
                &mut runtime,
                &format!(
                    "INSERT INTO archive (id, org_id, doc_id, note) VALUES ({id}, {org_id}, {doc_id}, '{note}')"
                ),
            );
    }

    let statement = parse_sql_statement(
        "SELECT docs.id, archive.note \
             FROM docs \
             JOIN archive ON docs.org_id = archive.org_id AND docs.id = archive.doc_id",
    )
    .expect("parse composite hashed join");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };
    let crate::sql::ast::QueryBody::Select(select) = &query.body else {
        panic!("expected select query");
    };
    let [FromItem::Join {
        left,
        right,
        kind,
        constraint,
    }] = select.from.as_slice()
    else {
        panic!("expected join from item");
    };

    let left_dataset = runtime
        .evaluate_from_item(left, &[], &BTreeMap::new())
        .expect("evaluate left input");
    let dataset = runtime
        .try_indexed_equi_join_with_right_table(
            &left_dataset,
            right,
            *kind,
            constraint,
            &BTreeMap::new(),
        )
        .expect("execute composite hashed equi-join")
        .expect("composite hashed join should stay on join path");
    let result = runtime
        .project_dataset(&dataset, &select.projection, &[], &BTreeMap::new(), None)
        .expect("project composite hashed join result");

    assert_eq!(result.rows.len(), 3);
    assert_eq!(
        result.rows[0],
        vec![Value::Int64(1), Value::Text("note-a".to_string())]
    );
    assert_eq!(
        result.rows[1],
        vec![Value::Int64(2), Value::Text("note-b".to_string())]
    );
    assert_eq!(
        result.rows[2],
        vec![Value::Int64(9), Value::Text("note-z".to_string())]
    );
}

#[test]
fn composite_indexed_join_with_filter_uses_indexed_join_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE docs (id INT64 PRIMARY KEY, org_id INT64, body TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE archive (id INT64 PRIMARY KEY, org_id INT64, doc_id INT64, note TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX idx_archive_org_doc ON archive(org_id, doc_id)",
    );
    for (id, org_id, body) in [(1, 10, "doc-a"), (2, 10, "doc-b"), (3, 20, "doc-c")] {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO docs (id, org_id, body) VALUES ({id}, {org_id}, '{body}')"),
        );
    }
    for (id, org_id, doc_id, note) in [
        (1000, 10, 1, "note-a"),
        (1001, 10, 2, "note-b"),
        (1002, 20, 3, "note-c"),
    ] {
        execute_sql(
                &mut runtime,
                &format!(
                    "INSERT INTO archive (id, org_id, doc_id, note) VALUES ({id}, {org_id}, {doc_id}, '{note}')"
                ),
            );
    }

    let statement = parse_sql_statement(
        "SELECT docs.id, archive.note \
             FROM docs \
             JOIN archive ON docs.org_id = archive.org_id AND docs.id = archive.doc_id \
             WHERE docs.id = 2",
    )
    .expect("parse composite indexed join query");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };
    let crate::sql::ast::QueryBody::Select(select) = &query.body else {
        panic!("expected select query");
    };

    let dataset = runtime
        .try_indexed_join(select, &[], &BTreeMap::new())
        .expect("execute composite indexed join query")
        .expect("composite indexed join query should stay on indexed join path");
    let result = runtime
        .project_dataset(&dataset, &select.projection, &[], &BTreeMap::new(), None)
        .expect("project composite indexed join query result");

    assert_eq!(
        result.columns,
        vec![
            ColumnBinding::visible(None, "id".to_string()),
            ColumnBinding::visible(None, "note".to_string()),
        ]
    );
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0],
        vec![Value::Int64(2), Value::Text("note-b".to_string())]
    );
}

#[test]
fn composite_hashed_join_with_filter_uses_join_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE docs (id INT64 PRIMARY KEY, org_id INT64, body TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE archive (id INT64 PRIMARY KEY, org_id INT64, doc_id INT64, note TEXT)",
    );
    for (id, org_id, body) in [(1, 10, "doc-a"), (2, 10, "doc-b"), (3, 20, "doc-c")] {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO docs (id, org_id, body) VALUES ({id}, {org_id}, '{body}')"),
        );
    }
    for (id, org_id, doc_id, note) in [
        (1000, 10, 1, "note-a"),
        (1001, 10, 2, "note-b"),
        (1002, 20, 3, "note-c"),
    ] {
        execute_sql(
                &mut runtime,
                &format!(
                    "INSERT INTO archive (id, org_id, doc_id, note) VALUES ({id}, {org_id}, {doc_id}, '{note}')"
                ),
            );
    }

    let statement = parse_sql_statement(
        "SELECT docs.id, archive.note \
             FROM docs \
             JOIN archive ON docs.org_id = archive.org_id AND docs.id = archive.doc_id \
             WHERE docs.id = 2",
    )
    .expect("parse composite hashed join query");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };
    let crate::sql::ast::QueryBody::Select(select) = &query.body else {
        panic!("expected select query");
    };

    let dataset = runtime
        .try_indexed_join(select, &[], &BTreeMap::new())
        .expect("execute composite hashed join query")
        .expect("composite hashed join query should stay on filtered join path");
    let result = runtime
        .project_dataset(&dataset, &select.projection, &[], &BTreeMap::new(), None)
        .expect("project composite hashed join query result");

    assert_eq!(
        result.columns,
        vec![
            ColumnBinding::visible(None, "id".to_string()),
            ColumnBinding::visible(None, "note".to_string()),
        ]
    );
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0],
        vec![Value::Int64(2), Value::Text("note-b".to_string())]
    );
}

#[test]
fn aggregate_join_query_uses_grouped_evaluation() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE artists (id INT64 PRIMARY KEY, name TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE albums (id INT64 PRIMARY KEY, artist_id INT64, name TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE songs (id INT64 PRIMARY KEY, album_id INT64, title TEXT)",
    );
    execute_sql(&mut runtime, "INSERT INTO artists VALUES (1, 'one')");
    execute_sql(&mut runtime, "INSERT INTO artists VALUES (2, 'two')");
    execute_sql(&mut runtime, "INSERT INTO albums VALUES (10, 1, 'first')");
    execute_sql(&mut runtime, "INSERT INTO albums VALUES (20, 2, 'second')");
    execute_sql(&mut runtime, "INSERT INTO songs VALUES (100, 10, 'alpha')");
    execute_sql(&mut runtime, "INSERT INTO songs VALUES (101, 10, 'beta')");
    execute_sql(&mut runtime, "INSERT INTO songs VALUES (200, 20, 'gamma')");

    let statement = parse_sql_statement(
        "SELECT COUNT(*) \
             FROM songs AS s \
             INNER JOIN albums AS a ON s.album_id = a.id \
             WHERE a.artist_id = 1",
    )
    .expect("parse aggregate join query");
    let result = runtime
        .execute_statement(&statement, &[], PAGE_SIZE)
        .expect("execute aggregate join query");

    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].values(), &[Value::Int64(2)]);
}

#[test]
fn base_table_join_filters_against_join_scope_before_projection() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE shares (id INT64 PRIMARY KEY, user_id INT64, share_unique_id TEXT)",
    );
    execute_sql(&mut runtime, "INSERT INTO users VALUES (1, 'user-one')");
    execute_sql(&mut runtime, "INSERT INTO users VALUES (2, 'user-two')");
    execute_sql(&mut runtime, "INSERT INTO shares VALUES (10, 1, 'target')");
    execute_sql(&mut runtime, "INSERT INTO shares VALUES (20, 2, 'other')");

    let statement = parse_sql_statement(
        "SELECT s.id, u.name \
             FROM shares AS s \
             INNER JOIN users AS u ON s.user_id = u.id \
             WHERE s.share_unique_id = 'target' \
             LIMIT 1",
    )
    .expect("parse filtered join query");
    let result = runtime
        .execute_statement(&statement, &[], PAGE_SIZE)
        .expect("execute filtered join query");

    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(10), Value::Text("user-one".to_string())]
    );
}

#[test]
fn simple_indexed_left_join_without_filter_uses_fast_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE docs (id INT64 PRIMARY KEY, body TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE archive (id INT64 PRIMARY KEY, doc_id INT64, note TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX archive_doc_idx ON archive (doc_id)",
    );
    for id in 1..=3 {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO docs (id, body) VALUES ({id}, 'doc-{id}')"),
        );
    }
    for (id, doc_id, note) in [(1000, 1, "note-a"), (1001, 2, "note-b")] {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO archive (id, doc_id, note) VALUES ({id}, {doc_id}, '{note}')"),
        );
    }

    let statement = parse_sql_statement(
        "SELECT docs.id, archive.note \
             FROM docs LEFT JOIN archive ON docs.id = archive.doc_id \
             ORDER BY docs.id ASC",
    )
    .expect("parse indexed left join without filter");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_simple_indexed_join_projection_query(query, &[])
        .expect("execute indexed left join without filter")
        .expect("indexed left join without filter should stay on fast path");

    assert_eq!(result.columns(), &["id".to_string(), "note".to_string()]);
    assert_eq!(result.rows().len(), 3);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(1), Value::Text("note-a".to_string())]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(2), Value::Text("note-b".to_string())]
    );
    assert_eq!(result.rows()[2].values(), &[Value::Int64(3), Value::Null]);
}

#[test]
fn simple_indexed_full_join_without_filter_uses_fast_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE docs (id INT64 PRIMARY KEY, body TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE archive (id INT64 PRIMARY KEY, doc_id INT64, note TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX archive_doc_idx ON archive (doc_id)",
    );
    for id in 1..=2 {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO docs (id, body) VALUES ({id}, 'doc-{id}')"),
        );
    }
    for (id, doc_id, note) in [(1000, 1, "note-a"), (1001, 99, "orphan")] {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO archive (id, doc_id, note) VALUES ({id}, {doc_id}, '{note}')"),
        );
    }

    let statement = parse_sql_statement(
        "SELECT docs.id, archive.note, COALESCE(docs.id, archive.doc_id) AS sort_key \
             FROM docs FULL JOIN archive ON docs.id = archive.doc_id \
             ORDER BY sort_key ASC",
    )
    .expect("parse indexed full join without filter");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query statement");
    };

    let result = runtime
        .try_execute_simple_indexed_join_projection_query(query, &[])
        .expect("execute indexed full join without filter")
        .expect("indexed full join without filter should stay on fast path");

    assert_eq!(
        result.columns(),
        &["id".to_string(), "note".to_string(), "sort_key".to_string()]
    );
    assert_eq!(result.rows().len(), 3);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(1),
            Value::Text("note-a".to_string()),
            Value::Int64(1)
        ]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(2), Value::Null, Value::Int64(2)]
    );
    assert_eq!(
        result.rows()[2].values(),
        &[
            Value::Null,
            Value::Text("orphan".to_string()),
            Value::Int64(99)
        ]
    );
}

#[test]
fn legacy_runtime_payload_decode_still_round_trips() {
    let mut runtime = EngineRuntime::empty(7);
    execute_sql(
        &mut runtime,
        "CREATE TABLE docs (id INT64 PRIMARY KEY, email TEXT, body TEXT)",
    );
    execute_sql(&mut runtime, "CREATE INDEX docs_email_idx ON docs (email)");
    execute_sql(
        &mut runtime,
        "INSERT INTO docs (id, email, body) VALUES (1, 'a@example.com', 'alphabet soup')",
    );

    let payload = encode_runtime_payload(&runtime).expect("encode legacy runtime payload");
    let decoded = decode_runtime_payload(&payload).expect("decode legacy runtime payload");

    assert_eq!(decoded.catalog.schema_cookie, runtime.catalog.schema_cookie);
    assert_eq!(decoded.tables["docs"], runtime.tables["docs"]);
    assert!(decoded.catalog.indexes.contains_key("docs_email_idx"));
}

#[test]
fn manifest_payload_decode_loads_per_table_rows() {
    let mut runtime = EngineRuntime::empty(9);
    execute_sql(
        &mut runtime,
        "CREATE TABLE docs (id INT64 PRIMARY KEY, email TEXT, body TEXT)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO docs (id, email, body) VALUES (1, 'a@example.com', 'alphabet soup')",
    );

    let mut store = InMemoryPageStore::new(PAGE_SIZE);
    let table_payload =
        encode_table_payload(runtime.tables["docs"].resident_data()).expect("encode table payload");
    let pointer =
        crate::record::overflow::write_overflow(&mut store, &table_payload, CompressionMode::Auto)
            .expect("write table payload");
    let tail = crate::record::overflow::read_uncompressed_overflow_tail(&store, pointer)
        .expect("read table tail")
        .expect("table tail");
    let mut table_states = runtime.persisted_tables.as_ref().clone();
    table_states.insert(
        "docs".to_string(),
        PersistedTableState {
            pointer,
            checksum: crate::storage::checksum::crc32c_parts(&[table_payload.as_slice()]),
            row_count: runtime.tables["docs"].resident_data().rows.len(),
            tail,
            pk_index_root: None,
        },
    );

    let manifest =
        encode_manifest_payload(&runtime, &table_states).expect("encode manifest payload");
    let decoded = decode_manifest_payload(&store, &manifest).expect("decode manifest payload");

    assert_eq!(decoded.catalog.schema_cookie, runtime.catalog.schema_cookie);
    // Table data is deferred — "docs" should not be in tables yet.
    assert!(decoded.deferred_tables.contains("docs"));
    assert!(!decoded.tables.contains_key("docs"));
    // Schema and persisted state (pointer, checksum) are available immediately.
    assert!(decoded.catalog.tables.contains_key("docs"));
    assert_eq!(
        decoded.persisted_tables["docs"].pointer,
        table_states["docs"].pointer
    );
    assert_eq!(
        decoded.persisted_tables["docs"].checksum,
        table_states["docs"].checksum
    );
}

#[test]
fn multi_chunk_paged_payload_reconstructs_legacy_table_payload() {
    let body = "x".repeat(2048);
    let data = TableData::from_rows(
        (0_i64..96_i64)
            .map(|row_id| StoredRow {
                row_id: row_id + 1,
                values: vec![
                    Value::Int64(row_id),
                    Value::Text(body.clone()),
                    Value::Int64(row_id * 10),
                ],
            })
            .collect(),
    );

    let encoded_chunks =
        encode_paged_table_chunks(&data, PAGE_SIZE).expect("encode paged table chunks");
    assert!(
        encoded_chunks.len() > 1,
        "expected large table to be split across multiple paged chunks"
    );
    let chunk_payloads = encoded_chunks
        .into_iter()
        .map(|chunk| Arc::new(chunk.payload))
        .collect::<Vec<_>>();

    let chunk_manifest = TablePageManifest::from_chunks(
        chunk_payloads
            .into_iter()
            .map(|payload| TablePageManifestChunk {
                pointer: OverflowPointer {
                    head_page_id: 0,
                    logical_len: 0,
                    flags: 0,
                },
                checksum: 0,
                row_count: 0,
                payload,
                tombstoned_row_ids: Arc::new(BTreeSet::new()),
                overlay_pointer: None,
                overlay_checksum: None,
                overlay_payload: None,
            })
            .collect(),
    )
    .expect("build chunk manifest");

    let reconstructed = encode_legacy_table_payload_from_manifest(&chunk_manifest)
        .expect("reconstruct legacy payload");
    let single_payload = encode_table_payload(&data).expect("encode legacy payload");

    assert_eq!(reconstructed, single_payload);
}

#[test]
fn deferred_row_lookup_reads_compressed_table_payload() {
    let data = TableData::from_rows(
        (1_i64..=4_i64)
            .map(|row_id| StoredRow {
                row_id,
                values: vec![Value::Int64(row_id), Value::Text("x".repeat(2048))],
            })
            .collect(),
    );
    let payload = encode_table_payload(&data).expect("encode table payload");
    let mut store = InMemoryPageStore::new(PAGE_SIZE);
    let pointer = crate::record::overflow::write_overflow(
        &mut store,
        &payload,
        CompressionMode::AutoMinBytes(1),
    )
    .expect("write compressed table payload");
    assert!(
        pointer.is_compressed(),
        "expected compressed overflow pointer"
    );
    let state = PersistedTableState {
        pointer,
        checksum: crc32c_parts(&[payload.as_slice()]),
        row_count: data.rows.len(),
        ..PersistedTableState::default()
    };

    let row = read_deferred_row_by_id_from_table_payload(&store, state, 3)
        .expect("read deferred row")
        .expect("row should exist");
    assert_eq!(row.row_id, 3);
    assert_eq!(row.values, data.rows[2].values);
    assert!(
        read_deferred_row_by_id_from_table_payload(&store, state, 99)
            .expect("read missing deferred row")
            .is_none()
    );
}

#[test]
fn persist_paged_table_writes_multi_chunk_manifest() {
    let body = "x".repeat(2048);
    let data = TableData::from_rows(
        (0_i64..96_i64)
            .map(|row_id| StoredRow {
                row_id: row_id + 1,
                values: vec![Value::Int64(row_id), Value::Text(body.clone())],
            })
            .collect(),
    );
    let mut store = InMemoryPageStore::new(PAGE_SIZE);

    let encoded_chunks =
        encode_paged_table_chunks(&data, PAGE_SIZE).expect("encode paged table chunks");
    let state = persist_paged_table(
        &mut store,
        PersistedTableState::default(),
        &encoded_chunks,
        data.rows.len(),
    )
    .expect("persist paged table");
    assert!(state.pointer.is_table_paged_manifest());
    assert_eq!(state.row_count, data.rows.len());

    let manifest_payload =
        crate::record::overflow::read_overflow(&store, state.pointer).expect("read manifest");
    let persisted_manifest =
        decode_paged_table_manifest_payload(&manifest_payload).expect("decode paged manifest");
    assert!(
        persisted_manifest.chunks.len() > 1,
        "expected persisted paged table to span multiple chunks"
    );

    let decoded_manifest =
        read_table_page_manifest_from_state(&store, state).expect("read paged table manifest");
    assert_eq!(decoded_manifest.row_count(), data.rows.len());
    let last_row = decoded_manifest
        .row_by_id(96)
        .expect("read row by id")
        .expect("row should exist");
    assert_eq!(
        last_row.values(),
        &[Value::Int64(95), Value::Text(body.clone())]
    );
}

#[test]
fn append_paged_table_chunks_preserves_existing_chunk_pointers() {
    let body = "x".repeat(2048);
    let mut store = InMemoryPageStore::new(PAGE_SIZE);
    let initial = TableData::from_rows(
        (0_i64..48_i64)
            .map(|row_id| StoredRow {
                row_id: row_id + 1,
                values: vec![Value::Int64(row_id), Value::Text(body.clone())],
            })
            .collect(),
    );
    let initial_chunks =
        encode_paged_table_chunks(&initial, PAGE_SIZE).expect("encode initial chunks");
    let initial_state = persist_paged_table(
        &mut store,
        PersistedTableState::default(),
        &initial_chunks,
        initial.rows.len(),
    )
    .expect("persist initial paged table");
    let initial_manifest_payload =
        crate::record::overflow::read_overflow(&store, initial_state.pointer)
            .expect("read initial manifest");
    let initial_manifest = decode_paged_table_manifest_payload(&initial_manifest_payload)
        .expect("decode initial manifest");
    let initial_chunk_pointers = initial_manifest
        .chunks
        .iter()
        .map(|chunk| chunk.pointer)
        .collect::<Vec<_>>();

    let appended_rows = (48_i64..96_i64)
        .map(|row_id| StoredRow {
            row_id: row_id + 1,
            values: vec![Value::Int64(row_id), Value::Text(body.clone())],
        })
        .collect::<Vec<_>>();
    let appended_chunks = encode_paged_table_chunks_from_rows(&appended_rows, PAGE_SIZE)
        .expect("encode appended chunks");
    let appended_state = append_paged_table_chunks(&mut store, initial_state, &appended_chunks, 96)
        .expect("append paged chunks");
    let appended_manifest_payload =
        crate::record::overflow::read_overflow(&store, appended_state.pointer)
            .expect("read appended manifest");
    let appended_manifest = decode_paged_table_manifest_payload(&appended_manifest_payload)
        .expect("decode appended manifest");

    assert!(
        appended_manifest.chunks.len() > initial_chunk_pointers.len(),
        "expected append path to add new chunk entries"
    );
    assert_eq!(
        appended_manifest.chunks[..initial_chunk_pointers.len()]
            .iter()
            .map(|chunk| chunk.pointer)
            .collect::<Vec<_>>(),
        initial_chunk_pointers,
        "append path should preserve existing paged chunk pointers"
    );
}

#[test]
fn append_only_paged_manifest_rewrite_preserves_untouched_chunk_pointers() {
    let body = "x".repeat(2048);
    let mut store = InMemoryPageStore::new(PAGE_SIZE);
    let initial = TableData::from_rows(
        (0_i64..96_i64)
            .map(|row_id| StoredRow {
                row_id: row_id + 1,
                values: vec![Value::Int64(row_id), Value::Text(body.clone())],
            })
            .collect(),
    );
    let initial_chunks =
        encode_paged_table_chunks(&initial, PAGE_SIZE).expect("encode initial chunks");
    let initial_state = persist_paged_table(
        &mut store,
        PersistedTableState::default(),
        &initial_chunks,
        initial.rows.len(),
    )
    .expect("persist initial paged table");
    let initial_manifest_payload =
        crate::record::overflow::read_overflow(&store, initial_state.pointer)
            .expect("read initial manifest");
    let initial_manifest = decode_paged_table_manifest_payload(&initial_manifest_payload)
        .expect("decode initial manifest");
    assert!(
        initial_manifest.chunks.len() > 2,
        "expected multiple chunks to observe pointer preservation"
    );
    let untouched_pointers = initial_manifest
        .chunks
        .iter()
        .take(initial_manifest.chunks.len() - 1)
        .map(|chunk| chunk.pointer)
        .collect::<Vec<_>>();

    let mut page_manifest =
        read_table_page_manifest_from_state(&store, initial_state).expect("read manifest");
    for row_id in 96_i64..144_i64 {
        page_manifest
            .append_row(
                &StoredRow {
                    row_id: row_id + 1,
                    values: vec![Value::Int64(row_id), Value::Text(body.clone())],
                },
                PAGE_SIZE,
            )
            .expect("append row to paged manifest");
    }

    let (appended_state, persisted_chunks) =
        try_append_only_paged_table_from_manifest(&mut store, initial_state, &page_manifest)
            .expect("append-only paged manifest rewrite")
            .expect("append-only path should handle tail rewrite and new chunks");
    let appended_manifest_payload =
        crate::record::overflow::read_overflow(&store, appended_state.pointer)
            .expect("read appended manifest");
    let appended_manifest = decode_paged_table_manifest_payload(&appended_manifest_payload)
        .expect("decode appended manifest");
    let appended_page_manifest =
        read_table_page_manifest_from_state(&store, appended_state).expect("read appended");
    let preserved_untouched = appended_manifest
        .chunks
        .iter()
        .take(untouched_pointers.len())
        .map(|chunk| chunk.pointer)
        .collect::<Vec<_>>();
    let last_row = appended_page_manifest
        .row_by_id(144)
        .expect("read last row")
        .expect("last row should exist");

    assert_eq!(appended_state.row_count, 144);
    assert_eq!(persisted_chunks.len(), appended_manifest.chunks.len());
    assert_eq!(
        preserved_untouched, untouched_pointers,
        "append-only manifest rewrite should preserve chunks before the previous tail"
    );
    assert_eq!(
        last_row.values(),
        &[Value::Int64(143), Value::Text(body.clone())]
    );
}

#[test]
fn rewrite_paged_table_from_resident_preserves_untouched_chunk_pointers() {
    let body = "x".repeat(2048);
    let mut store = InMemoryPageStore::new(PAGE_SIZE);
    let initial = TableData::from_rows(
        (0_i64..96_i64)
            .map(|row_id| StoredRow {
                row_id: row_id + 1,
                values: vec![Value::Int64(row_id), Value::Text(body.clone())],
            })
            .collect(),
    );
    let initial_chunks =
        encode_paged_table_chunks(&initial, PAGE_SIZE).expect("encode initial chunks");
    let initial_state = persist_paged_table(
        &mut store,
        PersistedTableState::default(),
        &initial_chunks,
        initial.rows.len(),
    )
    .expect("persist initial paged table");
    let initial_manifest_payload =
        crate::record::overflow::read_overflow(&store, initial_state.pointer)
            .expect("read initial manifest");
    let initial_manifest = decode_paged_table_manifest_payload(&initial_manifest_payload)
        .expect("decode initial manifest");
    assert!(
        initial_manifest.chunks.len() > 2,
        "expected multiple chunks to observe pointer preservation"
    );
    let initial_untouched_pointers = initial_manifest
        .chunks
        .iter()
        .skip(1)
        .map(|chunk| chunk.pointer)
        .collect::<Vec<_>>();

    let mut updated = initial.clone();
    updated
        .replace_value(5, 0, Value::Int64(500))
        .expect("replace integer value");
    updated
        .replace_value(5, 1, Value::Text("y".repeat(2600)))
        .expect("replace text value");
    updated.remove_row(6);

    let rewritten_state =
        rewrite_paged_table_from_resident(&mut store, initial_state, &updated, PAGE_SIZE)
            .expect("rewrite paged table from resident rows");
    let rewritten_manifest_payload =
        crate::record::overflow::read_overflow(&store, rewritten_state.pointer)
            .expect("read rewritten manifest");
    let rewritten_manifest = decode_paged_table_manifest_payload(&rewritten_manifest_payload)
        .expect("decode rewritten manifest");
    let preserved_untouched = rewritten_manifest
        .chunks
        .iter()
        .filter_map(|chunk| {
            initial_untouched_pointers
                .contains(&chunk.pointer)
                .then_some(chunk.pointer)
        })
        .collect::<Vec<_>>();

    assert_eq!(rewritten_state.row_count, updated.rows.len());
    assert_eq!(
        preserved_untouched, initial_untouched_pointers,
        "unchanged paged chunks should retain their original pointers in order"
    );
}

#[test]
fn persist_to_db_resident_paged_row_updates_preserves_untouched_chunk_pointers() {
    let body = "x".repeat(2048);
    let config = DbConfig {
        paged_row_storage: true,
        defer_table_materialization: false,
        ..DbConfig::default()
    };
    let db = Db::open_or_create(":memory:", config).expect("open db");
    db.execute("CREATE TABLE docs (id INT64 PRIMARY KEY, body TEXT)")
        .expect("create table");
    for row_id in 1_i64..=96_i64 {
        db.execute(&format!(
            "INSERT INTO docs (id, body) VALUES ({row_id}, '{}')",
            body
        ))
        .expect("insert row");
    }

    let mut runtime = db.debug_engine_snapshot().expect("snapshot runtime");
    let initial_state = runtime.persisted_tables["docs"];
    let store = DbTxnPageStore { db: &db };
    db.begin_write().expect("begin write transaction");
    let initial_manifest_payload =
        crate::record::overflow::read_overflow(&store, initial_state.pointer)
            .expect("read manifest payload");
    let initial_manifest = decode_paged_table_manifest_payload(&initial_manifest_payload)
        .expect("decode manifest payload");
    let initial_page_manifest =
        read_table_page_manifest_from_state(&store, initial_state).expect("read manifest");
    db.commit().expect("commit write transaction");
    assert!(
        initial_manifest.chunks.len() > 2,
        "expected multiple chunks to observe pointer preservation"
    );
    let changed_chunk_index = initial_page_manifest.rows[5].chunk_index as usize;
    let untouched_pointers = initial_manifest
        .chunks
        .iter()
        .enumerate()
        .filter_map(|(index, chunk)| (index != changed_chunk_index).then_some(chunk.pointer))
        .collect::<Vec<_>>();

    let updated_body = "y".repeat(2600);
    let statement = parse_sql_statement(&format!(
        "UPDATE docs SET body = '{}' WHERE id = 6",
        updated_body
    ))
    .expect("parse update");
    let crate::sql::ast::Statement::Update(update) = statement else {
        panic!("expected update statement");
    };
    let prepared = runtime
        .prepare_simple_update(&update)
        .expect("prepare update")
        .expect("expected prepared update");
    runtime
        .execute_prepared_simple_update(&prepared, &[], PAGE_SIZE)
        .expect("execute prepared update");
    assert_eq!(
        runtime
            .paged_mutations
            .get("docs")
            .map(|m| m.updated_rows.len()),
        Some(1)
    );

    db.begin_write().expect("begin write txn");
    runtime.persist_to_db(&db).expect("persist runtime");
    db.commit().expect("commit write txn");

    let rewritten_state = runtime.persisted_tables["docs"];
    db.begin_write().expect("begin write transaction");
    let rewritten_manifest_payload =
        crate::record::overflow::read_overflow(&store, rewritten_state.pointer)
            .expect("read manifest payload");
    let rewritten_manifest = decode_paged_table_manifest_payload(&rewritten_manifest_payload)
        .expect("decode manifest payload");
    let rewritten_page_manifest =
        read_table_page_manifest_from_state(&store, rewritten_state).expect("read manifest");
    db.commit().expect("commit write transaction");
    let preserved_untouched = rewritten_manifest
        .chunks
        .iter()
        .filter_map(|chunk| {
            untouched_pointers
                .contains(&chunk.pointer)
                .then_some(chunk.pointer)
        })
        .collect::<Vec<_>>();
    let updated_row = rewritten_page_manifest
        .row_by_id(6)
        .expect("read updated row")
        .expect("row should exist");

    assert_eq!(
        preserved_untouched, untouched_pointers,
        "persisted paged row updates should preserve untouched chunk pointers"
    );
    assert_eq!(
        updated_row.values(),
        &[Value::Int64(6), Value::Text(updated_body)]
    );
}

#[test]
fn prepared_simple_update_multiple_assignments_updates_indexed_queries() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE issues (
            id INT64 PRIMARY KEY,
            project_id INT64,
            status TEXT,
            title TEXT,
            updated_at INT64
        )",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX idx_issues_status ON issues (status)",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX idx_issues_project_status ON issues (project_id, status)",
    );

    execute_sql(
        &mut runtime,
        "INSERT INTO issues (id, project_id, status, title, updated_at) VALUES (1, 10, 'open', 'issue-1', 100)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO issues (id, project_id, status, title, updated_at) VALUES (2, 10, 'closed', 'issue-2', 200)",
    );

    let statement =
        parse_sql_statement("UPDATE issues SET status = $1, updated_at = $2 WHERE id = $3")
            .expect("parse update");
    let crate::sql::ast::Statement::Update(update) = &statement else {
        panic!("expected update");
    };
    let prepared = runtime
        .prepare_simple_update(update)
        .expect("prepare update")
        .expect("expected prepared update");
    runtime
        .execute_prepared_simple_update(
            &prepared,
            &[
                Value::Text("resolved".to_string()),
                Value::Int64(300),
                Value::Int64(1),
            ],
            PAGE_SIZE,
        )
        .expect("execute prepared update");

    let statement = parse_sql_statement(
        "SELECT id, status FROM issues WHERE project_id = $1 AND status = $2 ORDER BY id",
    )
    .expect("parse indexed query");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query");
    };
    let result = runtime
        .try_execute_simple_indexed_projection_query(
            query,
            &[Value::Int64(10), Value::Text("resolved".to_string())],
        )
        .expect("execute indexed projection")
        .expect("query should remain on indexed projection path");
    assert_eq!(
        result
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![vec![Value::Int64(1), Value::Text("resolved".to_string())]]
    );

    let statement =
        parse_sql_statement("SELECT id, status FROM issues WHERE status = $1 ORDER BY id")
            .expect("parse status-only query");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query");
    };
    let old_status = runtime
        .try_execute_simple_indexed_projection_query(query, &[Value::Text("open".to_string())])
        .expect("execute old-status query")
        .expect("old status query should remain on indexed projection path");
    assert!(old_status.rows().is_empty());
}

#[test]
fn persist_to_db_single_payload_update_succeeds_after_cached_payload_miss() {
    let config = DbConfig {
        paged_row_storage: false,
        defer_table_materialization: false,
        ..DbConfig::default()
    };
    let db = Db::open_or_create(":memory:", config).expect("open db");
    db.execute("CREATE TABLE docs (id INT64 PRIMARY KEY, body TEXT)")
        .expect("create table");
    for row_id in 1_i64..=8_i64 {
        db.execute(&format!(
            "INSERT INTO docs (id, body) VALUES ({row_id}, 'x')"
        ))
        .expect("insert row");
    }

    let mut runtime = db.debug_engine_snapshot().expect("snapshot runtime");
    runtime.cache_payload_remove("docs");
    assert!(runtime.cached_payload("docs").is_none());

    let statement =
        parse_sql_statement("UPDATE docs SET body = 'updated' WHERE id = 4").expect("parse update");
    let crate::sql::ast::Statement::Update(update) = statement else {
        panic!("expected update");
    };
    let prepared = runtime
        .prepare_simple_update(&update)
        .expect("prepare update")
        .expect("expected prepared update");
    runtime
        .execute_prepared_simple_update(&prepared, &[], PAGE_SIZE)
        .expect("execute prepared update");

    db.begin_write().expect("begin write txn");
    runtime.persist_to_db(&db).expect("persist runtime");
    db.commit().expect("commit write txn");

    assert!(runtime.cached_payload("docs").is_some());

    let updated = db
        .execute("SELECT body FROM docs WHERE id = 4")
        .expect("select updated row")
        .rows()
        .first()
        .and_then(|row| row.values().first())
        .cloned();
    assert_eq!(updated, Some(Value::Text("updated".to_string())));

    let untouched = db
        .execute("SELECT body FROM docs WHERE id = 3")
        .expect("select untouched row")
        .rows()
        .first()
        .and_then(|row| row.values().first())
        .cloned();
    assert_eq!(untouched, Some(Value::Text("x".to_string())));

    let mut reloaded_runtime = db.debug_engine_snapshot().expect("reload snapshot runtime");
    assert!(reloaded_runtime.cached_payload("docs").is_none());

    let reload_statement = parse_sql_statement("UPDATE docs SET body = 'again' WHERE id = 5")
        .expect("parse second update");
    let crate::sql::ast::Statement::Update(reload_update) = reload_statement else {
        panic!("expected update");
    };
    let reload_prepared = reloaded_runtime
        .prepare_simple_update(&reload_update)
        .expect("prepare second update")
        .expect("expected prepared second update");
    reloaded_runtime
        .execute_prepared_simple_update(&reload_prepared, &[], PAGE_SIZE)
        .expect("execute second update");

    db.begin_write().expect("begin write for second update");
    reloaded_runtime
        .persist_to_db(&db)
        .expect("persist second update");
    db.commit().expect("commit second write txn");

    let second = db
        .execute("SELECT body FROM docs WHERE id = 5")
        .expect("select second updated row")
        .rows()
        .first()
        .and_then(|row| row.values().first())
        .cloned();
    assert_eq!(second, Some(Value::Text("again".to_string())));
}

#[test]
fn persist_to_db_single_payload_delete_succeeds_after_cached_payload_miss() {
    let config = DbConfig {
        paged_row_storage: false,
        defer_table_materialization: false,
        ..DbConfig::default()
    };
    let db = Db::open_or_create(":memory:", config).expect("open db");
    db.execute("CREATE TABLE docs (id INT64 PRIMARY KEY, body TEXT)")
        .expect("create table");
    for row_id in 1_i64..=8_i64 {
        db.execute(&format!(
            "INSERT INTO docs (id, body) VALUES ({row_id}, 'x')"
        ))
        .expect("insert row");
    }

    let mut runtime = db.debug_engine_snapshot().expect("snapshot runtime");
    runtime.cache_payload_remove("docs");
    assert!(runtime.cached_payload("docs").is_none());

    let statement = parse_sql_statement("DELETE FROM docs WHERE id = 4").expect("parse delete");
    let crate::sql::ast::Statement::Delete(delete) = statement else {
        panic!("expected delete");
    };
    let prepared = runtime
        .prepare_simple_delete(&delete)
        .expect("prepare delete")
        .expect("expected prepared delete");
    runtime
        .execute_prepared_simple_delete(&prepared, &[], PAGE_SIZE)
        .expect("execute prepared delete");

    db.begin_write().expect("begin write txn");
    runtime.persist_to_db(&db).expect("persist runtime");
    db.commit().expect("commit write txn");

    assert!(runtime.cached_payload("docs").is_some());
    assert_eq!(
        db.execute("SELECT COUNT(*) FROM docs")
            .expect("count rows")
            .rows()
            .first()
            .and_then(|row| row.values().first())
            .cloned(),
        Some(Value::Int64(7))
    );
    assert_eq!(
        db.execute("SELECT COUNT(*) FROM docs WHERE id = 4")
            .expect("count deleted row")
            .rows()
            .first()
            .and_then(|row| row.values().first())
            .cloned(),
        Some(Value::Int64(0))
    );
}

#[test]
fn index_include_columns_round_trip_manifest_and_legacy_payloads() {
    let mut runtime = EngineRuntime::empty(13);
    execute_sql(
        &mut runtime,
        "CREATE TABLE cover_idx (id INT64 PRIMARY KEY, k TEXT, payload TEXT, flag BOOL)",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX cover_idx_k ON cover_idx (k) INCLUDE (payload, flag)",
    );

    let legacy = encode_runtime_payload(&runtime).expect("encode legacy runtime payload");
    let legacy_decoded = decode_runtime_payload(&legacy).expect("decode legacy runtime payload");
    assert_eq!(
        legacy_decoded.catalog.indexes["cover_idx_k"].include_columns,
        vec!["payload".to_string(), "flag".to_string()]
    );

    let store = InMemoryPageStore::new(PAGE_SIZE);
    let manifest =
        encode_manifest_payload(&runtime, &runtime.persisted_tables).expect("encode manifest");
    let manifest_decoded =
        decode_manifest_payload(&store, &manifest).expect("decode manifest payload");
    assert_eq!(
        manifest_decoded.catalog.indexes["cover_idx_k"].include_columns,
        vec!["payload".to_string(), "flag".to_string()]
    );
}

#[test]
fn manifest_decode_without_index_include_section_defaults_to_empty_include_columns() {
    let mut runtime = EngineRuntime::empty(14);
    execute_sql(
        &mut runtime,
        "CREATE TABLE cover_legacy (id INT64 PRIMARY KEY, k TEXT, payload TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX cover_legacy_idx ON cover_legacy (k) INCLUDE (payload)",
    );
    let store = InMemoryPageStore::new(PAGE_SIZE);
    let manifest =
        encode_manifest_payload(&runtime, &runtime.persisted_tables).expect("encode manifest");
    let legacy_manifest =
        drop_index_include_columns_section(&manifest).expect("drop include section");
    let decoded =
        decode_manifest_payload(&store, &legacy_manifest).expect("decode legacy manifest");
    assert!(decoded.catalog.indexes["cover_legacy_idx"]
        .include_columns
        .is_empty());
}

#[test]
fn schema_entries_round_trip_manifest_and_legacy_payloads() {
    let mut runtime = EngineRuntime::empty(15);
    execute_sql(&mut runtime, "CREATE SCHEMA app");
    execute_sql(&mut runtime, "CREATE SCHEMA IF NOT EXISTS analytics");

    let legacy = encode_runtime_payload(&runtime).expect("encode legacy runtime payload");
    let legacy_decoded = decode_runtime_payload(&legacy).expect("decode legacy runtime payload");
    assert!(legacy_decoded.catalog.schemas.contains_key("app"));
    assert!(legacy_decoded.catalog.schemas.contains_key("analytics"));

    let store = InMemoryPageStore::new(PAGE_SIZE);
    let manifest =
        encode_manifest_payload(&runtime, &runtime.persisted_tables).expect("encode manifest");
    let manifest_decoded =
        decode_manifest_payload(&store, &manifest).expect("decode manifest payload");
    assert!(manifest_decoded.catalog.schemas.contains_key("app"));
    assert!(manifest_decoded.catalog.schemas.contains_key("analytics"));
}

#[test]
fn manifest_template_patch_updates_next_row_id() {
    let mut runtime = EngineRuntime::empty(10);
    execute_sql(&mut runtime, "CREATE TABLE docs (id INT64 PRIMARY KEY)");
    runtime
        .catalog_mut()
        .tables
        .get_mut("docs")
        .expect("table should exist")
        .next_row_id = 42;
    let store = InMemoryPageStore::new(PAGE_SIZE);

    let first_payload = runtime
        .manifest_payload()
        .expect("build first manifest payload")
        .to_vec();
    let first = decode_manifest_payload(&store, &first_payload).expect("decode first payload");
    assert_eq!(first.catalog.tables["docs"].next_row_id, 42);

    runtime
        .catalog_mut()
        .tables
        .get_mut("docs")
        .expect("table should exist")
        .next_row_id = 99;
    let second_payload = runtime
        .manifest_payload()
        .expect("build second manifest payload")
        .to_vec();
    let second = decode_manifest_payload(&store, &second_payload).expect("decode second payload");
    assert_eq!(second.catalog.tables["docs"].next_row_id, 99);
}

#[test]
fn non_correlated_in_subquery_does_not_capture_outer_scope() {
    let mut runtime = EngineRuntime::empty(11);
    execute_sql(
        &mut runtime,
        "CREATE TABLE t (id INT64 PRIMARY KEY, name TEXT, grp INT64)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO t VALUES (1, 'a', 1), (2, 'b', 2), (3, 'c', 1)",
    );

    let statement = parse_sql_statement(
        "SELECT COUNT(*) FROM t WHERE grp IN (SELECT grp FROM t WHERE name = 'a')",
    )
    .expect("parse SQL");
    let result = runtime
        .execute_statement(&statement, &[], PAGE_SIZE)
        .expect("execute SQL");

    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].values(), &[Value::Int64(2)]);
}

#[test]
fn correlated_exists_uses_outer_table_name_when_inner_table_is_aliased() {
    let mut runtime = EngineRuntime::empty(12);
    execute_sql(
        &mut runtime,
        "CREATE TABLE del_artists (Id INT64 PRIMARY KEY, LibraryId INT64)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE del_contributors (Id INT64 PRIMARY KEY, ArtistId INT64)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO del_artists VALUES (1, 10), (2, 20)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO del_contributors VALUES (1, 1), (2, 2)",
    );

    let delete = parse_sql_statement(
        "DELETE FROM del_contributors WHERE EXISTS (\
             SELECT 1 FROM del_contributors AS c \
             INNER JOIN del_artists AS a ON c.ArtistId = a.Id \
             WHERE a.LibraryId = $1 AND del_contributors.Id = c.Id)",
    )
    .expect("parse delete");
    let result = runtime
        .execute_statement(&delete, &[Value::Int64(10)], PAGE_SIZE)
        .expect("execute delete");
    assert_eq!(result.affected_rows(), 1);

    let count = parse_sql_statement("SELECT COUNT(*) FROM del_contributors").expect("parse count");
    let result = runtime
        .execute_statement(&count, &[], PAGE_SIZE)
        .expect("execute count");
    assert_eq!(result.rows()[0].values(), &[Value::Int64(1)]);
}

#[test]
fn simple_count_star_without_filter_uses_fast_path() {
    let mut runtime = EngineRuntime::empty(16);
    execute_sql(
        &mut runtime,
        "CREATE TABLE t (id INT64 PRIMARY KEY, name TEXT)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    );

    let statement = parse_sql_statement("SELECT COUNT(*) FROM t").expect("parse count");
    let result = runtime
        .execute_statement(&statement, &[], PAGE_SIZE)
        .expect("execute count");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].values(), &[Value::Int64(3)]);
}

#[test]
fn engine_runtime_clone_is_shallow_when_unmutated() {
    let mut runtime = EngineRuntime::empty(17);
    execute_sql(
        &mut runtime,
        "CREATE TABLE docs (id INT64 PRIMARY KEY, name TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE people (id INT64 PRIMARY KEY, email TEXT)",
    );
    execute_sql(&mut runtime, "CREATE INDEX docs_name_idx ON docs (name)");
    execute_sql(
        &mut runtime,
        "INSERT INTO docs (id, name) VALUES (1, 'alpha')",
    );
    execute_sql(
        &mut runtime,
        "CREATE TEMP TABLE temp_docs (id INT64 PRIMARY KEY, name TEXT)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO temp_docs (id, name) VALUES (1, 'temp')",
    );

    let cloned = runtime.clone();

    assert!(Arc::ptr_eq(&runtime.catalog, &cloned.catalog));
    assert!(Arc::ptr_eq(&runtime.tables, &cloned.tables));
    assert!(Arc::ptr_eq(
        &runtime.temp_table_data,
        &cloned.temp_table_data
    ));
    assert!(Arc::ptr_eq(&runtime.indexes, &cloned.indexes));
    assert_eq!(Arc::strong_count(&runtime.catalog), 2);
    assert_eq!(Arc::strong_count(&runtime.tables), 2);
    assert_eq!(Arc::strong_count(&runtime.temp_table_data), 2);
    assert_eq!(Arc::strong_count(&runtime.indexes), 2);
}

#[test]
fn engine_runtime_cow_isolates_mutations() {
    let mut runtime = EngineRuntime::empty(18);
    execute_sql(
        &mut runtime,
        "CREATE TABLE docs (id INT64 PRIMARY KEY, name TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TEMP TABLE temp_docs (id INT64 PRIMARY KEY, name TEXT)",
    );

    let mut cloned = runtime.clone();
    execute_sql(&mut cloned, "CREATE INDEX docs_name_idx ON docs (name)");
    execute_sql(
        &mut cloned,
        "INSERT INTO docs (id, name) VALUES (1, 'alpha')",
    );
    execute_sql(
        &mut cloned,
        "INSERT INTO temp_docs (id, name) VALUES (1, 'temp')",
    );

    assert!(!Arc::ptr_eq(&runtime.catalog, &cloned.catalog));
    assert!(!Arc::ptr_eq(&runtime.tables, &cloned.tables));
    assert!(!Arc::ptr_eq(
        &runtime.temp_table_data,
        &cloned.temp_table_data
    ));
    assert!(!Arc::ptr_eq(&runtime.indexes, &cloned.indexes));
    assert!(!runtime.catalog.indexes.contains_key("docs_name_idx"));
    assert!(cloned.catalog.indexes.contains_key("docs_name_idx"));
    assert!(runtime
        .table_data("docs")
        .expect("original table data")
        .rows
        .is_empty());
    assert_eq!(
        cloned
            .table_data("docs")
            .expect("cloned table data")
            .rows
            .len(),
        1
    );
    assert!(runtime
        .temp_table_data("temp_docs")
        .expect("original temp table data")
        .rows
        .is_empty());
    assert_eq!(
        cloned
            .temp_table_data("temp_docs")
            .expect("cloned temp table data")
            .rows
            .len(),
        1
    );

    drop(cloned);

    assert_eq!(Arc::strong_count(&runtime.catalog), 1);
    assert_eq!(Arc::strong_count(&runtime.tables), 1);
    assert_eq!(Arc::strong_count(&runtime.temp_table_data), 1);
    assert_eq!(Arc::strong_count(&runtime.indexes), 1);
}

#[test]
fn engine_runtime_autocommit_select_does_not_grow_strong_counts_unboundedly() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    db.execute("CREATE TABLE docs (id INT64 PRIMARY KEY, name TEXT)")
        .expect("create table");
    db.execute("INSERT INTO docs (id, name) VALUES (1, 'alpha')")
        .expect("insert row 1");
    db.execute("INSERT INTO docs (id, name) VALUES (2, 'beta')")
        .expect("insert row 2");
    db.execute("INSERT INTO docs (id, name) VALUES (3, 'gamma')")
        .expect("insert row 3");

    for _ in 0..100 {
        let result = db
            .execute("SELECT COUNT(*) FROM docs")
            .expect("execute autocommit select");
        assert_eq!(result.rows()[0].values(), &[Value::Int64(3)]);
    }

    let snapshot = db.debug_engine_snapshot().expect("snapshot runtime");
    assert!(Arc::strong_count(&snapshot.catalog) <= 2);
    assert!(Arc::strong_count(&snapshot.tables) <= 2);
    assert!(Arc::strong_count(&snapshot.indexes) <= 2);
}

#[test]
fn cached_payloads_evicts_at_capacity() {
    let mut config = DbConfig::default();
    config.set_cached_payloads_max_entries_for_tests(4);
    let mut runtime = EngineRuntime::from_config(20, &config);

    for key in ["a", "b", "c", "d", "e"] {
        runtime.cache_payload_insert(key.to_string(), Arc::new(vec![key.as_bytes()[0]]));
    }

    let cache = runtime
        .payload_cache
        .lock()
        .expect("payload cache lock should not be poisoned");
    assert_eq!(cache.len(), 4);
    assert!(!cache.contains_key("a"));
    assert!(cache.contains_key("e"));
}

#[test]
fn cached_payloads_lru_promotes_on_access() {
    let mut config = DbConfig::default();
    config.set_cached_payloads_max_entries_for_tests(4);
    let mut runtime = EngineRuntime::from_config(21, &config);

    for key in ["a", "b", "c", "d"] {
        runtime.cache_payload_insert(key.to_string(), Arc::new(vec![key.as_bytes()[0]]));
    }
    assert!(runtime.cached_payload("a").is_some());

    runtime.cache_payload_insert("e".to_string(), Arc::new(vec![b'e']));

    let cache = runtime
        .payload_cache
        .lock()
        .expect("payload cache lock should not be poisoned");
    assert!(cache.contains_key("a"));
    assert!(!cache.contains_key("b"));
    assert!(cache.contains_key("e"));
}

#[test]
fn heap_bytes_cached_counter_accurate() {
    let data = TableData::from_rows(vec![
        StoredRow {
            row_id: 1,
            values: vec![
                Value::Int64(1),
                Value::Text("alpha".repeat(32)),
                Value::Blob(vec![0xAA; 128]),
            ],
        },
        StoredRow {
            row_id: 2,
            values: vec![Value::Text("beta".repeat(16)), Value::Bool(true)],
        },
    ]);

    assert_eq!(data.approximate_heap_bytes(), data.compute_heap_bytes());
    assert!(data.approximate_heap_bytes() > data.rows.len() * std::mem::size_of::<StoredRow>());
}

#[test]
fn heap_bytes_updates_on_mutation() {
    let mut data = TableData::default();
    assert_eq!(data.approximate_heap_bytes(), 0);

    data.push_row(StoredRow {
        row_id: 1,
        values: vec![Value::Int64(1), Value::Text("short".to_string())],
    });
    assert_eq!(data.approximate_heap_bytes(), data.compute_heap_bytes());
    let after_insert = data.approximate_heap_bytes();

    data.replace_value(0, 1, Value::Text("long".repeat(128)))
        .expect("replace text value");
    assert_eq!(data.approximate_heap_bytes(), data.compute_heap_bytes());
    assert!(data.approximate_heap_bytes() > after_insert);

    data.replace_row_values(0, vec![Value::Int64(1), Value::Blob(vec![0xBB; 256])])
        .expect("replace row values");
    assert_eq!(data.approximate_heap_bytes(), data.compute_heap_bytes());

    data.push_row(StoredRow {
        row_id: 2,
        values: vec![Value::Text("keep".repeat(32))],
    });
    data.retain_rows(|row| row.row_id == 2);
    assert_eq!(data.rows.len(), 1);
    assert_eq!(data.rows[0].row_id, 2);
    assert_eq!(data.approximate_heap_bytes(), data.compute_heap_bytes());

    let removed = data.remove_row(0);
    assert_eq!(removed.row_id, 2);
    assert_eq!(data.approximate_heap_bytes(), data.compute_heap_bytes());
}

#[test]
fn payload_cache_touch_is_o1() {
    let mut config = DbConfig::default();
    config.set_cached_payloads_max_entries_for_tests(512);
    let mut runtime = EngineRuntime::from_config(22, &config);

    for idx in 0..512 {
        let key = format!("k{idx:03}");
        runtime.cache_payload_insert(key, Arc::new(vec![idx as u8]));
    }

    let before: Vec<(String, u64)> = {
        let cache = runtime
            .payload_cache
            .lock()
            .expect("payload cache lock should not be poisoned");
        (0..512)
            .map(|idx| {
                let key = format!("k{idx:03}");
                let touch_gen = cache
                    .last_touch_gen(&key)
                    .expect("inserted cache key should have a touch generation");
                (key, touch_gen)
            })
            .collect()
    };

    assert!(runtime.cached_payload("k000").is_some());

    let cache = runtime
        .payload_cache
        .lock()
        .expect("payload cache lock should not be poisoned");
    for (key, touch_gen) in &before {
        let after_touch_gen = cache
            .last_touch_gen(key)
            .expect("cache key should remain present");
        if key == "k000" {
            assert_ne!(after_touch_gen, *touch_gen);
        } else {
            assert_eq!(after_touch_gen, *touch_gen);
        }
    }
}

#[test]
fn payload_cache_eviction_is_lru() {
    let mut config = DbConfig::default();
    config.set_cached_payloads_max_entries_for_tests(512);
    let mut runtime = EngineRuntime::from_config(23, &config);

    for idx in 0..512 {
        let key = format!("k{idx:03}");
        runtime.cache_payload_insert(key, Arc::new(vec![idx as u8]));
    }
    assert!(runtime.cached_payload("k000").is_some());

    runtime.cache_payload_insert("k512".to_string(), Arc::new(vec![0xFF]));

    let cache = runtime
        .payload_cache
        .lock()
        .expect("payload cache lock should not be poisoned");
    assert_eq!(cache.len(), 512);
    assert!(cache.contains_key("k000"));
    assert!(!cache.contains_key("k001"));
    assert!(cache.contains_key("k512"));
}

#[test]
fn transaction_clone_is_cheap() {
    let mut runtime = EngineRuntime::empty(24);
    for idx in 0..1_000 {
        let table_name = format!("t{idx}");
        runtime
            .persisted_tables_mut()
            .insert(table_name.clone(), PersistedTableState::default());
        runtime.deferred_tables_mut().insert(table_name.clone());
        runtime.dirty_tables_mut().insert(table_name);
    }
    runtime.cache_payload_insert("payload".to_string(), Arc::new(vec![0xAA; 4096]));

    let cloned = runtime.clone();

    assert!(Arc::ptr_eq(
        &runtime.persisted_tables,
        &cloned.persisted_tables
    ));
    assert!(Arc::ptr_eq(
        &runtime.deferred_tables,
        &cloned.deferred_tables
    ));
    assert!(Arc::ptr_eq(&runtime.dirty_tables, &cloned.dirty_tables));
    assert!(Arc::ptr_eq(&runtime.payload_cache, &cloned.payload_cache));
    assert_eq!(Arc::strong_count(&runtime.persisted_tables), 2);
    assert_eq!(Arc::strong_count(&runtime.deferred_tables), 2);
    assert_eq!(Arc::strong_count(&runtime.dirty_tables), 2);

    runtime
        .persisted_tables_mut()
        .insert("new_table".to_string(), PersistedTableState::default());
    assert!(!Arc::ptr_eq(
        &runtime.persisted_tables,
        &cloned.persisted_tables
    ));
    assert!(!cloned.persisted_tables.contains_key("new_table"));
}

#[test]
fn cte_alias_shares_row_storage() {
    let runtime = EngineRuntime::empty(19);
    let original = Dataset::with_rows(
        vec![ColumnBinding::visible(
            Some("cte".to_string()),
            "id".to_string(),
        )],
        (0..1000).map(|id| vec![Value::Int64(id)]).collect(),
    );
    let mut ctes = BTreeMap::new();
    ctes.insert("cte".to_string(), original.clone());

    let aliased = runtime
        .evaluate_from_item_in_scope(
            &FromItem::Table {
                name: "cte".to_string(),
                alias: Some("alias_cte".to_string()),
            },
            &[],
            &ctes,
            &Dataset::empty(),
            &[],
        )
        .expect("resolve aliased cte");

    assert!(Arc::ptr_eq(&original.rows, &aliased.rows));
    assert_eq!(aliased.columns[0].table.as_deref(), Some("alias_cte"));
    assert_eq!(Arc::strong_count(&original.rows), 3);
}

#[test]
fn analyze_collects_table_and_index_stats() {
    let mut runtime = EngineRuntime::empty(12);
    execute_sql(
        &mut runtime,
        "CREATE TABLE docs (id INT64 PRIMARY KEY, email TEXT)",
    );
    execute_sql(&mut runtime, "CREATE INDEX docs_email_idx ON docs (email)");
    execute_sql(
        &mut runtime,
        "INSERT INTO docs (id, email) VALUES (1, 'a@example.com')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO docs (id, email) VALUES (2, 'a@example.com')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO docs (id, email) VALUES (3, 'b@example.com')",
    );

    execute_sql(&mut runtime, "ANALYZE docs");

    assert_eq!(
        runtime.catalog.table_stats.get("docs"),
        Some(&crate::catalog::TableStats { row_count: 3 })
    );
    assert_eq!(
        runtime.catalog.index_stats.get("docs_email_idx"),
        Some(&crate::catalog::IndexStats {
            entry_count: 3,
            distinct_key_count: 2,
        })
    );
}

#[test]
fn manifest_round_trip_preserves_analyze_stats() {
    let mut runtime = EngineRuntime::empty(13);
    execute_sql(
        &mut runtime,
        "CREATE TABLE docs (id INT64 PRIMARY KEY, email TEXT)",
    );
    execute_sql(&mut runtime, "CREATE INDEX docs_email_idx ON docs (email)");
    execute_sql(
        &mut runtime,
        "INSERT INTO docs (id, email) VALUES (1, 'a@example.com')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO docs (id, email) VALUES (2, 'b@example.com')",
    );
    execute_sql(&mut runtime, "ANALYZE");

    let store = InMemoryPageStore::new(PAGE_SIZE);
    let manifest = encode_manifest_payload(&runtime, &runtime.persisted_tables)
        .expect("encode manifest payload");
    let decoded = decode_manifest_payload(&store, &manifest).expect("decode manifest payload");

    assert_eq!(
        decoded.catalog.table_stats.get("docs"),
        Some(&crate::catalog::TableStats { row_count: 2 })
    );
    assert_eq!(
        decoded.catalog.index_stats.get("docs_email_idx"),
        Some(&crate::catalog::IndexStats {
            entry_count: 2,
            distinct_key_count: 2,
        })
    );
}

fn execute_sql(runtime: &mut EngineRuntime, sql: &str) {
    let statement = parse_sql_statement(sql).expect("parse SQL");
    runtime
        .execute_statement(&statement, &[], PAGE_SIZE)
        .expect("execute SQL");
}

fn paged_row_source(rows: Vec<StoredRow>) -> TableRowSource {
    let payload = encode_table_payload(&TableData::from_rows(rows.clone()))
        .expect("encode paged test payload");
    let manifest =
        TablePageManifest::from_payload(Arc::new(payload)).expect("build paged test manifest");
    TableRowSource::Paged(Arc::new(manifest))
}

#[test]
fn general_grouped_group_concat_from_source() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE items (id INT64 PRIMARY KEY, cat TEXT, name TEXT)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO items (id, cat, name) VALUES (1, 'a', 'alpha')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO items (id, cat, name) VALUES (2, 'a', 'aplomb')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO items (id, cat, name) VALUES (3, 'b', 'bravo')",
    );

    let statement = parse_sql_statement(
        "SELECT cat, GROUP_CONCAT(name) AS names FROM items GROUP BY cat ORDER BY cat",
    )
    .expect("parse");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query");
    };
    let result = runtime
        .try_execute_general_grouped_query(query, &[])
        .expect("execute")
        .expect("general grouped path should handle GROUP_CONCAT");

    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Text("a".to_string()),
            Value::Text("alpha,aplomb".to_string())
        ]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[
            Value::Text("b".to_string()),
            Value::Text("bravo".to_string())
        ]
    );
}

#[test]
fn general_grouped_having_with_order_by() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE sales (id INT64 PRIMARY KEY, region TEXT, amount INT64)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO sales (id, region, amount) VALUES (1, 'east', 10)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO sales (id, region, amount) VALUES (2, 'east', 20)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO sales (id, region, amount) VALUES (3, 'west', 5)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO sales (id, region, amount) VALUES (4, 'north', 15)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO sales (id, region, amount) VALUES (5, 'north', 15)",
    );

    let statement = parse_sql_statement(
        "SELECT region, SUM(amount) AS total FROM sales GROUP BY region \
             HAVING SUM(amount) > 20 ORDER BY total DESC",
    )
    .expect("parse");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query");
    };
    let result = runtime
        .try_execute_general_grouped_query(query, &[])
        .expect("execute")
        .expect("general grouped path should handle HAVING + ORDER BY");

    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Text("east".to_string()), Value::Int64(30)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Text("north".to_string()), Value::Int64(30)]
    );
}

#[test]
fn general_grouped_order_by_qualified_projected_column_uses_projection_value() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE director_stats (person_id INT64 PRIMARY KEY, films INT64, avg_rating FLOAT64)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO director_stats (person_id, films, avg_rating) VALUES \
             (1, 2, 6.0), (2, 2, 9.5), (3, 3, 8.0)",
    );

    let statement = parse_sql_statement(
        "SELECT d.person_id, d.films, d.avg_rating \
             FROM director_stats d \
             GROUP BY d.person_id, d.films, d.avg_rating \
             ORDER BY d.avg_rating DESC \
             LIMIT 2",
    )
    .expect("parse");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query");
    };
    let result = runtime
        .try_execute_general_grouped_query(query, &[])
        .expect("execute")
        .expect("general grouped path should handle qualified ORDER BY projection");

    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(2), Value::Int64(2), Value::Float64(9.5)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(3), Value::Int64(3), Value::Float64(8.0)]
    );
}

#[test]
fn grouped_cte_order_by_qualified_projected_column_uses_projection_value() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE director_stats (person_id INT64 PRIMARY KEY, films INT64, avg_rating FLOAT64)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO director_stats (person_id, films, avg_rating) VALUES \
             (1, 2, 6.0), (2, 2, 9.5), (3, 3, 8.0)",
    );

    let statement = parse_sql_statement(
        "WITH top_dirs AS ( \
             SELECT person_id, films, avg_rating FROM director_stats \
         ) \
         SELECT d.person_id, d.films, d.avg_rating \
         FROM top_dirs d \
         GROUP BY d.person_id, d.films, d.avg_rating \
         ORDER BY d.avg_rating DESC \
         LIMIT 2",
    )
    .expect("parse");
    let result = runtime
        .execute_statement(&statement, &[], PAGE_SIZE)
        .expect("execute");

    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(2), Value::Int64(2), Value::Float64(9.5)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(3), Value::Int64(3), Value::Float64(8.0)]
    );
}

#[test]
fn general_grouped_mixed_aggregates() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE metrics (id INT64 PRIMARY KEY, grp TEXT, val INT64)",
    );
    for (id, grp, val) in [(1, "x", 10), (2, "x", 20), (3, "x", 30), (4, "y", 5)] {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO metrics (id, grp, val) VALUES ({id}, '{grp}', {val})"),
        );
    }

    let statement = parse_sql_statement(
        "SELECT grp, COUNT(*) AS cnt, AVG(val) AS avg_val, MIN(val) AS mn, MAX(val) AS mx \
             FROM metrics GROUP BY grp ORDER BY grp",
    )
    .expect("parse");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query");
    };

    let result = runtime
        .try_execute_general_grouped_query(query, &[])
        .expect("execute")
        .expect("general grouped path should handle mixed aggregates");

    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Text("x".to_string()),
            Value::Int64(3),
            Value::Float64(20.0),
            Value::Int64(10),
            Value::Int64(30),
        ]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[
            Value::Text("y".to_string()),
            Value::Int64(1),
            Value::Float64(5.0),
            Value::Int64(5),
            Value::Int64(5),
        ]
    );
}

#[test]
fn simple_numeric_aggregate_without_group_streams_rows() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE metrics (id INT64 PRIMARY KEY, val INT64)",
    );
    for (id, val) in [(1, 10), (2, 20), (3, 30)] {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO metrics (id, val) VALUES ({id}, {val})"),
        );
    }
    execute_sql(
        &mut runtime,
        "INSERT INTO metrics (id, val) VALUES (4, NULL)",
    );
    let rows = runtime
        .table_row_source("metrics")
        .expect("metrics rows")
        .resident_data()
        .rows
        .clone();
    runtime
        .replace_table_row_source("metrics", paged_row_source(rows))
        .expect("replace metrics row source");

    let statement =
        parse_sql_statement("SELECT COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM metrics")
            .expect("parse");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query");
    };

    let result = runtime
        .try_execute_simple_grouped_numeric_aggregate_query(query, &[])
        .expect("execute")
        .expect("simple numeric aggregate path should handle scalar aggregates");

    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(4),
            Value::Int64(60),
            Value::Float64(20.0),
            Value::Int64(10),
            Value::Int64(30),
        ]
    );
}

#[test]
fn simple_numeric_aggregate_without_group_returns_empty_group_defaults() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE metrics (id INT64 PRIMARY KEY, val INT64)",
    );

    let statement = parse_sql_statement(
        "SELECT COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) \
             FROM metrics WHERE val > 100",
    )
    .expect("parse");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query");
    };

    let result = runtime
        .try_execute_simple_grouped_numeric_aggregate_query(query, &[])
        .expect("execute")
        .expect("simple numeric aggregate path should keep scalar empty-group semantics");

    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(0),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
        ]
    );
}

#[test]
fn simple_scalar_filtered_count_sum_uses_fast_path() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE orders (id INT64 PRIMARY KEY, user_id INT64, amount FLOAT64)",
    );
    for (id, user_id, amount) in [(1, 7, 1.5), (2, 8, 10.0), (3, 7, 2.25)] {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO orders (id, user_id, amount) VALUES ({id}, {user_id}, {amount})"),
        );
    }

    let statement =
        parse_sql_statement("SELECT COUNT(*), SUM(amount) FROM orders WHERE user_id = $1")
            .expect("parse");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query");
    };

    let result = runtime
        .try_execute_simple_grouped_numeric_aggregate_query(query, &[Value::Int64(7)])
        .expect("execute")
        .expect("simple scalar filtered aggregate should stay on fast path");

    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(2), Value::Float64(3.75)]
    );
}

#[test]
fn indexed_join_grouped_count_uses_child_index_counts() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE artists (id INT64 PRIMARY KEY, name TEXT NOT NULL)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE songs (id INT64 PRIMARY KEY, artist_id INT64 NOT NULL, title TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX idx_songs_artist ON songs (artist_id)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO artists (id, name) VALUES (1, 'a')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO artists (id, name) VALUES (2, 'b')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO artists (id, name) VALUES (3, 'c')",
    );
    for (id, artist_id) in [(1, 1), (2, 1), (3, 2), (4, 2), (5, 2)] {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO songs (id, artist_id, title) VALUES ({id}, {artist_id}, 's')"),
        );
    }

    let statement = parse_sql_statement(
        "SELECT a.id, a.name, COUNT(s.id) AS song_count \
             FROM artists a JOIN songs s ON s.artist_id = a.id \
             GROUP BY a.id, a.name ORDER BY song_count DESC, a.id ASC LIMIT 2",
    )
    .expect("parse");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query");
    };

    assert_eq!(
        runtime
            .indexed_join_grouped_count_parent_table_name(query, &[])
            .expect("analyze")
            .expect("indexed grouped count parent table"),
        "artists"
    );
    let result = runtime
        .try_execute_indexed_join_grouped_count_query(query, &[])
        .expect("execute")
        .expect("indexed grouped join count path should match this query");

    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(2),
            Value::Text("b".to_string()),
            Value::Int64(3)
        ]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[
            Value::Int64(1),
            Value::Text("a".to_string()),
            Value::Int64(2)
        ]
    );
}

#[test]
fn indexed_inner_join_aggregate_counts_distinct_child_values() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE people (id INT64 PRIMARY KEY, name TEXT NOT NULL)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE roles (id INT64 PRIMARY KEY, person_id INT64 NOT NULL, movie_id INT64)",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX idx_roles_person ON roles (person_id)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO people (id, name) VALUES (1, 'Ada'), (2, 'Bea'), (3, 'Cid')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO roles (id, person_id, movie_id) VALUES \
             (1, 1, 10), (2, 1, 10), (3, 1, 11), (4, 2, 12), (5, 2, NULL)",
    );

    let statement = parse_sql_statement(
        "SELECT p.id, p.name, COUNT(DISTINCT r.movie_id) AS films, COUNT(*) AS roles \
             FROM people p JOIN roles r ON r.person_id = p.id \
             GROUP BY p.id, p.name ORDER BY films DESC, p.id LIMIT 10",
    )
    .expect("parse");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query");
    };

    let result = runtime
        .try_execute_left_join_aggregate_query(query, &[])
        .expect("execute")
        .expect("indexed join aggregate path should handle inner COUNT DISTINCT query");

    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(1),
            Value::Text("Ada".to_string()),
            Value::Int64(2),
            Value::Int64(3),
        ]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[
            Value::Int64(2),
            Value::Text("Bea".to_string()),
            Value::Int64(1),
            Value::Int64(2),
        ]
    );
}

#[test]
fn indexed_three_table_genre_popularity_aggregate_uses_bridge_index() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE genres (id INT64 PRIMARY KEY, name TEXT NOT NULL)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE movies (id INT64 PRIMARY KEY, title TEXT NOT NULL, rating FLOAT64)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE movie_genres (movie_id INT64 NOT NULL, genre_id INT64 NOT NULL)",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX idx_mgenres_genre ON movie_genres (genre_id)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO genres (id, name) VALUES (1, 'Action'), (2, 'Drama'), (3, 'Noir')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO movies (id, title, rating) VALUES \
             (10, 'A', 8.0), (20, 'B', 6.0), (30, 'C', 9.0)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO movie_genres (movie_id, genre_id) VALUES \
             (10, 1), (20, 1), (30, 2), (999, 2)",
    );

    let statement = parse_sql_statement(
        "SELECT g.name, COUNT(*) AS movie_count, AVG(m.rating) AS avg_rating \
             FROM genres g \
             JOIN movie_genres mg ON mg.genre_id = g.id \
             JOIN movies m ON m.id = mg.movie_id \
             GROUP BY g.name \
             ORDER BY movie_count DESC, g.name",
    )
    .expect("parse");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query");
    };

    let result = runtime
        .try_execute_three_table_genre_popularity_query(query, &[])
        .expect("execute")
        .expect("genre popularity fast path should match this query");

    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Text("Action".to_string()),
            Value::Int64(2),
            Value::Float64(7.0),
        ]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[
            Value::Text("Drama".to_string()),
            Value::Int64(1),
            Value::Float64(9.0),
        ]
    );
}

#[test]
fn showdown_directors_cte_fast_path_aggregates_without_materialized_rejoin() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE movies (id INT64 PRIMARY KEY, title TEXT NOT NULL, rating FLOAT64)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE roles (id INT64 PRIMARY KEY, movie_id INT64 NOT NULL, person_id INT64 NOT NULL, job TEXT NOT NULL)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO movies (id, title, rating) VALUES \
             (1, 'A', 8.0), (2, 'B', 10.0), (3, 'C', 6.0), (4, 'D', 9.0)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO roles (id, movie_id, person_id, job) VALUES \
             (1, 1, 7, 'Director'), \
             (2, 2, 7, 'Director'), \
             (3, 3, 8, 'Director'), \
             (4, 4, 8, 'Director'), \
             (5, 1, 9, 'Director'), \
             (6, 2, 10, 'Actor')",
    );

    let statement = parse_sql_statement(
        "WITH directed AS ( \
             SELECT r.person_id, r.movie_id, m.title, m.rating \
             FROM roles r \
             JOIN movies m ON m.id = r.movie_id \
             WHERE r.job = 'Director' \
         ), \
         top_dirs AS ( \
             SELECT person_id, COUNT(*) AS films, AVG(rating) AS avg_rating \
             FROM directed \
             GROUP BY person_id \
             HAVING COUNT(*) >= 2 \
         ) \
         SELECT d.person_id, d.films, d.avg_rating, \
                STRING_AGG(dir.title, ', ') AS titles \
         FROM top_dirs d \
         JOIN directed dir ON dir.person_id = d.person_id \
         GROUP BY d.person_id, d.films, d.avg_rating \
         ORDER BY d.avg_rating DESC \
         LIMIT 20",
    )
    .expect("parse");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query");
    };

    let result = runtime
        .try_execute_showdown_directors_cte_query(query, &[])
        .expect("execute")
        .expect("directors CTE fast path should recognize the benchmark shape");

    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(7),
            Value::Int64(2),
            Value::Float64(9.0),
            Value::Text("A, B".to_string()),
        ]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[
            Value::Int64(8),
            Value::Int64(2),
            Value::Float64(7.5),
            Value::Text("C, D".to_string()),
        ]
    );
}

#[test]
fn indexed_join_grouped_count_rejects_nullable_count_column() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE artists (id INT64 PRIMARY KEY, name TEXT NOT NULL)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE songs (id INT64 PRIMARY KEY, artist_id INT64 NOT NULL, title TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX idx_songs_artist ON songs (artist_id)",
    );

    let statement = parse_sql_statement(
        "SELECT a.id, COUNT(s.title) AS titled_songs \
             FROM artists a JOIN songs s ON s.artist_id = a.id \
             GROUP BY a.id ORDER BY titled_songs DESC",
    )
    .expect("parse");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query");
    };

    assert!(
        runtime
            .try_execute_indexed_join_grouped_count_query(query, &[])
            .expect("analyze")
            .is_none(),
        "nullable COUNT(child.column) must keep general SQL semantics"
    );
}

#[test]
fn indexed_join_limit_projection_stops_after_limit() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE artists (id INT64 PRIMARY KEY, name TEXT NOT NULL)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE albums (id INT64 PRIMARY KEY, artist_id INT64 NOT NULL, title TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE songs (id INT64 PRIMARY KEY, album_id INT64 NOT NULL, title TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX idx_albums_artist ON albums (artist_id)",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX idx_songs_album ON songs (album_id)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO artists (id, name) VALUES (1, 'a')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO artists (id, name) VALUES (2, 'b')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO albums (id, artist_id, title) VALUES (10, 1, 'a1')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO albums (id, artist_id, title) VALUES (20, 2, 'b1')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO songs (id, album_id, title) VALUES (100, 10, 's1')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO songs (id, album_id, title) VALUES (101, 10, 's2')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO songs (id, album_id, title) VALUES (200, 20, 's3')",
    );

    let statement = parse_sql_statement(
        "SELECT a.id AS artist_id, a.name AS artist_name, al.title AS album_title, \
                    s.title AS song_title \
             FROM artists a JOIN albums al ON al.artist_id = a.id \
             JOIN songs s ON s.album_id = al.id LIMIT 2",
    )
    .expect("parse");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query");
    };
    let result = runtime
        .try_execute_indexed_join_limit_projection_query(query, &[])
        .expect("execute")
        .expect("indexed join limit projection path should match this query");

    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(1),
            Value::Text("a".to_string()),
            Value::Text("a1".to_string()),
            Value::Text("s1".to_string()),
        ]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[
            Value::Int64(1),
            Value::Text("a".to_string()),
            Value::Text("a1".to_string()),
            Value::Text("s2".to_string()),
        ]
    );
}

#[test]
fn indexed_join_projection_orders_three_table_chain_without_limit() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE artists (id INT64 PRIMARY KEY, name TEXT NOT NULL)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE albums (id INT64 PRIMARY KEY, artist_id INT64 NOT NULL, title TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE songs (id INT64 PRIMARY KEY, album_id INT64 NOT NULL, title TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX idx_albums_artist ON albums (artist_id)",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX idx_songs_album ON songs (album_id)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO artists (id, name) VALUES (1, 'a')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO artists (id, name) VALUES (2, 'b')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO albums (id, artist_id, title) VALUES (10, 1, 'a1')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO albums (id, artist_id, title) VALUES (20, 2, 'b1')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO songs (id, album_id, title) VALUES (100, 10, 's1')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO songs (id, album_id, title) VALUES (101, 10, 's2')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO songs (id, album_id, title) VALUES (200, 20, 's3')",
    );

    let statement = parse_sql_statement(
        "SELECT a.id AS artist_id, a.name AS artist_name, al.title AS album_title, \
                    s.title AS song_title \
             FROM artists a JOIN albums al ON al.artist_id = a.id \
             JOIN songs s ON s.album_id = al.id \
             ORDER BY a.id, s.title DESC",
    )
    .expect("parse");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query");
    };
    let result = runtime
        .try_execute_three_table_indexed_join_projection_query(query, &[])
        .expect("execute")
        .expect("indexed join ordered projection path should match this query");

    let actual = result
        .rows()
        .iter()
        .map(|row| row.values().to_vec())
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        vec![
            vec![
                Value::Int64(1),
                Value::Text("a".to_string()),
                Value::Text("a1".to_string()),
                Value::Text("s2".to_string()),
            ],
            vec![
                Value::Int64(1),
                Value::Text("a".to_string()),
                Value::Text("a1".to_string()),
                Value::Text("s1".to_string()),
            ],
            vec![
                Value::Int64(2),
                Value::Text("b".to_string()),
                Value::Text("b1".to_string()),
                Value::Text("s3".to_string()),
            ],
        ]
    );
}

#[test]
fn view_projection_limit_pushes_into_indexed_join_chain() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE artists (id INT64 PRIMARY KEY, name TEXT NOT NULL)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE albums (id INT64 PRIMARY KEY, artist_id INT64 NOT NULL, title TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE songs (id INT64 PRIMARY KEY, album_id INT64 NOT NULL, title TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX idx_albums_artist ON albums (artist_id)",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX idx_songs_album ON songs (album_id)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO artists (id, name) VALUES (1, 'a')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO albums (id, artist_id, title) VALUES (10, 1, 'a1')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO songs (id, album_id, title) VALUES (100, 10, 's1')",
    );
    execute_sql(
        &mut runtime,
        "CREATE VIEW v_artist_songs AS \
             SELECT a.id AS artist_id, a.name AS artist_name, al.title AS album_title, \
                    s.title AS song_title \
             FROM artists a JOIN albums al ON al.artist_id = a.id \
             JOIN songs s ON s.album_id = al.id",
    );

    let statement = parse_sql_statement(
        "SELECT artist_id, artist_name, album_title, song_title FROM v_artist_songs LIMIT 1",
    )
    .expect("parse");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query");
    };

    let result = runtime
        .try_execute_simple_view_projection_limit_query(query, &[])
        .expect("execute")
        .expect("view projection limit path should match this query");

    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(1),
            Value::Text("a".to_string()),
            Value::Text("a1".to_string()),
            Value::Text("s1".to_string()),
        ]
    );
}

#[test]
fn view_filter_pushdown_can_prefilter_rowid_alias_join_chain() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE artists (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE albums (id INTEGER PRIMARY KEY, artist_id INTEGER NOT NULL, title TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE TABLE songs (id INTEGER PRIMARY KEY, album_id INTEGER NOT NULL, title TEXT)",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX idx_albums_artist ON albums (artist_id)",
    );
    execute_sql(
        &mut runtime,
        "CREATE INDEX idx_songs_album ON songs (album_id)",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO artists (id, name) VALUES (1, 'a')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO artists (id, name) VALUES (2, 'b')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO albums (id, artist_id, title) VALUES (10, 1, 'a1')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO albums (id, artist_id, title) VALUES (20, 2, 'b1')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO songs (id, album_id, title) VALUES (100, 10, 's1')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO songs (id, album_id, title) VALUES (101, 10, 's2')",
    );
    execute_sql(
        &mut runtime,
        "INSERT INTO songs (id, album_id, title) VALUES (200, 20, 's3')",
    );

    let statement = parse_sql_statement(
        "SELECT a.id AS artist_id, a.name AS artist_name, al.title AS album_title, \
                    s.title AS song_title \
             FROM artists a JOIN albums al ON al.artist_id = a.id \
             JOIN songs s ON s.album_id = al.id \
             WHERE a.id = $1",
    )
    .expect("parse");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query");
    };
    let crate::sql::ast::QueryBody::Select(select) = &query.body else {
        panic!("expected select");
    };

    let dataset = runtime
        .try_indexed_prefiltered_inner_join_tree(select, &[Value::Int64(1)], &BTreeMap::new())
        .expect("execute")
        .expect("rowid-filtered view join chain should stay on the prefiltered fast path");

    assert_eq!(dataset.rows.len(), 2);
    assert!(dataset
        .rows
        .iter()
        .all(|row| row.first() == Some(&Value::Int64(1))));
}

#[test]
fn rowid_range_projection_allows_order_by_unprojected_key() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT NOT NULL)",
    );
    for id in 0..5 {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO users (id, name) VALUES ({id}, 'u{id}')"),
        );
    }

    let statement = parse_sql_statement(
        "SELECT name FROM users WHERE id >= $1 AND id < $2 ORDER BY id LIMIT $3",
    )
    .expect("parse");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query");
    };

    let result = runtime
        .try_execute_simple_filtered_projection_query(
            query,
            &[Value::Int64(1), Value::Int64(4), Value::Int64(2)],
        )
        .expect("execute")
        .expect("rowid range projection path should handle ORDER BY id outside projection");

    assert_eq!(result.rows().len(), 2);
    assert_eq!(result.rows()[0].values(), &[Value::Text("u1".to_string())]);
    assert_eq!(result.rows()[1].values(), &[Value::Text("u2".to_string())]);
}

#[test]
fn general_grouped_paged_row_source() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE paged_grp (id INT64 PRIMARY KEY, category TEXT, value INT64)",
    );

    let rows: Vec<StoredRow> = vec![
        StoredRow {
            row_id: 1,
            values: vec![
                Value::Int64(1),
                Value::Text("a".to_string()),
                Value::Int64(10),
            ],
        },
        StoredRow {
            row_id: 2,
            values: vec![
                Value::Int64(2),
                Value::Text("a".to_string()),
                Value::Int64(20),
            ],
        },
        StoredRow {
            row_id: 3,
            values: vec![
                Value::Int64(3),
                Value::Text("b".to_string()),
                Value::Int64(30),
            ],
        },
    ];
    runtime
        .tables_mut()
        .insert("paged_grp".to_string(), paged_row_source(rows));

    let statement = parse_sql_statement(
        "SELECT category, SUM(value) AS total FROM paged_grp GROUP BY category ORDER BY category",
    )
    .expect("parse");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query");
    };

    let result = runtime
        .try_execute_general_grouped_query(query, &[])
        .expect("execute")
        .expect("general grouped path should handle paged row source");

    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Text("a".to_string()), Value::Int64(30)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Text("b".to_string()), Value::Int64(30)]
    );
}

#[test]
fn general_grouped_with_limit_offset() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE lo_data (id INT64 PRIMARY KEY, grp TEXT, val INT64)",
    );
    for (id, grp, val) in [(1, "a", 1), (2, "b", 2), (3, "c", 3), (4, "d", 4)] {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO lo_data (id, grp, val) VALUES ({id}, '{grp}', {val})"),
        );
    }

    let statement = parse_sql_statement(
        "SELECT grp, SUM(val) AS total FROM lo_data GROUP BY grp ORDER BY grp LIMIT 2 OFFSET 1",
    )
    .expect("parse");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query");
    };

    let result = runtime
        .try_execute_general_grouped_query(query, &[])
        .expect("execute")
        .expect("general grouped path should handle LIMIT/OFFSET");

    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Text("b".to_string()), Value::Int64(2)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Text("c".to_string()), Value::Int64(3)]
    );
}

#[test]
fn general_grouped_distinct() {
    let mut runtime = EngineRuntime::empty(1);
    execute_sql(
        &mut runtime,
        "CREATE TABLE dist_data (id INT64 PRIMARY KEY, grp TEXT, val INT64)",
    );
    for (id, grp, val) in [(1, "a", 10), (2, "a", 5), (3, "a", 5), (4, "b", 20)] {
        execute_sql(
            &mut runtime,
            &format!("INSERT INTO dist_data (id, grp, val) VALUES ({id}, '{grp}', {val})"),
        );
    }

    let statement = parse_sql_statement(
        "SELECT DISTINCT grp, SUM(val) AS total FROM dist_data GROUP BY grp, val ORDER BY grp",
    )
    .expect("parse");
    let crate::sql::ast::Statement::Query(query) = &statement else {
        panic!("expected query");
    };

    let result = runtime
        .try_execute_general_grouped_query(query, &[])
        .expect("execute")
        .expect("general grouped path should handle DISTINCT");

    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Text("a".to_string()), Value::Int64(10)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Text("b".to_string()), Value::Int64(20)]
    );
}
