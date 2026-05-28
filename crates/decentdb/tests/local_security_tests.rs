use decentdb::{Db, DbConfig, Value};

#[test]
fn policies_masks_and_audit_context_filter_and_mask_query_output() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    db.execute("CREATE TABLE employees (id INT PRIMARY KEY, tenant_id TEXT, name TEXT, ssn TEXT)")
        .expect("create table");
    db.execute(
        "INSERT INTO employees (id, tenant_id, name, ssn) VALUES
         (1, 'tenant-a', 'Ada', '111-22-3333'),
         (2, 'tenant-b', 'Grace', '222-33-4444')",
    )
    .expect("insert rows");

    db.execute("SET AUDIT CONTEXT tenant_id = 'tenant-a'")
        .expect("set tenant");
    db.execute("SET AUDIT CONTEXT actor = 'alice'")
        .expect("set actor");
    assert_eq!(
        db.audit_context_snapshot()
            .expect("audit context snapshot")
            .get("actor"),
        Some(&Value::Text("alice".to_string()))
    );

    let context = db
        .execute("SELECT key, value FROM sys_audit_context ORDER BY key")
        .expect("audit context view");
    assert_eq!(
        context
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![
                Value::Text("actor".to_string()),
                Value::Text("alice".to_string())
            ],
            vec![
                Value::Text("tenant_id".to_string()),
                Value::Text("tenant-a".to_string())
            ],
        ]
    );
    let functions = db
        .execute("SELECT current_actor(), current_tenant(), current_audit_context('tenant_id')")
        .expect("audit context functions");
    assert_eq!(
        functions.rows()[0].values(),
        &[
            Value::Text("alice".to_string()),
            Value::Text("tenant-a".to_string()),
            Value::Text("tenant-a".to_string()),
        ]
    );

    db.execute("CREATE POLICY tenant_filter ON employees USING tenant_id = current_tenant()")
        .expect("create policy");
    db.execute("CREATE MASK ssn_mask ON employees(ssn) USING '***-**-' || right(ssn, 4)")
        .expect("create mask");

    let result = db
        .execute("SELECT id, name, ssn FROM employees ORDER BY id")
        .expect("select masked tenant rows");
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(1),
            Value::Text("Ada".to_string()),
            Value::Text("***-**-3333".to_string()),
        ]
    );
    assert_eq!(result.rows().len(), 1);

    let audit = db
        .execute(
            "SELECT operation, target, actor, tenant FROM __decentdb_audit_events ORDER BY operation",
        )
        .expect("audit events");
    let audit_rows = audit
        .rows()
        .iter()
        .map(|row| row.values().to_vec())
        .collect::<Vec<_>>();
    assert!(audit_rows.contains(&vec![
        Value::Text("CREATE_MASK".to_string()),
        Value::Text("ssn_mask".to_string()),
        Value::Text("alice".to_string()),
        Value::Text("tenant-a".to_string()),
    ]));
    assert!(audit_rows.contains(&vec![
        Value::Text("CREATE_POLICY".to_string()),
        Value::Text("tenant_filter".to_string()),
        Value::Text("alice".to_string()),
        Value::Text("tenant-a".to_string()),
    ]));
}

#[test]
fn policy_and_mask_can_be_disabled_and_dropped() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    db.execute("CREATE TABLE docs (id INT PRIMARY KEY, tenant_id TEXT, body TEXT)")
        .expect("create table");
    db.execute(
        "INSERT INTO docs (id, tenant_id, body) VALUES
         (1, 'tenant-a', 'alpha-secret'),
         (2, 'tenant-b', 'beta-secret')",
    )
    .expect("insert rows");
    db.execute("SET AUDIT CONTEXT tenant_id = 'tenant-a'")
        .expect("set tenant");
    db.execute("CREATE POLICY docs_tenant ON docs USING tenant_id = current_tenant()")
        .expect("create policy");
    db.execute("CREATE MASK body_mask ON docs(body) USING 'masked'")
        .expect("create mask");

    let filtered = db
        .execute("SELECT body FROM docs ORDER BY id")
        .expect("filtered docs");
    assert_eq!(
        filtered.rows()[0].values(),
        &[Value::Text("masked".to_string())]
    );
    assert_eq!(filtered.rows().len(), 1);

    db.execute("ALTER MASK body_mask DISABLE")
        .expect("disable mask");
    let unmasked = db
        .execute("SELECT body FROM docs ORDER BY id")
        .expect("unmasked docs");
    assert_eq!(
        unmasked.rows()[0].values(),
        &[Value::Text("alpha-secret".to_string())]
    );
    assert_eq!(unmasked.rows().len(), 1);

    db.execute("ALTER POLICY docs_tenant DISABLE")
        .expect("disable policy");
    let all_rows = db
        .execute("SELECT body FROM docs ORDER BY id")
        .expect("all docs");
    assert_eq!(all_rows.rows().len(), 2);

    db.execute("DROP MASK body_mask").expect("drop mask");
    db.execute("DROP POLICY docs_tenant").expect("drop policy");
    let policies = db
        .execute("SELECT policy_name FROM __decentdb_policies")
        .expect("policies table");
    let masks = db
        .execute("SELECT mask_name FROM __decentdb_masks")
        .expect("masks table");
    assert!(policies.rows().is_empty());
    assert!(masks.rows().is_empty());
}

#[test]
fn policies_and_masks_persist_and_apply_through_aliases() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("secure-rules.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("open db");
        db.execute("CREATE TABLE employees (id INT PRIMARY KEY, tenant_id TEXT, ssn TEXT)")
            .expect("create employees");
        db.execute("CREATE TABLE payroll (id INT PRIMARY KEY, employee_id INT, ssn TEXT)")
            .expect("create payroll");
        db.execute(
            "INSERT INTO employees (id, tenant_id, ssn) VALUES
             (1, 'tenant-a', '111-22-3333'),
             (2, 'tenant-b', '222-33-4444')",
        )
        .expect("insert employees");
        db.execute(
            "INSERT INTO payroll (id, employee_id, ssn) VALUES
             (10, 1, 'payroll-111-22-3333'),
             (20, 2, 'payroll-222-33-4444')",
        )
        .expect("insert payroll");
        db.execute("CREATE POLICY tenant_filter ON employees USING tenant_id = current_tenant()")
            .expect("create policy");
        db.execute("CREATE MASK employee_ssn_mask ON employees(ssn) USING 'employee-mask'")
            .expect("create employee mask");
        db.execute("CREATE MASK payroll_ssn_mask ON payroll(ssn) USING 'payroll-mask'")
            .expect("create payroll mask");
    }

    let db = Db::open(&path, DbConfig::default()).expect("reopen db");
    db.set_audit_context_value("tenant_id", Value::Text("tenant-a".to_string()))
        .expect("set tenant");
    let result = db
        .execute(
            "SELECT e.id, e.ssn, p.ssn
             FROM employees e
             JOIN payroll p ON p.employee_id = e.id
             ORDER BY e.id",
        )
        .expect("masked aliased join");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(1),
            Value::Text("employee-mask".to_string()),
            Value::Text("payroll-mask".to_string()),
        ]
    );
}
