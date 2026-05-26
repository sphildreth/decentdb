# Local Data Security

DecentDB includes local data-security features for embedded applications that
need encrypted files, tenant-aware reads, masked projections, and explicit audit
context without running a database server.

## Transparent Data Encryption

Transparent data encryption (TDE) is configured when the database is created or
opened. The same key must be supplied for every later open of that encrypted
database.

```rust
use decentdb::{Db, DbConfig, DbEncryptionConfig};

let config = DbConfig {
    encryption: Some(DbEncryptionConfig::from_key_bytes(
        b"application-managed high entropy key bytes",
    )?),
    ..DbConfig::default()
};

let db = Db::create("secure.ddb", config.clone())?;
let reopened = Db::open("secure.ddb", config)?;
# Ok::<(), decentdb::DbError>(())
```

C ABI and binding callers can pass the key through open options:

```text
encryption_key_hex=00112233445566778899aabbccddeeff
```

`encryption_key=<text>` is also accepted, but production code should usually
prefer binary key material encoded as hex. Do not log option strings containing
key material.

TDE encrypts the database file, WAL, and sync journal through the VFS layer. A
small per-file plaintext prefix stores DecentDB TDE metadata and a key verifier;
key bytes are never written to database pages, WAL frames, sync journals, audit
rows, or telemetry.

TDE v1 is an encryption-at-rest confidentiality feature. It does not provide
platform key storage, online key rotation, or a new authenticated page/chunk
format. Applications should keep keys in an operating-system key store, browser
key store, secure enclave, KMS, or equivalent secret-management system.

## Audit Context

Audit context is connection-local metadata supplied by the host application.
Policies, masks, and audit rows can read it through SQL functions.

```sql
SET AUDIT CONTEXT tenant_id = 'tenant-a';
SET AUDIT CONTEXT actor = 'alice@example.com';

SELECT current_tenant();
SELECT current_actor();
SELECT current_audit_context('tenant_id');
SELECT key, value FROM sys_audit_context ORDER BY key;
```

`SET AUDIT CONTEXT <key> = NULL` clears a value. Values may be string literals,
integers, booleans, or `NULL`.

Rust applications can set the same context directly:

```rust
use decentdb::Value;

db.set_audit_context_value("tenant_id", Value::Text("tenant-a".to_string()))?;
db.clear_audit_context_value("tenant_id")?;
# Ok::<(), decentdb::DbError>(())
```

The C ABI exposes `ddb_db_set_audit_context_text` and
`ddb_db_clear_audit_context` for text values. Other bindings can also use SQL
`SET AUDIT CONTEXT`.

## Row Policies

Policies are durable row filters evaluated when user queries read a table.

```sql
CREATE POLICY tenant_filter
  ON invoices
  USING tenant_id = current_tenant();

ALTER POLICY tenant_filter DISABLE;
ALTER POLICY tenant_filter ENABLE;
DROP POLICY tenant_filter;
DROP POLICY IF EXISTS tenant_filter;
```

Policy expressions must evaluate to `BOOL`. `FALSE` and `NULL` hide the row.
Multiple enabled policies on the same table are combined with logical `AND`.
Internal integrity checks and security catalog maintenance do not use policies
to hide rows from the engine itself.

## Column Masks

Masks are durable projection rules. They do not rewrite stored values; they
rewrite query output for matching table columns.

```sql
CREATE MASK ssn_mask
  ON employees(ssn)
  USING '***-**-' || right(ssn, 4);

ALTER MASK ssn_mask DISABLE;
ALTER MASK ssn_mask ENABLE;
DROP MASK ssn_mask;
DROP MASK IF EXISTS ssn_mask;
```

Masks apply through aliases and wildcard projections. Mask expressions are
evaluated against the original row so they can reference other columns from the
same projected row.

## Audit Events

Security DDL records audit rows in `__decentdb_audit_events` with operation,
target, actor, tenant, statement text, timestamp, and a JSON snapshot of the
audit context.

```sql
SELECT operation, target, actor, tenant
FROM __decentdb_audit_events
ORDER BY created_at_micros;
```

Security metadata is stored in DecentDB-owned catalog tables:

- `__decentdb_policies`
- `__decentdb_masks`
- `__decentdb_audit_events`

These tables are hidden from normal compatibility catalog listings, but trusted
local connections can query them directly. DecentDB does not implement users,
roles, `GRANT`, or server-side authentication; host applications remain
responsible for deciding which code can open a database handle and execute SQL.
