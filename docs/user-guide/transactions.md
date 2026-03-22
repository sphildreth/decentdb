# Transactions

DecentDB supports ACID transactions with full durability guarantees.

## Transaction Basics

### Starting a Transaction

```sql
BEGIN;
BEGIN IMMEDIATE;   -- Accepted as synonym for BEGIN
BEGIN EXCLUSIVE;   -- Accepted as synonym for BEGIN
```

DecentDB is a single-writer engine, so `BEGIN`, `BEGIN IMMEDIATE`, and `BEGIN EXCLUSIVE` all behave identically.

### Committing a Transaction

```sql
COMMIT;
```

All changes are persisted to disk.

### Rolling Back

```sql
ROLLBACK;
```

All changes since BEGIN are discarded.

### Auto-Commit

Individual write statements (`INSERT`, `UPDATE`, `DELETE`, DDL) executed outside an explicit `BEGIN`/`COMMIT` block are automatically wrapped in their own transaction. If the statement succeeds, it is committed; if it fails, it is rolled back.

## ACID Properties

### Atomicity

All operations in a transaction succeed or none do:

```sql
BEGIN;
INSERT INTO accounts VALUES (1, 1000);
INSERT INTO accounts VALUES (2, 2000);
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;
```

If any statement fails, the entire transaction is rolled back.

### Consistency

Foreign key constraints are enforced during transactions:

```sql
BEGIN;
INSERT INTO orders (id, user_id) VALUES (1, 999);  -- Fails if user 999 doesn't exist
COMMIT;
```

### Isolation

DecentDB uses **Snapshot Isolation**:
- Readers see a consistent snapshot of data as of transaction start
- Writers block other writers (single writer model)
- Readers never block writers
- Writers never block readers

### Durability

Committed transactions survive crashes:

```bash
# Transaction is committed with fsync
decentdb exec --db=my.ddb --sql="BEGIN; INSERT INTO logs VALUES (1, 'important'); COMMIT"

# Even if system crashes here, the data is safe
```

## Single Writer Model

DecentDB enforces single writer semantics:

- Only one write transaction at a time
- Write transactions are serialized
- No deadlocks possible
- Readers see stable snapshots

## Durability

By default, DecentDB commits are durable: a successful `COMMIT` is persisted via the WAL before the command returns.

For bulk ingestion you can trade durability for throughput using `bulk-load --durability=<full|deferred|none>`:

- `full`: fsync each batch (safest, slowest)
- `deferred` (default): fsync every `--syncInterval` batches
- `none`: no fsync (fastest; unsafe on crash/power loss)

This setting applies to `bulk-load`; regular SQL writes use the default durable commit behavior.

## Savepoints

Savepoints allow you to create named checkpoints within a transaction. You can roll back to a savepoint without discarding the entire transaction. See [ADR-0110](../../design/adr/0110-savepoints.md).

### Creating a Savepoint

```sql
SAVEPOINT name;
```

Captures a snapshot of the current transaction state (catalog metadata and dirty pages).

### Releasing a Savepoint

```sql
RELEASE SAVEPOINT name;
```

Discards the named savepoint from the stack. Changes made since the savepoint **remain** part of the current transaction — `RELEASE` does not commit or roll back anything.

### Rolling Back to a Savepoint

```sql
ROLLBACK TO SAVEPOINT name;
```

Restores the transaction state to what it was when the savepoint was created:

- All data changes made after the savepoint are undone
- Catalog changes (DDL) after the savepoint are reverted
- The savepoint itself remains active — you can roll back to it again

### Example

```sql
BEGIN;
INSERT INTO users VALUES (1, 'Alice');

SAVEPOINT sp1;
INSERT INTO users VALUES (2, 'Bob');

SAVEPOINT sp2;
INSERT INTO users VALUES (3, 'Carol');

-- Undo Carol's insert, keep Alice and Bob
ROLLBACK TO SAVEPOINT sp2;

-- Discard sp1 (Alice and Bob remain)
RELEASE SAVEPOINT sp1;

COMMIT;
-- Result: users contains Alice (1) and Bob (2)
```

### Savepoint Nesting

Savepoints can be nested. Rolling back to an outer savepoint also discards all inner savepoints created after it:

```sql
BEGIN;
SAVEPOINT a;
  INSERT INTO t VALUES (1);
  SAVEPOINT b;
    INSERT INTO t VALUES (2);
  ROLLBACK TO SAVEPOINT a;
  -- Both inserts are undone; savepoint b is gone
COMMIT;
```

## Best Practices

### Keep Transactions Short

```sql
-- Good: Short transaction
BEGIN;
UPDATE inventory SET count = count - 1 WHERE id = 1;
INSERT INTO orders VALUES (...);
COMMIT;

-- Bad: Long transaction holding resources
BEGIN;
-- Do lots of work...
-- More work...
COMMIT;
```

### Use Transactions for Related Operations

```sql
-- Good: Atomic transfer
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;

-- Bad: Two separate operations (not atomic)
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
-- If crash happens here, money is lost!
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
```

### Handle Errors with Rollback

`BEGIN`/`COMMIT` works normally: if a statement fails and you `ROLLBACK`, all changes in the transaction are discarded.

Notes for the CLI:
- `decentdb exec --sql` may contain multiple statements separated by `;` and they run on a single connection.
- All statements are **parsed/bound up front** against the schema at the start of the call. That means `CREATE TABLE ...; INSERT INTO that_table ...;` in the same `--sql` will fail with “table not found”. Run schema changes as separate `exec` calls or use `decentdb repl`.

```bash
# Assuming users table already exists:
decentdb exec --db=my.ddb --sql="BEGIN; INSERT INTO users (name) VALUES ('Alice'); COMMIT"
```

## Transaction State

Check transaction status:

```bash
# Show database info including active readers/writers
decentdb info --db=my.ddb
```

## Limitations

- No distributed transactions
- Single writer only (no concurrent write transactions)
- Foreign keys enforced at statement time, not commit time
- Savepoints are only supported within explicit `BEGIN`/`COMMIT` blocks
- Multi-statement `exec` strings are bound against the starting schema (DDL in the same string doesn't affect later statements)
