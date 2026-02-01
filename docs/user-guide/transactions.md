# Transactions

DecentDb supports ACID transactions with full durability guarantees.

## Transaction Basics

### Starting a Transaction

```sql
BEGIN;
```

Or using the CLI:
```bash
decentdb exec --db=my.ddb --sql="BEGIN"
```

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

DecentDb uses **Snapshot Isolation**:
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

DecentDb enforces single writer semantics:

- Only one write transaction at a time
- Write transactions are serialized
- No deadlocks possible
- Readers see stable snapshots

## Durability Modes

### Full (Default)

```sql
PRAGMA wal_sync_mode = FULL;
```

- fsync on every commit
- Maximum durability
- Slower performance

### Normal

```sql
PRAGMA wal_sync_mode = NORMAL;
```

- fdatasync on commit
- Good balance of safety and speed
- Recommended for most applications

### Deferred (Bulk Operations)

Use bulk load API for large imports:

```bash
decentdb bulk-load --db=my.ddb --table=users --file=users.csv --durability=deferred
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

```bash
#!/bin/bash
set -e

decentdb exec --db=my.ddb --sql="BEGIN"

if ! decentdb exec --db=my.ddb --sql="INSERT INTO users VALUES (1, 'Alice')"; then
    decentdb exec --db=my.ddb --sql="ROLLBACK"
    echo "Transaction failed, rolled back"
    exit 1
fi

decentdb exec --db=my.ddb --sql="COMMIT"
```

## Transaction State

Check transaction status:

```bash
# Show database info including active readers/writers
decentdb exec --db=my.ddb --dbInfo --verbose
```

## Limitations

- No SAVEPOINT support (nested transactions)
- No distributed transactions
- Single writer only (no concurrent write transactions)
- Foreign keys enforced at statement time, not commit time
