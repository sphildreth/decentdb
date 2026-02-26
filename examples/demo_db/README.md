# Demo Database (for DBeaver / benchmarks)

This folder contains a reproducible generator for a **tooling-friendly** DecentDB database.

It’s intended for:
- Testing GUI tools (DBeaver, etc.)
- Benchmarking queries and indexes
- Exercising DecentDB features (types, constraints, indexes, views, triggers)

## Generate the DB

From the repo root:

```bash
nim c -r examples/demo_db/make_demo_db.nim --out=./demo.ddb --seed=1 --users=2000 --postsPerUser=5
```

Size presets (still overridable with `--users` / `--postsPerUser`):

```bash
# Tiny
nim c -d:release -r examples/demo_db/make_demo_db.nim --out=./demo_tiny.ddb --seed=1 --tiny

# Small
nim c -d:release -r examples/demo_db/make_demo_db.nim --out=./demo_small.ddb --seed=1 --small

# Medium
nim c -d:release -r examples/demo_db/make_demo_db.nim --out=./demo_medium.ddb --seed=1 --medium

# Large
nim c -d:release -r examples/demo_db/make_demo_db.nim --out=./demo_large.ddb --seed=1 --large

# Extra large (alias: --jumbo)
nim c -d:release -r examples/demo_db/make_demo_db.nim --out=./demo_xlarge.ddb --seed=1 --xlarge
```

All commands above assume your current directory is the DecentDB repo root. If you `cd examples/demo_db`, invoke the generator as `nim c -d:release -r make_demo_db.nim ...`.

Note on WAL / checkpointing:
- This generator produces a WAL-backed database, so you may see a sibling `*.ddb-wal` file.
- You do not need to VACUUM or rebuild indexes after generation.
- If you want a single self-contained `*.ddb` file for easy copying/sharing, run a checkpoint (flush WAL into the main DB) before moving the file. Otherwise, keep the `*.ddb` and `*.ddb-wal` together.

This creates:
- `./demo.ddb`
- `./demo.ddb-wal`

## What it contains

### Types
Table `demo_types` includes examples of:
- `INTEGER` / int64
- `REAL` / float64
- `BOOL`
- `DECIMAL(18,6)`
- `TEXT`
- `BLOB`
- `UUID`
- `DATE`, `TIMESTAMP` keywords (stored as `TEXT`)
- JSON stored as `TEXT` (use JSON_* functions)

### Features
- Primary keys + composite PK (`post_tags`)
- Foreign keys (`posts.user_id → users.id`, `post_tags`)
  - Note: DBeaver ER diagrams currently rely on JDBC FK metadata derived from per-column `REFERENCES` info; table-level `FOREIGN KEY (...) REFERENCES ...` constraints may not show diagram lines yet.
- `CHECK` constraint (`demo_types.int64_val >= 0`)
- Indexes:
  - BTREE: `idx_posts_user_created`
  - Expression: `idx_users_lower_username` (`LOWER(username)`)
  - Partial: `idx_users_email_not_null`
  - Trigram: `idx_users_bio_trgm`, `idx_posts_body_trgm`
- Views:
  - `v_user_post_counts`
  - `v_recent_posts`
- Trigger:
  - `users_ins_audit` writes to `audit_log` via `decentdb_exec_sql(...)`
- Savepoints (exercised during generation)

## Handy queries (copy/paste into DBeaver)

Trigram LIKE search (should use trigram index):
```sql
SELECT id, username, bio
FROM users
WHERE bio LIKE '%dbeaver%'
LIMIT 20;
```

Join + aggregation:
```sql
SELECT u.username, COUNT(p.id) AS posts
FROM users u
LEFT JOIN posts p ON p.user_id = u.id
GROUP BY u.username
ORDER BY posts DESC
LIMIT 20;
```

UUID formatting sanity check:
```sql
SELECT id, UUID_TO_STRING(external_id) AS external_id
FROM users
ORDER BY id
LIMIT 5;
```

Generated column:
```sql
SELECT id, text_val, text_len
FROM demo_types
ORDER BY id;
```

Audit trigger results:
```sql
SELECT * FROM audit_log ORDER BY id DESC LIMIT 20;
```
