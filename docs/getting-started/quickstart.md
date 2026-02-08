# Quick Start

This guide will get you up and running with DecentDB in 5 minutes.

## Create Your First Database

```bash
# Create a database file (automatically created on first access)
decentdb exec --db=myapp.ddb --sql="CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT)"
```

## Insert Data

```bash
# Insert a single row (id is auto-assigned)
decentdb exec --db=myapp.ddb --sql="INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')"

# Insert with explicit id
decentdb exec --db=myapp.ddb --sql="INSERT INTO users VALUES (10, 'Bob', 'bob@example.com')"

# Insert with parameters (id auto-assigned)
decentdb exec --db=myapp.ddb --sql="INSERT INTO users (name, email) VALUES (\$1, \$2)" \
  --params=text:Carol --params=text:carol@example.com
```

## Query Data

```bash
# Select all users
decentdb exec --db=myapp.ddb --sql="SELECT * FROM users"

# Select with WHERE clause
decentdb exec --db=myapp.ddb --sql="SELECT * FROM users WHERE id = 1"

# Update a user
decentdb exec --db=myapp.ddb --sql="UPDATE users SET name = 'Alice Smith' WHERE id = 1"

# Delete a user
decentdb exec --db=myapp.ddb --sql="DELETE FROM users WHERE id = 2"
```

## Create Indexes

```bash
# Create a regular index
decentdb exec --db=myapp.ddb --sql="CREATE INDEX idx_users_email ON users(email)"

# Create a trigram index for text search
decentdb exec --db=myapp.ddb --sql="CREATE INDEX idx_users_name_trgm ON users USING trigram(name)"

# Search with trigram index
decentdb exec --db=myapp.ddb --sql="SELECT * FROM users WHERE name LIKE '%ali%'"
```

## Schema Management

```bash
# List all tables
decentdb list-tables --db=myapp.ddb

# Describe a table
decentdb describe --db=myapp.ddb --table=users

# List indexes
decentdb list-indexes --db=myapp.ddb

# Drop a table
decentdb exec --db=myapp.ddb --sql="DROP TABLE users"
```

## Transactions

```bash
# Execute multiple statements atomically
decentdb exec --db=myapp.ddb --sql="BEGIN; INSERT INTO users VALUES (3, 'Carol', 'carol@example.com'); INSERT INTO users VALUES (4, 'Dave', 'dave@example.com'); COMMIT"
```

## Advanced Queries

```bash
# Aggregates and GROUP BY
decentdb exec --db=myapp.ddb --sql="SELECT COUNT(*) FROM users"

# DISTINCT values
decentdb exec --db=myapp.ddb --sql="SELECT DISTINCT name FROM users"

# JOINs
decentdb exec --db=myapp.ddb --sql="SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id"

# Upsert (insert or update on conflict)
decentdb exec --db=myapp.ddb --sql="INSERT INTO users (id, name) VALUES (1, 'Alice Updated') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name"

# INSERT RETURNING (get auto-assigned id)
decentdb exec --db=myapp.ddb --sql="INSERT INTO users (name, email) VALUES ('Eve', 'eve@example.com') RETURNING id"
```

## Bulk Loading

For large datasets, use the bulk load API:

```bash
# Create a CSV file
cat > users.csv << EOF
id,name,email
1,Alice,alice@example.com
2,Bob,bob@example.com
3,Carol,carol@example.com
EOF

# Import the CSV
decentdb import --db=myapp.ddb --table=users --input=users.csv
```

## Checkpoint and Maintenance

```bash
# Force a WAL checkpoint
decentdb checkpoint --db=myapp.ddb

# Get database statistics
decentdb stats --db=myapp.ddb
```

## Next Steps

- Learn about [SQL features](../user-guide/sql-reference.md)
- Understand [data types](../user-guide/data-types.md)
- Optimize performance with [indexes](../user-guide/indexes.md)
- Read the full [API reference](../api/cli-reference.md)
