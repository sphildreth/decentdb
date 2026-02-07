# Quick Start

This guide will get you up and running with DecentDB in 5 minutes.

## Create Your First Database

```bash
# Create a database file (automatically created on first access)
decentdb exec --db=myapp.ddb --sql="CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT)"
```

## Insert Data

```bash
# Insert a single row
decentdb exec --db=myapp.ddb --sql="INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')"

# Insert with parameters
decentdb exec --db=myapp.ddb --sql="INSERT INTO users VALUES (\$1, \$2, \$3)" \
  --params=int:2 --params=text:Bob --params=text:bob@example.com
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
# Begin a transaction
decentdb exec --db=myapp.ddb --sql="BEGIN"

# Multiple operations in a transaction
decentdb exec --db=myapp.ddb --sql="INSERT INTO users VALUES (3, 'Carol', 'carol@example.com')"
decentdb exec --db=myapp.ddb --sql="INSERT INTO users VALUES (4, 'Dave', 'dave@example.com')"

# Commit the transaction
decentdb exec --db=myapp.ddb --sql="COMMIT"

# Or rollback
decentdb exec --db=myapp.ddb --sql="ROLLBACK"
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

# Verify database integrity
decentdb exec --db=myapp.ddb --sql="PRAGMA integrity_check"
```

## Next Steps

- Learn about [SQL features](../user-guide/sql-reference.md)
- Understand [data types](../user-guide/data-types.md)
- Optimize performance with [indexes](../user-guide/indexes.md)
- Read the full [API reference](../api/cli-reference.md)
