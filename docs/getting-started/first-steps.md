# First Steps

Now that you have DecentDB installed, let's explore the basics.

## Creating Your First Database

DecentDB databases are single files, making them easy to manage:

```bash
# The database file is created automatically when you run your first command
decentdb exec --db=myapp.ddb --sql="CREATE TABLE users (id INT PRIMARY KEY, name TEXT)"
```

## Basic Operations

### Inserting Data

```bash
# Single row (id is auto-assigned when omitted)
decentdb exec --db=myapp.ddb --sql="INSERT INTO users (name) VALUES ('Alice')"

# With explicit id
decentdb exec --db=myapp.ddb --sql="INSERT INTO users VALUES (10, 'Bob')"

# With parameters (id auto-assigned)
decentdb exec --db=myapp.ddb --sql="INSERT INTO users (name) VALUES (\$1)" --params=text:Carol
```

### Querying Data

```bash
# All rows
decentdb exec --db=myapp.ddb --sql="SELECT * FROM users"

# With filter
decentdb exec --db=myapp.ddb --sql="SELECT * FROM users WHERE id = 1"

# Pattern matching
decentdb exec --db=myapp.ddb --sql="SELECT * FROM users WHERE name LIKE 'A%'"
decentdb exec --db=myapp.ddb --sql="SELECT * FROM users WHERE name LIKE 'A%'"
```

### Updating and Deleting

```bash
# Update a row
decentdb exec --db=myapp.ddb --sql="UPDATE users SET name = 'Alice Smith' WHERE id = 1"

# Delete a row
decentdb exec --db=myapp.ddb --sql="DELETE FROM users WHERE id = 3"
```

## Working with Multiple Tables

### Creating Related Tables

```bash
# Create tables with foreign keys
decentdb exec --db=myapp.ddb --sql="CREATE TABLE artists (id INT PRIMARY KEY, name TEXT)"
decentdb exec --db=myapp.ddb --sql="CREATE TABLE albums (id INT PRIMARY KEY, artist_id INT REFERENCES artists(id), title TEXT)"

# Insert related data
decentdb exec --db=myapp.ddb --sql="INSERT INTO artists (name) VALUES ('The Beatles')"
decentdb exec --db=myapp.ddb --sql="INSERT INTO albums (artist_id, title) VALUES (1, 'Abbey Road')"
```

### Joining Tables

```bash
# Join query
decentdb exec --db=myapp.ddb --sql="SELECT artists.name, albums.title FROM artists JOIN albums ON artists.id = albums.artist_id"
```

## Using Indexes

### Creating Indexes

```bash
# B-tree index for fast lookups
decentdb exec --db=myapp.ddb --sql="CREATE INDEX idx_users_name ON users(name)"

# Trigram index for text search
decentdb exec --db=myapp.ddb --sql="CREATE INDEX idx_users_name_trgm ON users USING trigram(name)"
```

### Using Indexed Queries

```bash
# Fast exact match (uses index)
decentdb exec --db=myapp.ddb --sql="SELECT * FROM users WHERE name = 'Alice'"

# Fast pattern search (uses trigram index)
decentdb exec --db=myapp.ddb --sql="SELECT * FROM users WHERE name LIKE '%lic%'"
```

## Transactions

Group multiple operations into atomic transactions:

```bash
# Begin transaction
decentdb exec --db=myapp.ddb --sql="BEGIN"

# Multiple operations
decentdb exec --db=myapp.ddb --sql="INSERT INTO users (name) VALUES ('Dave')"
decentdb exec --db=myapp.ddb --sql="INSERT INTO users (name) VALUES ('Eve')"

# Commit (or ROLLBACK to cancel)
decentdb exec --db=myapp.ddb --sql="COMMIT"
```

## Schema Management

### Viewing Schema

```bash
# List all tables
decentdb list-tables --db=myapp.ddb

# Describe a table
decentdb describe --db=myapp.ddb --table=users

# List indexes
decentdb list-indexes --db=myapp.ddb
```

### Modifying Schema

```bash
# Drop an index
decentdb exec --db=myapp.ddb --sql="DROP INDEX idx_users_name"

# Drop a table
decentdb exec --db=myapp.ddb --sql="DROP TABLE users"
```

## Next Steps

- Learn about [Data Types](data-types.md)
- Explore [Performance Tuning](performance.md)
- Read the full [SQL Reference](sql-reference.md)
- Check out the [CLI Reference](../api/cli-reference.md)
