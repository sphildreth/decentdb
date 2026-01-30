# First Steps

Now that you have DecentDb installed, let's explore the basics.

## Creating Your First Database

DecentDb databases are single files, making them easy to manage:

```bash
# The database file is created automatically when you run your first command
decentdb exec --db=myapp.db --sql="CREATE TABLE users (id INT PRIMARY KEY, name TEXT)"
```

## Basic Operations

### Inserting Data

```bash
# Single row
decentdb exec --db=myapp.db --sql="INSERT INTO users VALUES (1, 'Alice')"

# Multiple rows with parameters
decentdb exec --db=myapp.db --sql="INSERT INTO users VALUES (\$1, \$2)" --params=int:2 --params=text:Bob
decentdb exec --db=myapp.db --sql="INSERT INTO users VALUES (\$1, \$2)" --params=int:3 --params=text:Carol
```

### Querying Data

```bash
# All rows
decentdb exec --db=myapp.db --sql="SELECT * FROM users"

# With filter
decentdb exec --db=myapp.db --sql="SELECT * FROM users WHERE id = 1"

# Pattern matching
decentdb exec --db=myapp.db --sql="SELECT * FROM users WHERE name LIKE 'A%'"
```

### Updating and Deleting

```bash
# Update a row
decentdb exec --db=myapp.db --sql="UPDATE users SET name = 'Alice Smith' WHERE id = 1"

# Delete a row
decentdb exec --db=myapp.db --sql="DELETE FROM users WHERE id = 3"
```

## Working with Multiple Tables

### Creating Related Tables

```bash
# Create tables with foreign keys
decentdb exec --db=myapp.db --sql="CREATE TABLE artists (id INT PRIMARY KEY, name TEXT)"
decentdb exec --db=myapp.db --sql="CREATE TABLE albums (id INT PRIMARY KEY, artist_id INT REFERENCES artists(id), title TEXT)"

# Insert related data
decentdb exec --db=myapp.db --sql="INSERT INTO artists VALUES (1, 'The Beatles')"
decentdb exec --db=myapp.db --sql="INSERT INTO albums VALUES (1, 1, 'Abbey Road')"
```

### Joining Tables

```bash
# Join query
decentdb exec --db=myapp.db --sql="SELECT artists.name, albums.title FROM artists JOIN albums ON artists.id = albums.artist_id"
```

## Using Indexes

### Creating Indexes

```bash
# B-tree index for fast lookups
decentdb exec --db=myapp.db --sql="CREATE INDEX idx_users_name ON users(name)"

# Trigram index for text search
decentdb exec --db=myapp.db --sql="CREATE INDEX idx_users_name_trgm ON users USING trigram(name)"
```

### Using Indexed Queries

```bash
# Fast exact match (uses index)
decentdb exec --db=myapp.db --sql="SELECT * FROM users WHERE name = 'Alice'"

# Fast pattern search (uses trigram index)
decentdb exec --db=myapp.db --sql="SELECT * FROM users WHERE name LIKE '%lic%'"
```

## Transactions

Group multiple operations into atomic transactions:

```bash
# Begin transaction
decentdb exec --db=myapp.db --sql="BEGIN"

# Multiple operations
decentdb exec --db=myapp.db --sql="INSERT INTO users VALUES (4, 'Dave')"
decentdb exec --db=myapp.db --sql="INSERT INTO users VALUES (5, 'Eve')"

# Commit (or ROLLBACK to cancel)
decentdb exec --db=myapp.db --sql="COMMIT"
```

## Schema Management

### Viewing Schema

```bash
# List all tables
decentdb list-tables --db=myapp.db

# Describe a table
decentdb describe --db=myapp.db --table=users

# List indexes
decentdb list-indexes --db=myapp.db
```

### Modifying Schema

```bash
# Drop an index
decentdb exec --db=myapp.db --sql="DROP INDEX idx_users_name"

# Drop a table
decentdb exec --db=myapp.db --sql="DROP TABLE users"
```

## Next Steps

- Learn about [Data Types](data-types.md)
- Explore [Performance Tuning](performance.md)
- Read the full [SQL Reference](sql-reference.md)
- Check out the [CLI Reference](../api/cli-reference.md)
