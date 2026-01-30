# CLI Reference

The `decentdb` CLI provides all database operations through subcommands.

## Global Options

- `--db=<path>` - Database file path (required for most commands)
- `--help` - Show help for any command

## Commands

### exec

Execute SQL statements.

```bash
decentdb exec --db=<path> --sql=<statement> [options]
```

Options:
- `--sql=<statement>` - SQL statement to execute
- `--params=<type:value>` - Bind parameters (repeatable)
- `--timing` - Show execution timing
- `--format=<json|csv|table>` - Output format (default: json)
- `--checkpoint` - Force checkpoint after execution
- `--cachePages=<n>` - Number of pages to cache
- `--cacheMb=<n>` - Cache size in megabytes

Examples:
```bash
decentdb exec --db=my.db --sql="SELECT * FROM users"
decentdb exec --db=my.db --sql="INSERT INTO users VALUES (\$1, \$2)" --params=int:1 --params=text:Alice
decentdb exec --db=my.db --sql="SELECT * FROM users" --format=table
```

### list-tables

List all tables in the database.

```bash
decentdb list-tables --db=<path>
```

### describe

Show table structure.

```bash
decentdb describe --db=<path> --table=<name>
```

Options:
- `--table=<name>` - Table name to describe

### list-indexes

List all indexes.

```bash
decentdb list-indexes --db=<path> [--table=<name>]
```

Options:
- `--table=<name>` - Filter by table (optional)

### rebuild-index

Rebuild an index (compaction).

```bash
decentdb rebuild-index --db=<path> --index=<name>
```

Options:
- `--index=<name>` - Index name to rebuild

### verify-index

Verify index integrity.

```bash
decentdb verify-index --db=<path> --index=<name>
```

### import

Import data from CSV.

```bash
decentdb import --db=<path> --table=<name> --file=<path>
```

Options:
- `--table=<name>` - Target table
- `--file=<path>` - CSV file path

### export

Export table to CSV.

```bash
decentdb export --db=<path> --table=<name> --file=<path>
```

### checkpoint

Force WAL checkpoint.

```bash
decentdb exec --db=<path> --checkpoint
```

### stats

Show database statistics.

```bash
decentdb exec --db=<path> --dbInfo
```

### info

Show detailed database information.

```bash
decentdb exec --db=<path> --dbInfo --verbose
```

## Output Formats

### JSON (default)

```json
{
  "ok": true,
  "error": null,
  "rows": [
    "1|Alice|alice@example.com",
    "2|Bob|bob@example.com"
  ]
}
```

### CSV

```
id,name,email
1,Alice,alice@example.com
2,Bob,bob@example.com
```

### Table

```
id | name  | email
---|-------|------------------
1  | Alice | alice@example.com
2  | Bob   | bob@example.com
```

## Error Handling

When a command fails, the exit code is non-zero and output contains error details:

```json
{
  "ok": false,
  "error": {
    "code": "ERR_SQL",
    "message": "Table not found",
    "context": "users"
  },
  "rows": []
}
```

## Environment Variables

- `DECENTDB` - Path to decentdb CLI (used by test harness)
- `PGDATABASE` - PostgreSQL connection for differential tests
