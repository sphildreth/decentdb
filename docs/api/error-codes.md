# Error Codes

DecentDb uses specific error codes to indicate different types of failures.

## Error Code Reference

### ERR_IO (I/O Error)

File system operations failed.

**Common Causes:**
- Disk full
- Permission denied
- File not found
- Hardware failure

**Example:**
```bash
decentdb exec --db=/readonly/my.ddb --sql="SELECT 1"
# Error: ERR_IO: Permission denied
```

**Resolution:**
- Check disk space: `df -h`
- Check permissions: `ls -la`
- Verify path exists

### ERR_CORRUPTION (Database Corruption)

Database file structure is invalid.

**Common Causes:**
- Unexpected shutdown during write
- Storage hardware failure
- Manual file modification
- Bug in database engine

**Example:**
```bash
decentdb exec --db=corrupted.ddb --sql="SELECT 1"
# Error: ERR_CORRUPTION: Invalid page checksum
```

**Resolution:**
- Restore from backup
- Use `PRAGMA integrity_check`
- Contact support if reproducible

### ERR_CONSTRAINT (Constraint Violation)

Data violates schema constraints.

**Common Causes:**
- Duplicate PRIMARY KEY
- Violates UNIQUE constraint
- Violates NOT NULL
- Violates FOREIGN KEY

**Example:**
```sql
-- Table with UNIQUE constraint
CREATE TABLE users (id INT PRIMARY KEY, email TEXT UNIQUE);

INSERT INTO users VALUES (1, 'alice@example.com');
INSERT INTO users VALUES (2, 'alice@example.com');
-- Error: ERR_CONSTRAINT: UNIQUE constraint violation
```

**Resolution:**
- Check for existing values
- Handle duplicates in application
- Use INSERT OR REPLACE (if supported)

### ERR_TRANSACTION (Transaction Error)

Transaction-related failures.

**Common Causes:**
- Deadlock (rare in DecentDb)
- Lock timeout
- Write conflict

**Example:**
```bash
# Two processes trying to write simultaneously
# Process 1: BEGIN; UPDATE ...
# Process 2: BEGIN; UPDATE ... (waits)
```

**Resolution:**
- Retry the transaction
- Keep transactions short
- Use appropriate isolation level

### ERR_SQL (SQL Error)

Invalid SQL syntax or semantic error.

**Common Causes:**
- Syntax error in SQL
- Table doesn't exist
- Column doesn't exist
- Type mismatch

**Example:**
```bash
decentdb exec --db=my.ddb --sql="SELECT * FROM nonexistent"
# Error: ERR_SQL: Table not found

decentdb exec --db=my.ddb --sql="SELEC * FROM users"
# Error: ERR_SQL: Syntax error
```

**Resolution:**
- Verify table/column names
- Check SQL syntax
- Use DESCRIBE to see schema

### ERR_INTERNAL (Internal Error)

Unexpected internal engine error.

**Common Causes:**
- Bug in DecentDb
- Memory allocation failure
- Internal assertion failure

**Example:**
```
Error: ERR_INTERNAL: Unexpected null pointer
```

**Resolution:**
- Report bug with reproduction steps
- Check system resources
- Restart application

## Error Response Format

CLI errors are returned as JSON:

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

Fields:
- `code`: Error category (see above)
- `message`: Human-readable description
- `context`: Additional context (table name, column name, etc.)

## Nim API Error Handling

```nim
import decentdb/engine

let res = execSql(db, "SELECT * FROM users")
if not res.ok:
  echo "Error code: ", res.err.code
  echo "Message: ", res.err.message
  echo "Context: ", res.err.context
  
  case res.err.code
  of ERR_SQL:
    echo "Fix your SQL query"
  of ERR_CONSTRAINT:
    echo "Check your data"
  of ERR_IO:
    echo "Check file system"
  else:
    echo "Unexpected error"
```

## Common Error Patterns

### "Table not found"

```
ERR_SQL: Table not found: users
```

**Check:**
1. Did you create the table?
2. Is the database path correct?
3. Are you using the right database file?

### "UNIQUE constraint violation"

```
ERR_CONSTRAINT: UNIQUE constraint violation: email
```

**Check:**
1. Does the value already exist?
2. Are you inserting duplicates?

### "FOREIGN KEY constraint violation"

```
ERR_CONSTRAINT: FOREIGN KEY constraint violation: user_id
```

**Check:**
1. Does the referenced row exist?
2. Is the foreign key value correct?

### "Missing parameter"

```
ERR_SQL: Missing parameter: 1
```

**Check:**
1. Did you provide all parameters?
2. Are parameter indices correct ($1, $2, etc.)?

### "Type mismatch"

```
ERR_SQL: Type mismatch for column: age
```

**Check:**
1. Are you passing the right type (int vs text)?
2. Is the column type what you expect?

## Troubleshooting Guide

### Database Won't Open

1. Check file permissions
2. Verify file isn't corrupted
3. Ensure sufficient disk space
4. Check if another process has locked it

### Queries Are Slow

1. Check if indexes exist: `list-indexes`
2. Analyze query plan
3. Consider adding indexes
4. Check cache size

### Write Failures

1. Check disk space
2. Verify write permissions
3. Check if database is read-only
4. Ensure single writer

### Recovery Mode

If database is corrupted:

```bash
# Check integrity
decentdb exec --db=my.ddb --sql="PRAGMA integrity_check"

# Export and reimport if needed
decentdb export --db=my.ddb --table=users --output=users.csv
# Create new database and import
```

## Getting Help

If you encounter an error:

1. Check this error code reference
2. Review the [SQL Reference](../user-guide/sql-reference.md)
3. Check [common issues on GitHub](https://github.com/sphildreth/decentdb/issues)
4. Report new issues with:
   - Error code and message
   - Steps to reproduce
   - Expected vs actual behavior
