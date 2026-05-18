# Interactive SQL Shell

`decentdb repl` opens an interactive SQL shell against a DecentDB database. Use
it when you want to explore data, run statements one at a time, keep a
transaction open while you inspect results, or run schema changes followed by
dependent statements in the same session.

## What Is A REPL?

REPL stands for **Read-Evaluate-Print Loop**:

1. read a command or SQL statement
2. evaluate it
3. print the result
4. loop back for the next input

The term is common in programming language tools such as Lisp, Python, Node.js,
and Rust. Database products more often call the same experience an interactive
shell, SQL shell, terminal, or command-line client. In DecentDB, the command is
named `decentdb repl`, while the user-facing docs describe it as the interactive
SQL shell.

## Start The REPL

```bash
decentdb repl --db ./app.ddb
```

The database is created if it does not already exist.

Choose an output format with `--format`:

```bash
decentdb repl --db ./app.ddb --format table
decentdb repl --db ./app.ddb --format json
decentdb repl --db ./app.ddb --format csv
decentdb repl --db ./app.ddb --format markdown
```

`table` is the default because the REPL is primarily meant for humans.

## Basic Session

```sql
CREATE TABLE users (
  id INT PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT UNIQUE
);

INSERT INTO users (name, email)
VALUES ('Alice', 'alice@example.com')
RETURNING id;

SELECT id, name, email
FROM users
ORDER BY id;
```

Exit the session with `.exit`, `.quit`, or end-of-file.

```text
decentdb> .exit
```

## Special Commands

The REPL currently supports a small command set:

| Command | Meaning |
|---|---|
| `.help` | Print the available REPL commands. |
| `.exit` | Exit the REPL. |
| `.quit` | Exit the REPL. |

Other administrative operations, such as `sync`, `doctor`, `import`, `export`,
`describe`, and `list-tables`, are CLI commands. Run those in your shell rather
than inside the REPL.

## Statement Completion

Most SQL statements must end with a semicolon before the REPL executes them:

```sql
CREATE TABLE items (
  id INT PRIMARY KEY,
  name TEXT NOT NULL
);
```

The prompt changes while a statement is incomplete:

```text
decentdb> CREATE TABLE items (
...>   id INT PRIMARY KEY,
...>   name TEXT NOT NULL
...> );
```

Semicolons inside quoted strings do not complete the statement:

```sql
INSERT INTO notes (body) VALUES ('keep this; semicolon');
```

Transaction control statements can be entered without a semicolon:

```sql
BEGIN
COMMIT
ROLLBACK
```

Using semicolons for transaction statements is still fine and usually clearer:

```sql
BEGIN;
COMMIT;
ROLLBACK;
```

## Transactions

The REPL keeps one database connection open for the duration of the session, so
interactive transactions work naturally.

```sql
BEGIN;

INSERT INTO users (name, email)
VALUES ('Bob', 'bob@example.com');

SELECT * FROM users;

COMMIT;
```

When a transaction is open, the prompt changes from `decentdb>` to
`decentdb*>`:

```text
decentdb*> 
```

Savepoints work the same way:

```sql
BEGIN;
SAVEPOINT before_import;

INSERT INTO users (name, email)
VALUES ('Carol', 'carol@example.com');

ROLLBACK TO SAVEPOINT before_import;
COMMIT;
```

## DDL Followed By DML

`decentdb exec --sql` can run multiple statements, but it parses and binds the
whole SQL string before execution. That means a single `exec` call can fail when
it creates a table and then immediately inserts into that new table.

The REPL is the recommended path for that workflow because each completed
statement is executed before the next one is parsed:

```sql
CREATE TABLE projects (
  id INT PRIMARY KEY,
  name TEXT NOT NULL
);

INSERT INTO projects (name) VALUES ('Launch');
SELECT * FROM projects;
```

You can also use separate `decentdb exec` calls from the shell.

## Output Formats

`--format table` prints human-readable tables:

```bash
decentdb repl --db ./app.ddb --format table
```

`--format json` prints each completed statement batch as a JSON execution
result. This can be useful when piping scripted input into the REPL:

```bash
printf "SELECT 1 AS n;\n.exit\n" | decentdb repl --db ./app.ddb --format json
```

`--format csv` and `--format markdown` are useful for copying result sets into
other tools or documentation.

## History

The REPL stores command history in:

```text
~/.decentdb_history
```

History loading and saving are best-effort. If the history file does not exist
or cannot be written, the REPL still runs.

## Current Limits

- The REPL is a SQL shell, not a full administrative shell.
- It does not currently support `.tables`, `.schema`, `.read`, `.mode`, or
  other SQLite-style meta commands.
- It does not provide per-statement parameter binding like `decentdb exec
  --params`; write literal SQL values or use an application binding when you
  need bound parameters.
- The local HTTP sync transport is managed through `decentdb sync ...` commands
  outside the REPL.

## Related Pages

- [CLI Reference](../api/cli-reference.md#repl)
- [SQL Reference](sql-reference.md)
- [Transactions](transactions.md)
- [First Steps](../getting-started/first-steps.md)
