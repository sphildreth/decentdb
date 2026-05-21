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

When the shell starts it prints the DecentDB CLI version, the DecentDB banner,
and:

```text
Type "help" for help.
```

Open a branch-scoped REPL with `--branch`:

```bash
decentdb repl --db ./app.ddb --branch work
```

Branch-local writes are isolated from `main`. For full branch workflows,
including diff, restore, and merge, see
[Branching, Diff, Restore, And Time Travel](branching.md).

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

Exit the session with `.exit`, `.quit`, `\q`, or end-of-file.

```text
decentdb> .exit
```

## Help And Special Commands

Help is available through `help`, `\?`, `/?`, `/help`, `\help`, and `.help`.
Use topic-specific help for focused command groups:

```text
help schema
help output
help files
help parameters
help branches
help explain
```

The REPL supports these dot commands:

| Command | Meaning |
|---|---|
| `.tables`, `.dt` | List tables and row counts. |
| `.d <table>` | Show columns, types, and constraints. |
| `.schema [object]` | Show DDL for all schema objects or one object. |
| `.indexes [table]` | List indexes, optionally for one table. |
| `.views` | List views. |
| `.df`, `.functions` | List built-in functions. |
| `.g` | Run the last completed SQL command again. |
| `.s`, `.history` | Show commands entered in this session. |
| `.mode <mode>` | Set `table`, `csv`, `json`, or `markdown` output. |
| `.headers on\|off` | Show or hide headers in table, CSV, and Markdown output. |
| `.nullvalue <text>` | Set rendered NULL text for text outputs. |
| `.width [n ...\|auto]` | Set table column widths or return to automatic widths. |
| `.timer on\|off` | Show elapsed time after SQL execution. |
| `.read <file>` | Run SQL and dot commands from a file. |
| `.output <file\|stdout>` | Redirect subsequent command output. |
| `.once <file>` | Redirect the next command output only. |
| `.import <csv-file> <table> [batch-size]` | Bulk-load CSV rows into a table. |
| `.export <table> <file> [csv\|json]` | Export a table to CSV or JSON. |
| `.explain <sql>`, `.plan <sql>` | Run `EXPLAIN`. |
| `.explain-analyze <sql>` | Run `EXPLAIN ANALYZE`. |
| `.param list` | Show positional parameter values. |
| `.param set <index> <type:value>` | Set a `$1`-style positional parameter. |
| `.param unset <index>` | Reset a parameter to NULL. |
| `.param clear` | Clear all parameters. |
| `.branch` | Print the active branch name. |
| `.branch <branch>` | Create a branch from the current branch and check it out. |
| `.checkout <branch>` | Switch to another branch or `main`. |
| `.quit`, `.exit`, `\q` | Exit the REPL. |

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

You can also change output while the REPL is running:

```text
.mode csv
.headers off
.nullvalue (null)
.timer on
```

Use `.output` to redirect subsequent output and `.once` to redirect only the
next output:

```text
.output report.csv
SELECT * FROM users;
.output stdout

.once one-query.md
.mode markdown
SELECT id, name FROM users;
```

## Schema Inspection

Use `.tables` or `.dt` to list tables, `.d <table>` to inspect a table, and
`.schema` to print canonical schema DDL.

```text
.tables
.d users
.schema users
.indexes users
.views
```

## Files, Import, And Export

`.read <file>` executes SQL and REPL dot commands from a file. If the file ends
with an incomplete SQL statement, the REPL reports the incomplete statement and
returns to the prompt without poisoning the next interactive command.

```text
.read ./setup.sql
```

CSV import uses the same bulk-load path as the CLI `import` command:

```text
.import ./users.csv users
.import ./large-users.csv users 50000
```

Export table rows to CSV or JSON:

```text
.export users ./users.csv csv
.export users ./users.json json
```

## Parameters

Use `$1`, `$2`, and later positional parameters in SQL, then set values with
`.param`:

```text
.param set 1 int:42
.param set 2 text:Alice
SELECT * FROM users WHERE id = $1 OR name = $2;
.param list
.param clear
```

Supported parameter values are `null`, `int:value`, `float:value`,
`bool:value`, `text:value`, `timestamp:value`, and `blob:hex`.

## Explain Helpers

Use `.explain`, `.plan`, and `.explain-analyze` for interactive planning:

```text
.explain SELECT * FROM users WHERE id = 1;
.explain-analyze SELECT * FROM users;
```

When no SQL is supplied, the explain helper uses the last completed SQL command.

## History

The REPL stores command history in:

```text
~/.decentdb_history
```

History loading and saving are best-effort. If the history file does not exist
or cannot be written, the REPL still runs.

## Related Pages

- [CLI Reference](../api/cli-reference.md#repl)
- [SQL Reference](sql-reference.md)
- [Transactions](transactions.md)
- [First Steps](../getting-started/first-steps.md)
