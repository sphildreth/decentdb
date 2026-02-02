## DecentDb Unified CLI
## 
## This is the main entry point for all DecentDb command-line utilities.

import cligen
import ./decentdb_cli

const Version = "0.0.1"

when isMainModule:
  dispatchMulti(
    ["multi", doc="DecentDb CLI v" & Version & "\n\nA unified tool for DecentDb operations."],
    
    # 1. Main SQL Execution
    [decentdb_cli.cliMain, 
     cmdName = "exec",
     doc = "Execute SQL statements or manage database",
     help = {
       "db": "Path to database file (required)",
       "sql": "SQL statement to execute",
       "openClose": "Open and close database without executing SQL (testing mode)",
       "timing": "Show query execution timing in milliseconds",
       "cachePages": "Number of 4KB pages to cache (default: 1024 = 4MB)",
       "cacheMb": "Cache size in megabytes (overrides --cachePages if specified)",
       "checkpoint": "Force a WAL checkpoint and exit",
       "readerCount": "Show number of active readers and exit",
       "longReaders": "Show readers active longer than N milliseconds",
       "dbInfo": "Display database information (format, size, cache, LSN) and exit",
       "warnings": "Include WAL warnings in output",
       "verbose": "Include verbose diagnostics (LSN, readers, cache) in output",
       "checkpointBytes": "Auto-checkpoint when WAL reaches N bytes",
       "checkpointMs": "Auto-checkpoint when N milliseconds elapse since last checkpoint",
       "readerWarnMs": "Warn when readers are older than N milliseconds (0 = disabled)",
       "readerTimeoutMs": "Abort/truncate for readers older than N milliseconds (0 = disabled)",
       "forceTruncateOnTimeout": "Force WAL truncation when readerTimeoutMs triggers (dangerous; use for testing)",
       "format": "Output format: json, csv, table (default: json). Note: timing/warnings/verbose currently require json",
       "params": "Bind parameters in order (repeatable). Use type:value, e.g. int:1, text:hi, null",
       "walFailpoints": "Set WAL failpoints (repeatable). Format: label:kind[:bytes][:count]",
       "clearWalFailpoints": "Clear all WAL failpoints before executing"
     },
     short = {
       "db": 'd',
       "sql": 's',
       "timing": 't',
       "verbose": 'v'
     }],
     
    # 2. Schema Tools
    [decentdb_cli.schemaListTables,
     cmdName = "list-tables",
     doc = "List all tables in the database",
     help = {
       "db": "Path to database file (required)"
     },
     short = {
       "db": 'd'
     }],
    [decentdb_cli.schemaDescribe,
     cmdName = "describe",
     doc = "Describe table structure",
     help = {
       "table": "Table name to describe",
       "db": "Path to database file (required)"
     },
     short = {
       "db": 'd',
       "table": 't'
     }],
    [decentdb_cli.schemaListIndexes,
     cmdName = "list-indexes",
     doc = "List all indexes",
     help = {
       "db": "Path to database file (required)",
       "table": "Optional table name to filter indexes"
     },
     short = {
       "db": 'd',
       "table": 't'
     }],
    [decentdb_cli.cmdRebuildIndex,
     cmdName = "rebuild-index",
     doc = "Rebuild an index",
     help = {
       "index": "Index name to rebuild",
       "db": "Path to database file (required)"
     },
     short = {
       "db": 'd',
       "index": 'i'
     }],

    [decentdb_cli.cmdRebuildIndexes,
     cmdName = "rebuild-indexes",
     doc = "Rebuild all indexes",
     help = {
       "db": "Path to database file (required)",
       "table": "Optional table name to filter indexes"
     },
     short = {
       "db": 'd',
       "table": 't'
     }],
    [decentdb_cli.cmdVerifyIndex,
     cmdName = "verify-index",
     doc = "Verify index integrity",
     help = {
       "index": "Index name to verify",
       "db": "Path to database file (required)"
     },
     short = {
       "db": 'd',
       "index": 'i'
     }],
     
    # 3. Data Tools
    [decentdb_cli.importData,
     cmdName = "import",
     doc = "Import data from CSV or JSON",
     help = {
       "table": "Table name to import into",
       "input": "Input file path",
       "db": "Path to database file (required)",
       "batchSize": "Number of rows per batch (default: 10000)",
       "format": "Import format: csv or json (default: csv)"
     },
     short = {
       "db": 'd',
       "table": 't'
     }],
    [decentdb_cli.exportData,
     cmdName = "export",
     doc = "Export table data to CSV or JSON",
     help = {
       "table": "Table name to export",
       "output": "Output file path",
       "db": "Path to database file (required)",
       "format": "Export format: csv or json (default: csv)"
     },
     short = {
       "db": 'd',
       "table": 't'
     }],
    [decentdb_cli.dumpSql,
     cmdName = "dump",
     doc = "Dump database as SQL",
     help = {
       "db": "Path to database file (required)",
       "output": "Output SQL file path (optional, defaults to stdout)"
     },
     short = {
       "db": 'd',
       "output": 'o'
     }],
    [decentdb_cli.bulkLoadCsv,
     cmdName = "bulk-load",
     doc = "Bulk load data from CSV",
     help = {
       "table": "Table name to import into",
       "input": "CSV file path",
       "db": "Path to database file (required)",
       "batchSize": "Rows per batch (default: 10000)",
       "syncInterval": "Batches between fsync when durability is deferred (default: 10)",
       "durability": "Durability mode: full, deferred, none (default: deferred)",
       "disableIndexes": "Disable indexes during load (default: true)",
       "noCheckpoint": "Skip checkpoint after load completes"
     },
     short = {
       "db": 'd',
       "table": 't'
     }],
    [decentdb_cli.checkpointCmd,
     cmdName = "checkpoint",
     doc = "Force a WAL checkpoint",
     help = {
       "db": "Path to database file (required)",
       "warnings": "Include WAL warnings in output",
       "verbose": "Include verbose diagnostics in output"
     },
     short = {
       "db": 'd'
     }],
    [decentdb_cli.statsCmd,
     cmdName = "stats",
     doc = "Show basic engine statistics",
     help = {
       "db": "Path to database file (required)"
     },
     short = {
       "db": 'd'
     }],
    [decentdb_cli.vacuumCmd,
     cmdName = "vacuum",
     doc = "Rewrite database into a new file to reclaim space",
     help = {
       "db": "Path to database file (required)",
       "output": "Output database path (required)",
       "overwrite": "Overwrite output if it exists",
       "cachePages": "Number of 4KB pages to cache (default: 1024 = 4MB)",
       "cacheMb": "Cache size in megabytes (overrides --cachePages if specified)"
     },
     short = {
       "db": 'd'
     }],
    [decentdb_cli.infoCmd,
     cmdName = "info",
     doc = "Display database information",
     help = {
       "db": "Path to database file (required)",
       "schema-summary": "Include schema summary (tables, columns, indexes)"
     },
     short = {
       "db": 'd'
     }],
    [decentdb_cli.dumpHeader,
     cmdName = "dump-header",
     doc = "Dump raw database header fields",
     help = {
       "db": "Path to database file (required)"
     },
     short = {
       "db": 'd'
     }],
    [decentdb_cli.verifyHeader,
     cmdName = "verify-header",
     doc = "Verify database header checksum",
     help = {
       "db": "Path to database file (required)"
     },
     short = {
       "db": 'd'
     }],
    [decentdb_cli.repl,
     cmdName = "repl",
     doc = "Interactive REPL mode",
     help = {
       "db": "Path to database file (required)",
       "format": "Output format: json, csv, table (default: table)"
     },
     short = {
       "db": 'd'
     }],
    [decentdb_cli.completion,
     cmdName = "completion",
     doc = "Emit shell completion script",
     help = {
       "shell": "Shell type: bash or zsh (default: bash)"
     }]
  )
