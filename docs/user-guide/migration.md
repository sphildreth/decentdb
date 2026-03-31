# Upgrading Legacy Databases

As DecentDB evolves, the on-disk file format occasionally changes to support new features, optimizations, and durability enhancements. DecentDB prioritizes a lean, fast engine and therefore does not bundle legacy file parsers into the core database engine or the primary `decentdb` CLI.

If you attempt to open an older database file (for example, a database created by a version 1.x Nim-era release) with a modern version of DecentDB, you will receive an `UnsupportedFormatVersion` error.

To upgrade your data to the current format, you must use the official standalone migration tool: **`decentdb-migrate`**.

## Installing `decentdb-migrate`

The migration tool is shipped alongside the primary DecentDB releases. If you are building from source, you can build it from the repository root:

```bash
cargo build --release -p decentdb-migrate
```

The compiled executable will be located at `target/release/decentdb-migrate`.

## How to Migrate a Database

The migration tool operates safely by strictly reading from your old database and writing to a *new* file. It will never overwrite or destroy your original data in place.

### Basic Usage

To upgrade a database, run the tool providing the path to your old database (`--source`) and the path where you want the new database to be created (`--dest`).

```bash
decentdb-migrate --source /path/to/legacy_database.ddb --dest /path/to/new_database.ddb
```

### Example

Suppose you have an old version 3 database named `musicbrainz.ddb`. 

1. **Verify the mismatch (Optional):**
   If you try to run the standard CLI on it, you will see a message like this:
   ```bash
   decentdb info --db musicbrainz.ddb
   # Error: Database is in legacy format version 3. To upgrade it to the current format version 8, please use the standalone migration tool...
   ```

2. **Run the migration:**
   ```bash
   decentdb-migrate --source musicbrainz.ddb --dest musicbrainz_upgraded.ddb
   ```
   
   *Output:*
   ```text
   Migrating database from format version 3 to 8...
   Detected Nim-era Version 3 format.
   Extracting schema...
   Migrating table 'artists'... [10,000 rows]
   Migrating table 'albums'... [50,000 rows]
   Migration complete! Your upgraded database is ready at: musicbrainz_upgraded.ddb
   ```

3. **Verify and Replace:**
   You can now verify the new file using the standard CLI:
   ```bash
   decentdb info --db musicbrainz_upgraded.ddb
   ```
   Once you are satisfied that the data has migrated successfully, you can swap the files:
   ```bash
   mv musicbrainz_upgraded.ddb musicbrainz.ddb
   ```

## Supported Legacy Formats

Currently, `decentdb-migrate` is focused on providing an upgrade path from the widely-used **Version 3** (the final Nim-era format) to the current Rust-era format. 

If you encounter an even older format version that is unsupported, please open an issue on the DecentDB GitHub repository.
