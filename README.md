# BCPJSON

A fast, simple command-line tool for getting data **out of**, **into**, and **between** Microsoft SQL Server databases.

## Why

After reading about how Facebook backs up its MySQL fleet at scale — and, just as importantly, routinely *tests* those backups by restoring them — I wanted to see whether part of that model could be replicated for SQL Server. BCPJSON unloads every table into a structured, deterministic file format so that:

- exports are **diff-able**: two exports of the same table can be compared with ordinary line-based diff tools;
- exports can be **restored** into another database to prove the backup is good;
- the whole process stays **simple**: one executable, no agents, no SSIS packages.

## Features

| Verb | What it does |
|---|---|
| `export` | Writes each selected table to its own file, in **JSON** (via SqlClient) or native **BCP** character format (via the `bcp` utility). Can also export the result of a single `--source-query`. |
| `import` | Loads JSON or BCP files produced by `export` back into existing tables. |
| `copy` | Copies tables server-to-server with `SqlBulkCopy`, creating target tables if needed. |
| `verify` | Checks an export folder against its manifest (sizes and SHA-256 of every file); no database needed. |

- Tables are processed in parallel (`--max-tasks` limits how many at once).
- Select tables with include/exclude lists for tables and schemas, with `*` wildcards. With no include filters, every user table is selected.
- JSON exports are ordered by primary key (or by every sortable column if there is no PK), so repeated exports of unchanged data are identical.
- **Fast loads by default:** imports and copies take a table lock (`TABLOCK`), which allows minimal logging. Opt out with `--no-table-lock` — see [Table locking](#table-locking).
- **Point-in-time exports:** `--snapshot` reads every table from a database snapshot, so a long export or copy reflects one moment — see [Consistent point-in-time exports](#consistent-point-in-time-exports).
- **Resilient runs:** transient failures (network drops, failovers, throttling) are retried; export files only appear under their real names once complete; every export writes a manifest with each file's SHA-256, can be checked later with `verify`, and an interrupted export continues with `--resume` — see [Resilience](#resilience).
- **Very large tables:** `--split-tables` cuts chosen tables into key ranges that export, load and copy in parallel, and retry and resume part by part; `import --disable-indexes` loads first and rebuilds nonclustered indexes afterwards, and `import --load-as-heap` drops all indexes of a split table, loads it as a heap and recreates them — see [Splitting large tables](#splitting-large-tables).
- **Compression:** `--compress gzip` writes `.json.gz` / `.bcp.gz` files, several times smaller (3.2 times on the CI test data); import reads them directly — see [Compressed exports](#compressed-exports).
- **Streaming JSON import:** rows go from the file straight into the bulk load, so memory use stays flat however large the table.
- Windows Authentication by default; SQL logins supported.
- Exit code `0` on success, `1` if anything reported an error — suitable for scheduled jobs.

## Requirements

- Windows (x64 or Arm64) or Linux (x64). Release downloads are self-contained: no .NET install needed. Building from source needs the [.NET 8 SDK](https://dotnet.microsoft.com/download).
- For `--file-format bcp` and for `copy` of tables with geography/geometry/hierarchyid columns: the SQL Server **bcp utility** on `PATH` (installed with SQL Server or the *Microsoft Command Line Utilities for SQL Server*).
- SQL Server 2008 or later; every release is tested against SQL Server 2019, 2022 and 2025. `--snapshot` needs SQL Server 2016 SP1 or later (any edition) and is not available on Azure SQL Database.

## Install

Download the package for your platform from the [Releases page](https://github.com/chrisgelhaus/BCPJSON/releases):

| Platform | Package |
|---|---|
| Windows x64 | `bcpjson-<version>-win-x64.zip` |
| Windows Arm64 | `bcpjson-<version>-win-arm64.zip` |
| Linux x64 | `bcpjson-<version>-linux-x64.tar.gz` |
| Linux Arm64 | `bcpjson-<version>-linux-arm64.tar.gz` |
| macOS (Apple Silicon) | `bcpjson-<version>-osx-arm64.tar.gz` |

Each package holds a single self-contained executable (`bcpjson.exe` / `bcpjson`) plus `LICENSE`, `NOTICE` and this README. Unpack it anywhere and add the folder to `PATH`.

Every release is round-trip tested against SQL Server on Linux x64. The Arm64 and macOS builds are built and smoke-tested on their own platforms, but SQL Server containers run only on x64, so they are not tested against a database in CI.

**macOS:** the executable is not signed with an Apple Developer ID or notarized, so macOS blocks a copy downloaded with a browser ("cannot be opened because the developer cannot be verified"). After checking the download against `SHA256SUMS`, remove the quarantine flag: `xattr -d com.apple.quarantine bcpjson`. Files downloaded with `curl` are not quarantined. For BCP files and spatial-type copies, install `bcp` with Homebrew (`brew install mssql-tools18` from Microsoft's `microsoft/mssql-release` tap).

To check a download, compare it with `SHA256SUMS` from the same release, or verify where and how it was built (requires the [GitHub CLI](https://cli.github.com/)):

```powershell
Get-FileHash .\bcpjson-2.0.0-win-x64.zip -Algorithm SHA256        # compare with SHA256SUMS
gh attestation verify .\bcpjson-2.0.0-win-x64.zip --repo chrisgelhaus/BCPJSON
```

## Build from source

```powershell
git clone https://github.com/chrisgelhaus/BCPJSON.git
cd BCPJSON
dotnet build -c Release
dotnet test                                           # unit tests, no database needed
dotnet publish src/BcpJson -c Release -r win-x64 -o publish   # self-contained single-file publish\bcpjson.exe
```

Use `-r win-arm64`, `-r linux-x64`, `-r linux-arm64` or `-r osx-arm64` for the other platforms.

## Usage

```powershell
bcpjson --help
bcpjson export --help
```

### Export

```powershell
# Every user table in a database to JSON
bcpjson export --source-server localhost --source-database AdventureWorks `
               --file-format json --export-path C:\dumps\aw

# Only the Sales schema, excluding history tables, as BCP files
bcpjson export --source-database AdventureWorks --include-schemas Sales `
               --exclude-tables "Sales.*History" --file-format bcp --export-path C:\dumps\aw

# A query to a single file (C:\dumps\top_customers.json)
bcpjson export --source-database AdventureWorks --file-format json --export-path C:\dumps `
               --source-query "SELECT TOP (100) * FROM Sales.Customer ORDER BY CustomerID" `
               --output-name top_customers
```

| Option | Description |
|---|---|
| `--source-server` | Instance to read from (default `localhost`). |
| `--source-database` | Database to export. Required unless `--source-query` is used. |
| `--source-login`, `--source-password` | SQL login. Omit both for Windows Authentication. |
| `--file-format` | `json` or `bcp` (default `bcp`). |
| `--export-path` | Output folder (default: current directory; created if missing). |
| `--include-tables`, `--exclude-tables` | Comma-separated `schema.table` names; `*` is a wildcard. |
| `--include-schemas`, `--exclude-schemas` | Comma-separated schema names; `*` is a wildcard. |
| `--source-query`, `--output-name` | Export one query's first result set to `<output-name>.json`/`.bcp` (default name `query`). |
| `--nolock` | Read with `WITH (NOLOCK)` (JSON export). Faster, but can return dirty reads. |
| `--trust-server-certificate` | Accept the server's TLS certificate without validation (typical for local instances). |
| `--log-file` | Also append all console messages to this file. |
| `--max-tasks` | Maximum tables exported in parallel (default unlimited). |
| `--snapshot` | Export from a database snapshot so every table reflects the same moment (see [Consistent point-in-time exports](#consistent-point-in-time-exports)). |
| `--split-tables` | Split large tables into key ranges exported in parallel: `schema.table[:parts]`, `*` wildcard, 2 to 200 parts (default 8). See [Splitting large tables](#splitting-large-tables). |
| `--compress` | `none` (default) or `gzip`. See [Compressed exports](#compressed-exports). |
| `--compression-level` | gzip level: `fastest` (default), `optimal` or `smallest`. |
| `--retries`, `--resume` | See [Resilience](#resilience). |

**Output files**

- JSON: `<schema>.<table>.json` (`.json.gz` with `--compress gzip`); a split table gets `<schema>.<table>.part0001.json`, `.part0002.json`, and so on instead (see [Splitting large tables](#splitting-large-tables))
- BCP: `<schema>.<table>.bcp` (pipe-delimited character data; `.bcp.gz` with `--compress gzip`), `<schema>.<table>-c.fmt` (format file, needed for import; older exports' `-c.xml` files are still accepted), plus `_output.log` / `_error.log` from bcp. The parts of a split table are `<schema>.<table>.part0001.bcp` and so on, with one shared format file and one pair of logs per part.
- `bcpjson-manifest.json` (see [The export manifest](#the-export-manifest)).

### Import

The target table must already exist (create the schema first, e.g. from a script of the source database).

```powershell
# All JSON files in a folder; table name taken from each file name
bcpjson import --file-format json --source-path C:\dumps\aw --source-file "*.json" `
               --target-database AdventureWorks_Restore --truncate-target

# One BCP file into an explicitly named table
bcpjson import --file-format bcp --source-path C:\dumps\aw --source-file Sales.Customer.bcp `
               --target-database Scratch --target-table dbo.CustomerCopy

# A compressed export, each file in a single transaction
bcpjson import --file-format json --source-path D:\dumps\aw --source-file "*.json.gz" `
               --target-database AdventureWorks_Restore --batch-size 0
```

| Option | Description |
|---|---|
| `--file-format` | `json` or `bcp` (required). |
| `--source-path` | Folder containing the files (default: current directory). |
| `--source-file` | File name or wildcard pattern (required). |
| `--target-server`, `--target-database` | Where to load. |
| `--target-login`, `--target-password` | SQL login. Omit both for Windows Authentication. |
| `--target-table` | Load into this table instead of the one named by the file. |
| `--truncate-target` | Truncate the table before loading. |
| `--max-tasks` | Maximum files imported in parallel (default unlimited). |
| `--no-table-lock` | Load without a table lock (default: locked). See [Table locking](#table-locking). |
| `--disable-indexes` | Disable the target's nonclustered indexes during the load and rebuild them afterwards. See [Splitting large tables](#splitting-large-tables). |
| `--load-as-heap` | For split tables loaded into a table with a clustered index: drop its indexes, load the parts in parallel into the heap, then recreate the indexes. Other tables load as with `--disable-indexes`. See [Splitting large tables](#splitting-large-tables). |
| `--batch-size` | Rows per committed batch (default `50000`). `0` loads each file in one transaction (each part file of a split table separately). |
| `--trust-server-certificate`, `--log-file`, `--retries` | As for export. |

Identity values and NULLs are preserved. Computed and `rowversion` columns are skipped and recalculated by the server.

Compressed files (`.json.gz`, `.bcp.gz`) are recognised by their extension; the table name comes from the name without `.gz`. Use a pattern that matches them, such as `*.json.gz`. JSON files, compressed or not, are streamed: each row is parsed and handed to the bulk load as it is read, so memory use does not grow with the table. BCP files are decompressed to the temp folder first, because `bcp` reads only plain files, so that needs free space for the largest uncompressed file.

**Batch size.** Each batch is committed on its own. If a load fails, the batches already committed stay in the table, so rerun with `--truncate-target`. Larger batches load a little faster and commit less often. Use `--batch-size 0` to get all or nothing per file, at the cost of one long transaction, which needs log space for the whole file unless the load is minimally logged.

### Copy

```powershell
bcpjson copy --source-server PROD01 --source-database Sales `
             --target-server DEV01  --target-database Sales_Copy `
             --include-schemas dbo --drop-target --max-tasks 4
```

| Option | Description |
|---|---|
| `--source-server`, `--source-database`, `--source-login`, `--source-password` | Where to read from (as for export). |
| `--target-server`, `--target-database`, `--target-login`, `--target-password` | Where to write. The target database must exist. |
| `--include-tables`, `--exclude-tables`, `--include-schemas`, `--exclude-schemas` | Table selection, as for export. |
| `--drop-target` | Drop and recreate target tables that already exist. |
| `--truncate-target` | Empty existing target tables before copying. |
| `--max-tasks` | Maximum tables copied in parallel (default unlimited). |
| `--nolock` | Read the source with `WITH (NOLOCK)`. |
| `--no-table-lock` | Load without a table lock (default: locked). See [Table locking](#table-locking). |
| `--snapshot` | Copy from a database snapshot of the source. See [Consistent point-in-time exports](#consistent-point-in-time-exports). |
| `--split-tables` | Copy large tables as parallel key-range streams: `schema.table[:parts]`, `*` wildcard, 2 to 200 parts (default 8). See [Splitting large tables](#splitting-large-tables). |
| `--no-indexes` | Don't recreate primary keys, unique constraints and indexes on the tables copy creates (default: recreated). See [Keys and indexes](#keys-and-indexes). |
| `--foreign-keys` | Also recreate foreign keys (default: off). See [Foreign keys](#foreign-keys). |
| `--no-check-constraints` | Don't recreate check constraints (default: recreated). See [Check constraints](#check-constraints). |
| `--triggers` | Also recreate triggers (default: off). See [Triggers](#triggers). |
| `--trust-server-certificate`, `--log-file` | As for export. |

Missing target tables are created from the source definition: columns, nullability, identity, defaults and computed columns, then — after the data is loaded — the primary key, unique constraints and indexes. Tables with geography, geometry or hierarchyid columns are moved with `bcp` through a temporary folder.

#### Keys and indexes

**Default:** on. **Option:** `--no-indexes` turns it off.

When `copy` creates a target table (it did not exist, or `--drop-target` dropped it), it recreates every primary key, unique constraint and index of the source table: clustered and nonclustered, unique and filtered indexes, included columns, descending keys, data compression, columnstore, XML and spatial indexes. They are built **after the data is loaded**, because bulk loading into a table without indexes (with the default table lock) is the fastest way to load, and building each index once at the end is much cheaper than maintaining it row by row. The clustered index is built first, then the others in dependency order.

Indexes are created on the target database's default filegroup: filegroup placement is not copied, because the target may not have the same filegroups. A partitioned source table is the exception: when the target database already has a partition scheme with the same name, the table (and its indexes) is created on it; otherwise it is created unpartitioned. That scheme's partition function must take the type of the table's partitioning column, or creating the table fails. Index and constraint names are kept, except for constraints that were declared without a name (such as `PRIMARY KEY` on a column): SQL Server named those itself (`PK__Orders__3214EC07…`), and the copy gets its own system-generated name, just as running the original `CREATE TABLE` would. An index that cannot be created is reported as an error (exit code 1) and the remaining indexes are still created.

A target table that **already exists** keeps its own indexes; `copy` does not add, change or remove them.

Use `--no-indexes` when:

- the target is a staging or scratch database and you will create your own indexes (or none);
- you want the copy to finish as quickly as possible and do not need to query the result efficiently;
- the target has little free space: building large indexes needs temporary space in the data files and `tempdb`.

Check constraints are recreated too ([Check constraints](#check-constraints)); foreign keys and triggers are copied on request ([Foreign keys](#foreign-keys), [Triggers](#triggers)).

#### Foreign keys

**Default:** off. **Option:** `--foreign-keys` turns it on.

With `--foreign-keys`, after **every** table has been copied and indexed, `copy` recreates the foreign keys of the tables it created: multi-column keys, keys that reference a unique constraint rather than the primary key, self-references and cross-schema references, and their `ON DELETE` / `ON UPDATE` actions. Keys that are trusted in the source are created `WITH CHECK`, so SQL Server validates the copied rows and the key is trusted in the target too; keys that are disabled or not trusted in the source are created the same way in the target. They are created one at a time, and one that fails is reported as an error (exit code 1) while the others are still created.

```powershell
# Consistent copy of a whole database, including its relationships
bcpjson copy --source-server PROD01 --source-database Sales `
             --target-server TEST01 --target-database Sales_Test --drop-target --snapshot --foreign-keys
```

Why it is off by default — a foreign key involves two tables, so it fails in situations that do not affect a single table:

- **Partial copies.** With `--include-*` / `--exclude-*` filters, the table a key references may not be part of the copy. The key can only be created if that table already exists in the target with the referenced key.
- **Copies of a changing database.** Without `--snapshot`, a child row can be copied without the parent row it points to, and the `WITH CHECK` validation fails. Use `--snapshot` with `--foreign-keys` (bcpJson warns if you don't).
- **Validation time.** Checking a large table against its parent takes time; skip foreign keys for scratch copies where you don't need them.

`--foreign-keys` cannot be combined with `--no-indexes`, because foreign keys need the referenced primary keys and unique constraints.

**Re-running a copy:** with `--drop-target`, `copy` first removes the foreign keys defined on the tables it is about to drop and recreate (they would be dropped with their tables anyway), so a database previously copied with `--foreign-keys` can be refreshed. A foreign key on a table *outside* the copy that references a copied table is left alone; if it prevents a table from being dropped, that table is reported as an error.

#### Check constraints

**Default:** on. **Option:** `--no-check-constraints` turns it off.

On every table it creates, `copy` recreates the source's check constraints right after the table's indexes. A check constraint only involves its own table, and the copied rows already satisfied it in the source, so trusted constraints are created `WITH CHECK` (validated and trusted in the target); disabled or untrusted ones keep that state. Names are kept, except that constraints declared without a name get a new system name. A constraint that cannot be created — typically one that calls a user-defined function that does not exist in the target — is reported as an error and the others are still created.

Use `--no-check-constraints` when the target is a staging area that must accept rows the source would reject, or to save the validation scan on very large tables.

#### Triggers

**Default:** off. **Option:** `--triggers` turns it on.

With `--triggers`, `copy` recreates the DML triggers (`AFTER` and `INSTEAD OF`) of the tables it created, **after all tables are loaded**, so they never fire on the copied rows. Each trigger's body is copied as is (the `CREATE TRIGGER ... ON ...` header is regenerated with bracketed names, which also keeps it correct for triggers renamed with `sp_rename`); disabled triggers stay disabled and the `FIRST` / `LAST` firing order set with `sp_settriggerorder` is kept.

```powershell
# Full copy of the schema objects bcpJson handles: keys, indexes, check constraints, foreign keys and triggers
bcpjson copy --source-database Sales --target-database Sales_Test --drop-target --snapshot --foreign-keys --triggers
```

Why it is off by default:

- **Triggers run on every later write.** In a copy, an audit trigger keeps writing audit rows, and a trigger that sends mail, calls a procedure or writes to another database keeps doing so — often not wanted in a test or reporting copy.
- **Triggers reference other objects.** Procedures, functions, other tables or databases used by the trigger must exist in the target, or creating (or later firing) the trigger fails.
- **Encrypted triggers** (`WITH ENCRYPTION`) cannot be scripted and are reported as errors; CLR triggers need their assembly in the target.

Each trigger that cannot be created is reported as an error and the others are still created. Database-level (DDL) triggers are not copied.

## Table locking

**Applies to:** `import` (JSON and BCP) and `copy`. **Default:** on. **Option:** `--no-table-lock` turns it off.

### How table locking works

Every load asks SQL Server for a table lock (`TABLOCK`) on the target table: `SqlBulkCopyOptions.TableLock` for JSON import and copy, `bcp ... -h "TABLOCK"` for BCP import. Taking one table-level lock instead of thousands of row and page locks is the fastest way to bulk load:

- far less lock management and no lock escalation part-way through the load;
- under the **SIMPLE** or **BULK_LOGGED** recovery model the load can be **minimally logged** (for example into a heap or an empty table), which cuts transaction-log writes dramatically. Under FULL recovery every row is still logged, but the locking savings remain.

The lock is held while each batch is written (`--batch-size`, 50,000 rows by default, for import; 10,000 for copy), so for practical purposes other sessions cannot use the table until the load finishes. On a heap the lock is a bulk-update (BU) lock; on a table with a clustered index it is an exclusive table lock.

### Table locking examples

```powershell
# Default: restore a whole export into empty tables as fast as possible (table locks on)
bcpjson import --file-format json --source-path D:\dumps\sales --source-file "*.json" `
               --target-database Sales_Restore --truncate-target

# Append a feed into a table that applications are reading and writing right now
bcpjson import --file-format bcp --source-path D:\feeds --source-file Sales.Orders.bcp `
               --target-database Sales --no-table-lock

# Several files into one table with a clustered index, loading in parallel
bcpjson import --file-format json --source-path D:\feeds --source-file "orders_*.json" `
               --target-database Sales --target-table dbo.Orders --no-table-lock

# Refresh a reporting database that stays online during the copy
bcpjson copy --source-server PROD01 --source-database Sales `
             --target-server RPT01 --target-database Sales_Reporting --truncate-target --no-table-lock
```

### When to keep the default (table lock on)

- Restoring an export into a new or scratch database, or into tables you have just truncated.
- Nightly or batch loads into staging tables that nothing else reads during the load.
- Any load where speed and transaction-log volume matter more than concurrent access.

### When to use `--no-table-lock`

- **The target table is live.** Readers block until each batch commits (unless they use snapshot isolation, RCSI or `NOLOCK`), and writers block too. Use row locking when an application must keep working with the table during the load.
- **Several files load into the same table with a clustered index** (as in the third example). With table locks those loads run one at a time; without them they can run in parallel. Heaps are the exception: BU locks are compatible with each other, so parallel table-locked loads into a heap work well and you can keep the default. (For the parts of a split table, bcpJson makes this choice itself: see [Splitting large tables](#splitting-large-tables).)
- **Lock-sensitive environments**, for example a busy server where an exclusive table lock would trip blocking alerts.

Without the table lock the load is fully logged, uses row/page locks (which SQL Server may still escalate to a table lock on very large loads), and is noticeably slower on big tables.

## Consistent point-in-time exports

**Applies to:** `export` (JSON, BCP and `--source-query`) and `copy`. **Default:** off. **Option:** `--snapshot` (requires `--source-database`, the database to snapshot).

### The problem: drift during a long run

Tables are read one after another (or a few at a time with `--max-tasks`). On a busy database a long export is therefore not a single point in time: a table read at minute 40 contains changes that happened after the tables read at minute 1. Typical symptoms when such an export is restored or compared:

- child rows (order lines, payments) whose parent rows are missing, or the reverse;
- totals that do not reconcile across tables;
- day-to-day diffs that show changes in one table but not in the related table that changed in the same transaction.

### How `--snapshot` works

With `--snapshot`, bcpJson:

1. creates a [database snapshot](https://learn.microsoft.com/sql/relational-databases/databases/database-snapshots-sql-server) of the source database, named `<database>_bcpjson_<UTC timestamp>`, with its sparse files next to the source data files;
2. reads every table (and `--source-query`, which runs in the snapshot database) from that snapshot — in parallel, in JSON or BCP, or through `copy`;
3. drops the snapshot at the end, including when the export fails.

The console reports when the snapshot was created; every file reflects that moment. Creating the snapshot is typically near-instant regardless of database size (its files start empty), and the source stays fully online for reads and writes throughout.

### Snapshot examples

```powershell
# Nightly point-in-time JSON export of a busy OLTP database
bcpjson export --source-server PROD01 --source-database Sales --file-format json `
               --export-path D:\dumps\sales\2026-11-20 --snapshot

# Compare two nightly exports: only the rows that changed show up
git diff --no-index --stat D:\dumps\sales\2026-11-19 D:\dumps\sales\2026-11-20

# Consistent BCP export of one schema, for a fast reload elsewhere
bcpjson export --source-server PROD01 --source-database Sales --include-schemas Billing `
               --file-format bcp --export-path D:\dumps\billing --snapshot

# Refresh a test database with a consistent copy of production
bcpjson copy --source-server PROD01 --source-database Sales `
             --target-server TEST01 --target-database Sales_Test --drop-target --snapshot
```

### When to use `--snapshot`

- **Backups you intend to restore or verify**: the restored database is transactionally consistent.
- **Day-to-day comparisons** of exports, so a diff shows real changes rather than timing artefacts.
- **Long exports or copies of a database that is being written to** while they run.
- **Databases with foreign keys between tables**, where a restore must not contain orphaned rows.

### When not to use `--snapshot`

- **The source is idle** during the export (a maintenance window, a restored copy, a read-only database): there is nothing to drift, so the snapshot adds no value.
- **No permission**: creating a snapshot needs `CREATE DATABASE` permission on the server (for example the `dbcreator` role). Without it the run stops with an error rather than silently exporting without a snapshot.
- **Azure SQL Database** (database snapshots are not supported) or **SQL Server older than 2016 SP1** outside Enterprise/Developer edition. Alternative: export from a database copy or a point-in-time restore instead.
- **Databases with memory-optimized (In-Memory OLTP) filegroups**, which cannot be snapshotted.
- **A very write-heavy source with little free disk space on the data volume**: while the snapshot exists, the first change to each source page copies the original page into the snapshot's sparse file. The cost grows with the amount of data changed during the run, not with database size.

### Snapshot notes

- **How it is tested.** On every change, CI runs a writer that keeps moving value between two 300,000-row tables in single transactions, so their combined total is constant at every committed moment. Two `export --snapshot` runs made while transfers are committing must each add up to exactly that total, and the same export *without* `--snapshot` must be caught drifting — proving that the check can tell the difference.
- `--nolock` is unnecessary with `--snapshot` (a snapshot is read-only, so reads cannot block writers). Without `--snapshot`, `--nolock` is the opposite trade-off: it avoids blocking but can read uncommitted, inconsistent data — not something to rely on for backups.
- `--snapshot` only affects the source. For `copy`, the target is written normally (and table-locked unless `--no-table-lock`).
- If a run is killed before it can clean up, a snapshot database is left behind. Find and drop it:

  ```sql
  SELECT name, create_date FROM sys.databases WHERE source_database_id IS NOT NULL AND name LIKE N'%[_]bcpjson[_]%';
  DROP DATABASE [Sales_bcpjson_20261120214450];
  ```

## Resilience

Long runs against busy or remote servers hit network drops, failovers and throttling. bcpJson is built to survive them, to never leave a half-written file that looks complete, and to let you check and finish an export later.

### Retries for transient failures

`--retries` (default `3`, `0` turns it off) applies to every verb:

- **Opening a connection** is retried when SQL Server reports a transient error: network errors, deadlock victims, Azure SQL throttling and failover, or "cannot open database" while a database is coming online. The waits grow exponentially (about 1, 2, 4, 8 s, at most 30 s), and each retry is logged as a warning.
- **Exporting a table** is retried as a whole when it fails with a transient error (JSON) or bcp fails (BCP; bcp reports no error numbers, so any failure is retried). Exporting only rewrites the table's file, so starting over is safe. Waits: 2, 4, 8, 16, then 30 s.
- **Not retried:** `--source-query` exports (the query may be a procedure with side effects), and the data loads of `import` and `copy`. A load may have committed some batches before failing, and replaying it could duplicate rows. Rerun those with `--truncate-target` or `--drop-target` instead.

```powershell
# A flaky VPN link: allow more retries
bcpjson export --source-server remote01 --source-database Sales --file-format json --export-path D:\dumps\sales --retries 8
```

### Complete files only

Every export file is first written as `<name>.partial` and renamed to its real name only when it is complete (for BCP, the data and format files together). A crash, a killed process or a failed table never leaves a truncated `.json` or `.bcp` file behind. If an export fails, a file from an earlier export keeps its previous content. `import` ignores `.partial` files, and the next export overwrites them.

### The export manifest

Each export writes `bcpjson-manifest.json` into the export folder. It records the tool version, format, server, database (the real database, also with `--snapshot`), start and completion times, and for every table its row count and the size and SHA-256 of each file:

```json
{
  "tool": "bcpjson 2.4.0+...",
  "format": "json",
  "server": "PROD01",
  "database": "Sales",
  "query": null,
  "snapshot": true,
  "started": "2026-11-21T02:00:00Z",
  "completed": "2026-11-21T02:41:13Z",
  "tables": [
    {
      "name": "Sales.Customer",
      "rows": 19820,
      "exported": "2026-11-21T02:00:04Z",
      "files": [ { "name": "Sales.Customer.json", "bytes": 4120334, "sha256": "9f2c..." } ],
      "orderedBy": [ { "column": "CustomerID", "type": "int", "descending": false, "collation": null } ]
    }
  ]
}
```

The manifest is saved after every table, so it always reflects what has finished. `completed` is set only when the whole export succeeded. `import` does not load the manifest as data, so `--source-file "*.json"` still works.

`orderedBy` (JSON exports of tables with a primary key) is the key the rows are sorted by. JSON import reads it: when the target table's clustered index has exactly that key (the same columns, types, directions and collations), it tells SQL Server the rows already arrive in clustered index order, so the load needs no sort. That makes large loads into an empty clustered table noticeably faster. SQL Server checks the order and would fail the load if it were wrong, so for any other target, including a JSON file without a manifest, the import runs as before. The console says `Rows are in clustered index order; loading with an order hint.` when it applies.

### Verifying an export

`verify` checks an export folder against its manifest without touching a database. Every file must exist with its recorded size and SHA-256, and the export must have completed:

```powershell
bcpjson verify --path D:\dumps\sales
```

It exits `1` and names each problem: a missing file, a changed size or hash (corruption, an accidental edit, an incomplete copy to another disk), or an export that never completed. Extra `.json`/`.bcp` files the manifest doesn't list, for example tables left from an older export, and leftover `.partial` files are reported as warnings. Run it after copying an export to archive storage, and before restoring from it.

### Resuming an interrupted export

If an export stops partway (a crash, a reboot, a table that kept failing), rerun the same command with `--resume`:

```powershell
bcpjson export --source-database Sales --file-format json --export-path D:\dumps\sales --resume
```

Tables that the manifest records, and whose files still have the recorded size, are skipped; everything else is exported. The run must use the same `--file-format`, `--compress`, `--source-database` and `--source-query` as the manifest, otherwise it stops with an error. With `--snapshot`, each run takes its own snapshot, so tables from the earlier run reflect an earlier point in time (a warning says so). Use a fresh export when one point in time matters more than saving the work already done.

A split table resumes part by part: parts the manifest records, with their files intact, are kept and only the missing or damaged ones are exported (see [Splitting large tables](#splitting-large-tables)).

## Compressed exports

**Applies to:** `export` (JSON and BCP); `import` reads the results. **Default:** off. **Options:** `--compress gzip`, `--compression-level fastest|optimal|smallest`.

```powershell
# Nightly JSON backup, compressed
bcpjson export --source-database Sales --file-format json --export-path D:\dumps\sales --compress gzip

# Smallest files for archive storage (slower to write)
bcpjson export --source-database Sales --file-format bcp --export-path D:\archive\sales --compress gzip --compression-level smallest
```

- **JSON** is compressed while it is written, so no uncompressed copy ever reaches the disk.
- **BCP** data is compressed after `bcp` has written it (bcp can only write plain files), so the export briefly needs room for one table's uncompressed file. Format files stay uncompressed.
- The manifest records `"compression": "gzip"` and the size and SHA-256 of the `.gz` files, so `verify` works the same. `--resume` refuses to continue an export with a different `--compress` setting.
- `gzip` was chosen because every platform can read it without bcpJson: `zcat`, `gzip -d`, 7-Zip, `tar`. To diff two compressed exports, decompress them first, for example `diff <(zcat a/dbo.T.json.gz) <(zcat b/dbo.T.json.gz)`.

**Which level?** `fastest` (the default) gets most of the reduction and is fast enough not to slow down an export. How much a file shrinks depends on the data: repetitive JSON (the same property names on every row, many similar values) compresses best. The CI test data, which is deliberately varied, shrinks 3.2 times. `optimal` saves a little more space for noticeably more CPU, and `smallest` more again; use them for archives written once and kept long.

**Why compression is off by default.** Plain files can be diffed line by line, opened in any editor and read by any tool. That was the point of bcpJson's format, and existing scripts expect `.json` and `.bcp` names. Turn it on when disk space or copy time matters more.

## Splitting large tables

**Applies to:** `export` (JSON and BCP) and `copy` (`--split-tables`); `import` loads the parts and adds `--disable-indexes` and `--load-as-heap`. **Default:** off, every table is one file. **Options:** `--split-tables pattern[:parts]`, `--disable-indexes`, `--load-as-heap`.

One table is normally one unit of work: a single task reads it, writes its file and loads it, so a table of hundreds of millions of rows sets the length of the whole run, and a transient error redoes it from the start. `--split-tables` cuts the tables you name into **key ranges**, each handled as its own task: parts are exported, loaded and copied in parallel, and retried and resumed one by one. Tables you do not name are exported exactly as before.

```powershell
# A history table with an identity key, cut into 16 parts and exported 8 at a time
bcpjson export --source-server PROD01 --source-database Sales --file-format json --export-path D:\dumps\sales `
               --include-tables dbo.OrderHistory --split-tables dbo.OrderHistory:16 --snapshot --max-tasks 8

# Restore it: load the parts in parallel, with the nonclustered indexes rebuilt once at the end
bcpjson import --file-format json --source-path D:\dumps\sales --source-file "*.json" `
               --target-database Sales_Restore --truncate-target --disable-indexes --max-tasks 8

# The same idea server to server: 16 parallel streams, no files in between
bcpjson copy --source-server PROD01 --source-database Sales --target-server TEST01 --target-database Sales_Test `
             --include-tables dbo.OrderHistory --split-tables dbo.OrderHistory:16 --drop-target --snapshot --max-tasks 16
```

### Choosing tables and parts

`--split-tables` takes comma-separated entries of the form `schema.table[:parts]`. `*` is a wildcard (`dbo.*History:16`), the number of parts is 2 to 200 and defaults to 8, and the first entry that matches a table wins. An entry that ends in `:` or asks for a part count outside 2 to 200 is rejected. On `export` it cannot be combined with `--source-query`. `--max-tasks` limits how many parts (and unsplit tables) run at once; the largest are started first.

Splitting pays off for tables of tens of millions of rows or more, on a server that can feed several readers at once. For small tables it only adds files.

**Use `--snapshot` with `--split-tables`.** Without it the parts of one table are read at different moments, so rows that change during the run can be missed or read twice, and bcpJson warns about it. With `--snapshot` every part reads the same moment.

### How the ranges are chosen

- **Partitioned tables** are cut at their partition boundaries: neighbouring partitions are grouped so each part holds about the same number of rows (exact row counts, read from the server). A single partition is never cut further, so a table with fewer partitions than parts you asked for gets fewer parts.
- **Other tables** are cut on the first column of the clustered rowstore index, using the statistics histogram of that index. For number and date/time keys the boundaries are interpolated inside the histogram steps, so a key that grows evenly (an identity column, which the histogram describes with only a handful of steps) still gets the number of parts you asked for. Text, `uniqueidentifier` and binary keys use the histogram's own step values, so they may get fewer parts than requested.

The ranges together cover the table exactly once, including NULL keys. Out-of-date statistics make the parts uneven in size, never wrong; run `UPDATE STATISTICS` on a table whose parts come out very uneven.

### Tables that are not split

A table that cannot be split is exported whole, with a warning of the form `<table>: not split: <reason>.`, and the run carries on. Reasons: a heap or clustered columnstore table that is not partitioned; a split column of a type that cannot be cut (or a `varchar` key whose code page differs from the database's); no statistics histogram (run `UPDATE STATISTICS`); too few distinct key values; a NULL partition boundary; a split column whose name contains `"`, `\` or control characters; a table with a sparse column set; a graph table; or any error while planning. A key where one value holds most of the rows cannot be cut inside that value, so such a table gets uneven parts. For `copy`, tables with geography, geometry or hierarchyid columns (copied through bcp) are never split.

### Part files and the manifest

A split table writes `<schema>.<table>.part0001.json` (`.json.gz` with `--compress gzip`), `.part0002.json`, and so on, each holding one key range. When the split column leads the primary key, the parts in order are exactly the single file's rows; otherwise (a partitioned table split on its partitioning column but sorted by another key) they hold the same rows in a different order. BCP parts are `<schema>.<table>.part0001.bcp` and so on; they share one `<schema>.<table>-c.fmt` and each part has its own `_error.log` and `_output.log`.

In `bcpjson-manifest.json` the table stays one entry that lists all its part files, plus a `split` object recording how it was cut:

```json
{
  "name": "dbo.OrderHistory",
  "rows": 120000000,
  "files": [ { "name": "dbo.OrderHistory.part0001.json", "bytes": 912345678, "sha256": "4be1..." }, "... one per part ..." ],
  "split": {
    "kind": "histogram",
    "column": "OrderHistoryID",
    "type": "bigint",
    "descending": false,
    "nullable": false,
    "partCount": 16,
    "boundaries": [ "7500000", "15000000", "... 15 values in all ..." ],
    "parts": [
      { "part": 1, "rows": 7500000, "files": [ "dbo.OrderHistory.part0001.json" ],
        "lower": null, "lowerInclusive": false, "upper": "7500000", "upperInclusive": false, "includeNulls": false },
      "... one per part ..."
    ]
  }
}
```

`kind` is `histogram` or `partition` (which also records `partitionFunction` and `rangeRight`). Boundaries are stored as the text SQL Server converts the key to. `verify` checks every part file like any other file and reports `<table>: <k> of <n> parts exported.` when a split table is missing parts.

Export never deletes files. Re-exporting into a folder that holds an earlier export can leave data files behind (fewer parts than before, other boundaries, or a table that was split and now is not, or the other way round); export names each one: `<table>: <file> is left from an earlier export; delete it (import ignores it while the manifest is present).` When the source folder has a `bcpjson-manifest.json`, import loads only the files the manifest lists for a table it knows (`<table>: skipping <file>: not part of the export in <manifest>.`) and refuses a split table whose export is unfinished (`<table>: the export is incomplete (<k> of <n> parts); finish it with export --resume.`). Files of tables the manifest does not mention are loaded as before. With or without a manifest, a table that has both a whole-table file and part files is refused, and nothing is loaded into it: `<table>: both <file> and part files are present; remove the files of the older export.`

A `--split-tables` entry that matches none of the exported or copied tables is reported (`--split-tables <pattern>: no table matched.`), so a typo does not go unnoticed.

### Resuming and retrying

Each part is exported, retried and recorded on its own. After an interruption, `export --resume` keeps the recorded split (it says `<table>: --resume keeps its recorded split (<n> parts).` when your `--split-tables` differs or is missing, because new boundaries would not match the parts already written), skips the parts whose files are intact and exports only the rest (`<table>: <k> part(s) to go.`). A transient error therefore redoes one part, not the table.

### Importing parts and how they load

`import` groups the files by table (`--source-file "*.json"` picks up every part), truncates the table once (with `--truncate-target`) before any part loads, and then loads the parts into it. Whether they can load **in parallel** depends on what the target looks like, because SQL Server only lets several table-locked bulk loads run side by side in some cases. bcpJson looks at the target and reports its choice per table, as `<table>: <n> parts, loading in parallel.` or `<table>: <n> parts, loading one after another (<reason>).`

| Target table | With the table lock (default) | With `--no-table-lock` |
|---|---|---|
| Heap, or a table with no enabled index | Parallel (bulk-update locks do not conflict) | Parallel |
| Clustered rowstore index | One after another (`clustered index`; the table lock is exclusive) | Parallel |
| Heap with enabled nonclustered indexes | One after another (`enabled nonclustered indexes`) | Parallel |
| Clustered columnstore index | One after another (`columnstore`; measured in CI: a columnstore load takes S and X locks, not bulk-update) | Parallel |
| Table with geography/geometry/hierarchyid columns (JSON) | One after another (`CLR columns`; loaded through a staging table that always takes a table lock) | One after another |

When parts load one after another they go in part (key) order; a clustered table therefore receives sorted rows when the split column is the leading column of its clustered key. For a partitioned target with `--no-table-lock`, bcpJson suggests `ALTER TABLE ... SET (LOCK_ESCALATION = AUTO)`, which lets parts in different partitions load without blocking each other.

### Load first, index afterwards: `--disable-indexes`

**Applies to:** `import` (JSON and BCP), all tables of the run. **Default:** off.

The tables that matter most for a restore are those with many nonclustered indexes, and every index slows every row of the load. `--disable-indexes` is the "load first, index afterwards" restore: before a table's first part it disables the table's nonclustered indexes; after the last part it rebuilds them, once each. The gain is that the load maintains fewer indexes. Whether the parts then load in parallel still follows the [load-mode table](#importing-parts-and-how-they-load): a heap whose indexes are all disabled loads them in parallel, while a table with a clustered primary key (which is never disabled) still loads one part after another.

- **Not disabled:** the primary key, the clustered index, and any index a foreign key references (disabling those would block the load or the keys). Unique indexes are disabled and checked again on rebuild, so duplicate rows fail the rebuild, not the load.
- **Always rebuilt:** the rebuild runs even when the load failed part way, so the table is not left with disabled indexes. An index that cannot be rebuilt is reported as an error by name together with the `ALTER INDEX ... REBUILD` command to run by hand; the others are still rebuilt.
- **Memory-optimized tables** are skipped (`<table>: memory-optimized; --disable-indexes ignored.`).
- Two groups of files that resolve to the same target table (for example `T.json` and `dbo.T.json`) are refused.

On the CI benchmark (1,000,000 rows into an indexed table on a GitHub runner, so indicative only), loading with `--disable-indexes` took 4.8 s instead of 7.6 s with the indexes in place; loading the same data into a bare heap took 2.9 s. Splitting a 1,000,000-row export into 8 parts took 2.1 s instead of 2.5 s for JSON and 1.0 s instead of 1.3 s for BCP; the gains grow with the table, the server and the number of readers it can feed. A benchmark workflow (`.github/workflows/benchmark.yml`, run manually) reproduces these numbers on any row count.

### Load as a heap: `--load-as-heap`

**Applies to:** `import` (JSON and BCP), split tables only. **Default:** off.

A table with a clustered index takes its parts one after another, even with `--disable-indexes`. `--load-as-heap` drops **every** index of such a table (the clustered one and the nonclustered ones, primary key and unique constraints included), loads all parts in parallel into the resulting heap, then creates the clustered index and the others again, with the same names, columns, uniqueness, filters, compression, fill factor and filegroup or partition scheme (they are scripted with SMO from the target before anything is dropped). Use it to restore a big table into an empty target that already has its keys and indexes.

```powershell
bcpjson import --file-format json --source-path D:\dumps\sales --source-file "*.json" `
               --target-database Sales_Restore --load-as-heap --max-tasks 8
```

The option applies to a table only when the import has two or more files for it (part files). Every other table of the run, and every table that falls back as described below, loads as with `--disable-indexes`. A target that is already a heap needs no change and gets no message. For a heap load, bcpJson logs `<table>: <k> index(es) dropped; loading <n> parts in parallel as a heap.` before the parts load and `<table>: clustered index <name> and <k> other index(es) recreated.` afterwards. The indexes are recreated even when the load failed. The target must be empty: a table with rows falls back (with `--truncate-target` it is emptied first), and rows that arrive between planning and the drop fail the table with `<table>: rows were added after planning; not loaded.`

**When a table falls back.** The warning is `<table>: not loaded as a heap: <reason>; disabling indexes instead.` The first reason that applies is reported, in this order:

| Reason | Why |
|---|---|
| `--no-table-lock` | The heap load relies on the table lock. |
| `CLR columns` | JSON import of geography/geometry/hierarchyid goes through a staging table. |
| `memory-optimized` | Not supported. |
| `clustered columnstore` | Loaded one after another for a different reason; rebuilding one is a different trade-off. |
| `referenced by foreign key <name>` | Any foreign key, including the table's own, that references the table: its keys cannot be dropped. |
| `XML or spatial index <name>` | These need a clustered primary key and cannot exist on a heap. |
| `disabled index <name>` | Recreating it would enable it. |
| `indexed view <name>` | An indexed view references the table. |
| `temporal table` | System-versioned, or a history table. |
| `change tracking`, `replication`, `change data capture` | Enabled on the table. |
| `FILESTREAM` | The table has FILESTREAM data. |
| `full-text index` | The table has one. |
| `not empty` | The target has rows and `--truncate-target` is not set. |
| `index definition too long: <name>` | Each index statement is also saved in a marker (see below) and must fit in 3,500 characters. |
| `cannot write <path>: <message>` | The recovery script could not be written to `--source-path`. |

**If the run is interrupted, or an index cannot be recreated.** Before it drops anything, bcpJson takes two safeguards: it writes `<schema>.<table>.load-as-heap.sql` into `--source-path` (at planning), and it adds one extended property to the table per index, `bcpjson_load_as_heap_001`, `_002` and so on in creation order, each holding that index's statement. After the last part it creates the clustered index first (if that fails it stops, because nonclustered indexes built on a heap would be rebuilt again when the clustered index comes back), then each missing nonclustered index. A failure is an error, `<table>: Could not recreate <name>: <reason>. Run the script in <path>.`, and the other indexes are still created. The usual reason is duplicate keys in the loaded data: the error names the key or unique index. The markers and the script are removed only when every index exists again. The script file is never read as data (`--source-file "*"` skips it).

- **Run the recovery script.** It switches to the target database and sets the session options index statements need (`QUOTED_IDENTIFIER ON` and the others, for filtered indexes), creates only the indexes that are missing, and then, only if every index exists, drops the markers; otherwise it raises `Not every index exists; the markers are kept. Fix the cause and run this script again.` and keeps them. Running it again is safe. It is UTF-8 with a byte order mark; with sqlcmd add `-f 65001` so non-ASCII names are read correctly: `sqlcmd -S <server> -f 65001 -i dbo.OrderHistory.load-as-heap.sql`. If the failure was duplicate keys, fix the data first, or the key will fail again.
- **The next import refuses a marked table.** Every import, with or without `--load-as-heap`, checks its target tables for the markers. A marked table is an error and is skipped, unchanged: `<table>: an earlier --load-as-heap run left it without some of its indexes; run the script in <file> (...), then import again.` The path in the message is built from the current `--source-path`; if you import from another folder, use the script in the folder of the earlier run. If the script file is lost, the statements are in the table's extended properties `bcpjson_load_as_heap_*`: run them, then drop the properties.
- **A script without markers** (a run that stopped before dropping anything) only warns: `<file> is left over and <table> has no markers; if all its indexes exist, delete it.` Before deleting it, check that the indexes in the script all exist.

**Edition note.** Building an index uses several threads only on Enterprise and Developer editions. On other editions SQL Server builds each index on one thread, so recreating the indexes takes longer and the gain over `--disable-indexes` is smaller than below.

On the CI benchmark (5,000,000 rows into a table with a bigint identity clustered primary key and two nonclustered indexes, 8 JSON parts, SQL Server 2022 Developer on a 4-core runner, so indicative only), loading into the indexed table took 53.9 s and `--disable-indexes` took 30.1 s. Loading into a heap and building the indexes took 20.4 s (12.9 s load, 3.2 s clustered key, 1.7 s and 2.6 s for the nonclustered indexes), done by hand. At 1,000,000 rows the two options were even (5.0 s against 4.8 s), so the gain grows with the table. `benchmark.sh` also has an `import indexed, --load-as-heap` row; it has not been run yet (the benchmark workflow runs only from master).

### Copy

With `--split-tables`, `copy` streams each key range from the source straight into the target, several at once and without files in between. When `copy` creates the table, it first loads it as a heap (so the parts load in parallel) and builds the indexes after every part has arrived, as for any copy. Into a table that already exists, the load-mode rules above apply. Keys, indexes and check constraints are created only when every part succeeded; otherwise an error says `<table>: copy incomplete (<k> of <n> parts failed)` and the table needs to be copied again.

## JSON format

Each file is a JSON array with one object per row, one row per line:

```json
[{"CustomerID":1,"Name":"Contoso","Created":"2024-01-15T09:30:00","Logo":null}
,{"CustomerID":2,"Name":"Fabrikam","Created":"2024-02-01T00:00:00","Logo":"iVBORw0KGgo="}
]
```

- Dates and times are ISO 8601; `datetimeoffset` keeps its offset.
- `decimal`/`numeric` are JSON numbers with every digit, up to the full 38 digits of precision (beyond what .NET's `decimal` holds), and are read back exactly. A JSON parser in another tool may round very long numbers.
- `binary`/`varbinary`/`image` are base64.
- geography, geometry and hierarchyid are written as text (`POINT (-122.3 47.6)`, `/1/3/`) and parsed back on import. A geography or geometry value whose SRID is not the default (4326 for geography, 0 for geometry) gets an SRID prefix, as in PostGIS: `SRID=27700;POINT (530000 180000)`. Import rebuilds it with that SRID. Values with the default SRID have no prefix, so they look exactly as in older exports, and older files import as before.
- NULL is written explicitly as `null`.

## Known limitations

- **Passwords on the command line.** bcp only accepts SQL passwords via `-P`, so they are visible in the process list while bcp runs. Prefer Windows Authentication.
- **BCP character format uses `|` as the field terminator**; data containing `|` or line breaks will not round-trip through BCP files. Use JSON for such tables.
- BCP exports (`bcp out`) are not ordered, so BCP files are not reliably diff-able; JSON files are.
- `sql_variant` values are re-imported as `nvarchar`.
- `copy --source-query` is not supported.
- `copy` recreates keys, indexes and check constraints (and foreign keys and triggers on request), but not other objects such as views, procedures, functions or DDL triggers; indexes are placed on the target's default filegroup. A partitioned source table is created on the target's partition scheme of the same name if there is one (its function must take the partitioning column's type, or creating the table fails), and unpartitioned otherwise.
- **Split tables:** a key with one dominant value cannot be cut inside that value, and text keys may yield fewer parts than asked; BCP exports are unordered, so BCP imports give SQL Server no `ORDER` hint; `--batch-size 0` is one transaction per part, not per table; and a real table whose name ends in `.part` and four digits (for example `dbo.x.part0001`) is read as a part on import. bcpJson does not test loading a table with a sparse column set through JSON import or copy; such tables are never split.
- **`--load-as-heap`** applies only to split tables loaded into empty targets, and not to tables referenced by foreign keys, with XML, spatial, disabled or full-text indexes, indexed views, temporal tables, change tracking, replication, CDC, FILESTREAM, memory-optimized tables or clustered columnstore indexes (see the list under [Splitting large tables](#splitting-large-tables)). CI proves the recovery script is written but does not run it.
- `import` bulk loads without checking constraints (the bcp and SqlBulkCopy default, which is what makes it fast). Foreign key and check constraints on the target tables are not validated during the load and SQL Server marks them as not trusted; run `ALTER TABLE ... WITH CHECK CHECK CONSTRAINT ALL` afterwards if you need them trusted.
- BCP import into a table whose schema or table name contains `.` or `"` **and** that has a filtered, XML or spatial index fails (bcp cannot set `QUOTED_IDENTIFIER ON` for such names). Use JSON import for that table.
- `-k` on export is reserved; JSON always writes NULLs.

## Releasing

Releases are built by [`.github/workflows/release.yml`](.github/workflows/release.yml) from a version tag on `master`:

```powershell
git checkout master; git pull
git tag v2.1.0            # or v2.1.0-beta.1 for a pre-release
git push origin v2.1.0
```

The workflow checks that the tag is on `master`, reruns the full CI suite (unit tests and the SQL Server round trips), publishes and smoke-tests the executable for each platform on that platform, then creates the GitHub Release with generated notes, the packages, `SHA256SUMS` and a signed build-provenance attestation. The version shown by `bcpjson --version` comes from the tag.

To try the workflow without releasing anything, run it manually (*Actions → Release → Run workflow*): everything is built and tested and the packages are attached to the run as artifacts, but no release is created.

## Project layout

The solution `BcpJson.sln` holds the app in `src/BcpJson/` and the xUnit tests in `tests/BcpJson.Tests/`. Files in `src/BcpJson/`:

| File | Contents |
|---|---|
| `Program.cs` | Entry point, verb dispatch, console/log output, external process runner |
| `Program.Export.cs` | JSON and BCP export, per-table metadata query |
| `Program.Import.cs` | JSON (SqlBulkCopy) and BCP import |
| `JsonRowReader.cs` | Streams a JSON export into SqlBulkCopy as a data reader |
| `FileCompression.cs` | gzip reading and writing for `--compress` |
| `Program.Copy.cs` | Server-to-server copy (SMO + SqlBulkCopy) |
| `Program.Snapshot.cs` | Database snapshots for `--snapshot` |
| `Program.Resilience.cs` | Retries for transient failures, `.partial` files |
| `Program.Verify.cs` | The `verify` verb |
| `ExportManifest.cs` | `bcpjson-manifest.json`: read, write, file hashes, resume checks |
| `Program.Split.cs` | Split planning: `TableSplit` and `PlanSplit` (the split exports and copies run in `Program.Export.cs` and `Program.Copy.cs`) |
| `Program.Indexes.cs` | `--disable-indexes`: disabling and rebuilding indexes |
| `Program.HeapLoad.cs`, `HeapLoadScript.cs` | `--load-as-heap`: planning, dropping and recreating indexes; recovery script and markers |
| `SplitRequest.cs`, `KeyRange.cs`, `KeyInterpolation.cs`, `RangePlanner.cs` | `--split-tables` parsing, key ranges and boundary selection |
| `PartFiles.cs`, `LoadPlanning.cs`, `WorkQueue.cs` | Part file names, parallel or sequential loading, per-table work ordering |
| `Program.Sql.cs` | Table selection, name quoting, shared SQL helpers |
| `ExportOptions.cs`, `ImportOptions.cs`, `CopyOptions.cs`, `VerifyOptions.cs` | Command-line options for each verb |
| `SqlConnectionFactory.cs` | Connection string building and connection tests |
| `SqlBulkCopyHelper.cs` | Row-count helper for SqlBulkCopy |
| `CustomSelect.sql` | Reference copy of the JSON export metadata query |

## Resources

- [Under the hood: Automated backups](https://engineering.fb.com/2013/01/14/web/under-the-hood-automated-backups/) — Facebook's case for logical (mysqldump-style) backups
- [Continuous MySQL backup validation: Restoring backups](https://engineering.fb.com/2016/10/28/data-infrastructure/continuous-mysql-backup-validation-restoring-backups/) — continuously proving backups by restoring them
- [bcp utility](https://learn.microsoft.com/sql/tools/bcp-utility)
- [SqlBulkCopy](https://learn.microsoft.com/dotnet/api/microsoft.data.sqlclient.sqlbulkcopy)

## Security

Please report vulnerabilities privately, not in public issues. See [SECURITY.md](SECURITY.md) for how, and for security notes on running bcpJson.

## License

Licensed under the [Apache License, Version 2.0](LICENSE). Copyright 2019-2026 Chris Gelhaus.

You are free to use, modify and redistribute bcpJson, including commercially. If you redistribute it or a derivative, keep the copyright notice and include the [NOTICE](NOTICE) file, as the license requires.
