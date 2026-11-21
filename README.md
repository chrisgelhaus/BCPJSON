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

- Tables are processed in parallel (`--max-tasks` limits how many at once).
- Select tables with include/exclude lists for tables and schemas, with `*` wildcards. With no include filters, every user table is selected.
- JSON exports are ordered by primary key (or by every sortable column if there is no PK), so repeated exports of unchanged data are identical.
- **Fast loads by default:** imports and copies take a table lock (`TABLOCK`), which allows minimal logging. Opt out with `--no-table-lock` — see [Table locking](#table-locking).
- **Point-in-time exports:** `--snapshot` reads every table from a database snapshot, so a long export or copy reflects one moment — see [Consistent point-in-time exports](#consistent-point-in-time-exports).
- Windows Authentication by default; SQL logins supported.
- Exit code `0` on success, `1` if anything reported an error — suitable for scheduled jobs.

## Requirements

- Windows (x64 or Arm64) or Linux (x64). Release downloads are self-contained: no .NET install needed. Building from source needs the [.NET 8 SDK](https://dotnet.microsoft.com/download).
- For `--file-format bcp` and for `copy` of tables with geography/geometry/hierarchyid columns: the SQL Server **bcp utility** on `PATH` (installed with SQL Server or the *Microsoft Command Line Utilities for SQL Server*).
- SQL Server 2008 or later. `--snapshot` needs SQL Server 2016 SP1 or later (any edition) and is not available on Azure SQL Database.

## Install

Download the package for your platform from the [Releases page](https://github.com/chrisgelhaus/BCPJSON/releases):

| Platform | Package |
|---|---|
| Windows x64 | `bcpjson-<version>-win-x64.zip` |
| Windows Arm64 | `bcpjson-<version>-win-arm64.zip` |
| Linux x64 | `bcpjson-<version>-linux-x64.tar.gz` |

Each package holds a single self-contained executable (`bcpjson.exe` / `bcpjson`) plus `LICENSE`, `NOTICE` and this README. Unpack it anywhere and add the folder to `PATH`.

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

Use `-r win-arm64` or `-r linux-x64` for the other platforms.

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

**Output files**

- JSON: `<schema>.<table>.json`
- BCP: `<schema>.<table>.bcp` (pipe-delimited character data), `<schema>.<table>-c.fmt` (format file, needed for import; older exports' `-c.xml` files are still accepted), plus `_output.log` / `_error.log` from bcp.

### Import

The target table must already exist (create the schema first, e.g. from a script of the source database).

```powershell
# All JSON files in a folder; table name taken from each file name
bcpjson import --file-format json --source-path C:\dumps\aw --source-file "*.json" `
               --target-database AdventureWorks_Restore --truncate-target

# One BCP file into an explicitly named table
bcpjson import --file-format bcp --source-path C:\dumps\aw --source-file Sales.Customer.bcp `
               --target-database Scratch --target-table dbo.CustomerCopy
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
| `--trust-server-certificate`, `--log-file` | As for export. |

Identity values and NULLs are preserved. Computed and `rowversion` columns are skipped and recalculated by the server.

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
| `--no-indexes` | Don't recreate primary keys, unique constraints and indexes on the tables copy creates (default: recreated). See [Keys and indexes](#keys-and-indexes). |
| `--foreign-keys` | Also recreate foreign keys (default: off). See [Foreign keys](#foreign-keys). |
| `--no-check-constraints` | Don't recreate check constraints (default: recreated). See [Check constraints](#check-constraints). |
| `--triggers` | Also recreate triggers (default: off). See [Triggers](#triggers). |
| `--trust-server-certificate`, `--log-file` | As for export. |

Missing target tables are created from the source definition: columns, nullability, identity, defaults and computed columns, then — after the data is loaded — the primary key, unique constraints and indexes. Tables with geography, geometry or hierarchyid columns are moved with `bcp` through a temporary folder.

#### Keys and indexes

**Default:** on. **Option:** `--no-indexes` turns it off.

When `copy` creates a target table (it did not exist, or `--drop-target` dropped it), it recreates every primary key, unique constraint and index of the source table: clustered and nonclustered, unique and filtered indexes, included columns, descending keys, data compression, columnstore, XML and spatial indexes. They are built **after the data is loaded**, because bulk loading into a table without indexes (with the default table lock) is the fastest way to load, and building each index once at the end is much cheaper than maintaining it row by row. The clustered index is built first, then the others in dependency order.

Indexes are created on the target database's default filegroup: filegroup and partition-scheme placement is not copied, because the target may not have the same filegroups. Index and constraint names are kept, except for constraints that were declared without a name (such as `PRIMARY KEY` on a column): SQL Server named those itself (`PK__Orders__3214EC07…`), and the copy gets its own system-generated name, just as running the original `CREATE TABLE` would. An index that cannot be created is reported as an error (exit code 1) and the remaining indexes are still created.

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

The lock is held while each batch is written (10,000 rows for BCP and copy, 50,000 for JSON import), so for practical purposes other sessions cannot use the table until the load finishes. On a heap the lock is a bulk-update (BU) lock; on a table with a clustered index it is an exclusive table lock.

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
- **Several files load into the same table with a clustered index** (as in the third example). With table locks those loads run one at a time; without them they can run in parallel. Heaps are the exception: BU locks are compatible with each other, so parallel table-locked loads into a heap work well and you can keep the default.
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

## JSON format

Each file is a JSON array with one object per row, one row per line:

```json
[{"CustomerID":1,"Name":"Contoso","Created":"2024-01-15T09:30:00","Logo":null}
,{"CustomerID":2,"Name":"Fabrikam","Created":"2024-02-01T00:00:00","Logo":"iVBORw0KGgo="}
]
```

- Dates and times are ISO 8601; `datetimeoffset` keeps its offset.
- `binary`/`varbinary`/`image` are base64.
- geography, geometry and hierarchyid are written as text (`POINT (-122.3 47.6)`, `/1/3/`) and parsed back on import.
- NULL is written explicitly as `null`.

## Known limitations

- **Passwords on the command line.** bcp only accepts SQL passwords via `-P`, so they are visible in the process list while bcp runs. Prefer Windows Authentication.
- **BCP character format uses `|` as the field terminator**; data containing `|` or line breaks will not round-trip through BCP files. Use JSON for such tables.
- BCP exports (`bcp out`) are not ordered, so BCP files are not reliably diff-able; JSON files are.
- geography/geometry exported to JSON lose their SRID (re-imported as 4326 / 0).
- `sql_variant` values are re-imported as `nvarchar`.
- `copy --source-query` is not supported.
- `copy` recreates keys, indexes and check constraints (and foreign keys and triggers on request), but not other objects such as views, procedures, functions or DDL triggers; indexes are placed on the target's default filegroup.
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
| `Program.Copy.cs` | Server-to-server copy (SMO + SqlBulkCopy) |
| `Program.Snapshot.cs` | Database snapshots for `--snapshot` |
| `Program.Sql.cs` | Table selection, name quoting, shared SQL helpers |
| `ExportOptions.cs`, `ImportOptions.cs`, `CopyOptions.cs` | Command-line options for each verb |
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
