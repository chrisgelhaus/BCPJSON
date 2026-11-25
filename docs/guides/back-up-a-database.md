# Back up a database

Use this guide to export a database's tables to files you can keep, compare from day to day, check later and restore elsewhere.

## Steps

1. **Export the tables.** JSON files are ordered and line-diffable; BCP files are faster to write and load but unordered.

   ```powershell
   bcpjson export --source-server sql01 --source-database Sales --file-format json `
                  --export-path D:\exports\Sales --snapshot
   ```

   With no table filters, every user table is exported. `--snapshot` makes every table reflect the same moment (see [below](#point-in-time-exports-with---snapshot)).

2. **Check the result.** The run exits with `0` when every table was exported, and `1` if anything failed (see [Exit codes](../reference/common.md#exit-codes)). The folder now holds one file per table, plus `bcpjson-manifest.json` with each file's size and SHA-256 (see [The export manifest](../reference/manifest.md)).

3. **Verify the copy you keep.** After moving the export to archive storage, check it against its manifest:

   ```powershell
   bcpjson verify --path D:\exports\Sales
   ```

4. **Prove the backup.** Restore it into a scratch database from time to time: see [Restore an export](restore-an-export.md).

## Variations

### Only some tables, as BCP files

```powershell
bcpjson export --source-server sql01 --source-database Sales --include-schemas Sales `
               --exclude-tables "Sales.*History" --file-format bcp --export-path D:\exports\Sales
```

The filters take comma-separated names with `*` wildcards; see [Table filters](../reference/common.md#table-filters). BCP needs the `bcp` utility on `PATH`. A BCP export also writes a `<schema>.<table>-c.fmt` format file per table, which import needs.

### One query to one file

```powershell
bcpjson export --source-server sql01 --source-database Sales --file-format json --export-path D:\exports `
               --source-query "SELECT TOP (100) * FROM Sales.Customer ORDER BY CustomerID" --output-name top_customers
```

This writes `D:\exports\top_customers.json`. Only the first result set is written. Query exports are not retried, because the query may be a procedure with side effects.

### Point-in-time exports with `--snapshot`

Tables are read one after another, or a few at a time with `--max-tasks`. On a busy database a long export therefore does not capture a single point in time. A table read at minute 40 contains changes made after the tables read at minute 1. When such an export is restored or compared, the typical symptoms are:

- child rows (order lines, payments) whose parent rows are missing, or the reverse;
- totals that do not reconcile across tables;
- day-to-day diffs that show changes in one table but not in the related table that changed in the same transaction.

With `--snapshot`, bcpJson:

1. creates a [database snapshot](https://learn.microsoft.com/sql/relational-databases/databases/database-snapshots-sql-server) of the source database, named `<database>_bcpjson_<UTC timestamp>`, with its sparse files next to the source data files;
2. reads every table from that snapshot: in parallel, in JSON or BCP, or through `copy`. A `--source-query` runs in the snapshot database too;
3. drops the snapshot at the end, also when the export fails.

The console reports when the snapshot was created, and every file reflects that moment. Creating a snapshot is typically near-instant whatever the database size, because its files start empty. The source stays fully online for reads and writes throughout.

```powershell
# Nightly point-in-time export of a busy database
bcpjson export --source-server sql01 --source-database Sales --file-format json `
               --export-path D:\exports\Sales\2026-11-20 --snapshot

# Compare two nightly exports: only the rows that changed show up
git diff --no-index --stat D:\exports\Sales\2026-11-19 D:\exports\Sales\2026-11-20
```

**Use `--snapshot` for:**
- backups you intend to restore or verify, so the restored database is transactionally consistent;
- day-to-day comparisons of exports, so a diff shows real changes rather than timing artefacts;
- long exports or copies of a database that is being written to while they run;
- databases with foreign keys between tables, where a restore must not contain orphaned rows.

**Don't use it when:**
- **The source is idle** during the export (a maintenance window, a restored copy, a read-only database). There is nothing to drift, so the snapshot adds no value.
- **You have no permission.** Creating a snapshot needs `CREATE DATABASE` permission on the server, for example through the `dbcreator` role. Without it the run stops with an error rather than silently exporting without a snapshot.
- **The source can't be snapshotted:**
  - Azure SQL Database does not support database snapshots;
  - SQL Server older than 2016 SP1 supports them only in Enterprise and Developer editions;
  - databases with memory-optimized (In-Memory OLTP) filegroups cannot be snapshotted.

  Export from a database copy or a point-in-time restore instead.
- **The source is very write-heavy and the data volume has little free space.** While the snapshot exists, the first change to each source page copies the original page into the snapshot's sparse file. The cost grows with the amount of data changed during the run, not with database size.

**Notes:**
- `--nolock` is unnecessary with `--snapshot`: a snapshot is read-only, so reads cannot block writers. Without `--snapshot`, `--nolock` is the opposite trade-off. It avoids blocking but can read uncommitted, inconsistent data, which is not something to rely on for backups.
- **How it is tested.** On every change, CI runs a writer that keeps moving value between two 300,000-row tables in single transactions, so their combined total is constant at every committed moment. Two `export --snapshot` runs made while transfers are committing must each add up to exactly that total. The same export without `--snapshot` must be caught drifting, which proves the check can tell the difference.

### Compressed exports

```powershell
# Nightly JSON backup, compressed
bcpjson export --source-server sql01 --source-database Sales --file-format json --export-path D:\exports\Sales --compress gzip

# Smallest files for archive storage (slower to write)
bcpjson export --source-server sql01 --source-database Sales --file-format bcp --export-path D:\exports\Sales --compress gzip --compression-level smallest
```

- **How each format is compressed:**
  - JSON is compressed while it is written, so no uncompressed copy ever reaches the disk.
  - BCP data is compressed after `bcp` has written it, because bcp can only write plain files. So the export briefly needs room for one table's uncompressed file. Format files stay uncompressed.
- **The manifest** records `"compression": "gzip"` and the size and SHA-256 of the `.gz` files, so `verify` works the same. `--resume` refuses to continue an export with a different `--compress` setting.
- **Why gzip:** every platform can read it without bcpJson, with `zcat`, `gzip -d`, 7-Zip or `tar`. To diff two compressed exports, decompress them first, for example `diff <(zcat a/dbo.T.json.gz) <(zcat b/dbo.T.json.gz)`.
- **Import** reads `.gz` files directly (see [Restore an export](restore-an-export.md)).

**Which level?**
- `fastest` (the default) gets most of the reduction, and is fast enough not to slow down an export.
- `optimal` saves a little more space for noticeably more CPU, and `smallest` saves more again. Use them for archives written once and kept long.

How much a file shrinks depends on the data. Repetitive JSON (the same property names on every row, many similar values) compresses best. The CI test data, which is deliberately varied, shrinks 3.2 times.

**Why compression is off by default.** Plain files can be diffed line by line, opened in any editor and read by any tool. That was the point of bcpJson's format, and existing scripts expect `.json` and `.bcp` names. Turn compression on when disk space or copy time matters more.

### Very large tables

Cut big tables into key ranges that export in parallel: see [Large tables](large-tables.md).

## If something goes wrong

- **A transient failure** (network drop, failover, throttling): the table is retried automatically; see [Retries](../reference/common.md#retries).
- **The export stopped partway:** rerun the same command with `--resume`; see [Resume an interrupted export](recover-a-failed-run.md#resume-an-interrupted-export).
- **A snapshot was left behind** after a killed run: find it and drop it.

  ```sql
  SELECT name, create_date FROM sys.databases WHERE source_database_id IS NOT NULL AND name LIKE N'%[_]bcpjson[_]%';
  DROP DATABASE [Sales_bcpjson_20261120214450];
  ```

## See also

- [export options](../reference/export.md)
- [verify](../reference/verify.md)
- [The export manifest](../reference/manifest.md)
- [JSON format](../reference/json-format.md)
