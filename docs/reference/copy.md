# copy

Copies tables from one database to another with `SqlBulkCopy`, creating target tables that do not exist. Tables with geography, geometry or hierarchyid columns are moved with `bcp` through a temporary folder.

```powershell
bcpjson copy --source-server sql01 --source-database Sales --target-server sql02 --target-database Sales --drop-target
```

How-to: [Copy between servers](../guides/copy-between-servers.md), [Large tables](../guides/large-tables.md#copy).

## Source

| Option | Default | Description |
|---|---|---|
| `--source-server` | `localhost` | SQL Server instance to read from. |
| `--source-database` | | Database to copy from. |
| `--source-login`, `--source-password` | empty | SQL login for the source. Leave both out for Windows Authentication. See [Connecting](common.md#connecting). |
| `--nolock` | off | Read the source `WITH (NOLOCK)`. Can return dirty reads. |
| `--snapshot` | off | Copy from a database snapshot of the source, so every table reflects the same moment. Needs `CREATE DATABASE` permission and SQL Server 2016 SP1 or later; the snapshot is dropped at the end. Only the source is affected. |

## Target

| Option | Default | Description |
|---|---|---|
| `--target-server` | `localhost` | SQL Server instance to write to. |
| `--target-database` | | Database to write to. It must exist. |
| `--target-login`, `--target-password` | empty | SQL login for the target. Leave both out for Windows Authentication. |
| `--trust-server-certificate` | off | Accept both servers' TLS certificates without validation. |
| `--drop-target` | off | Drop target tables that exist and create them again from the source. Foreign keys defined on those tables are dropped first, so a copy made with `--foreign-keys` can be refreshed. |
| `--truncate-target` | off | Empty target tables that exist before copying. |
| `--no-table-lock` | off (tables are locked) | Load without a table lock (`TABLOCK`). See [Load into a live database](../guides/load-into-a-live-database.md). |
| `--load-as-heap` | off | For split tables copied into an existing table with a clustered index: drop its indexes, copy the parts in parallel into the heap, then recreate the indexes. Other existing tables load as with `--disable-indexes`. Needs `--recovery-path` and `--snapshot`. See [Large tables](../guides/large-tables.md#copy). |
| `--disable-indexes` | off | Disable the nonclustered indexes of existing target tables during the copy, except the primary key and indexes used by foreign keys, and rebuild them afterwards. See [Large tables](../guides/large-tables.md#copy). |
| `--recovery-path` | | Folder for the recovery script of each table copied with `--load-as-heap`; created if missing. See [The load-as-heap recovery script](../guides/recover-a-failed-run.md#the-load-as-heap-recovery-script). |

## Tables

| Option | Default | Description |
|---|---|---|
| `--include-tables` | all user tables | Comma-separated `schema.table` names to copy. `*` is a wildcard. See [Table filters](common.md#table-filters). |
| `--include-schemas` | | Comma-separated schemas to copy. |
| `--exclude-tables` | | Comma-separated `schema.table` names to leave out. |
| `--exclude-schemas` | | Comma-separated schemas to leave out. |
| `--split-tables` | off | Copy the named tables as parallel key-range streams: `schema.table[:parts]`, `*` wildcard, 2 to 200 parts, default 8. Tables with geography, geometry or hierarchyid columns are never split. See [Large tables](../guides/large-tables.md#copy). |

## What copy creates on new tables

| Option | Default | Description |
|---|---|---|
| `--no-indexes` | off (indexes are created) | Do not recreate the primary keys, unique constraints and indexes on tables copy creates. Cannot be combined with `--foreign-keys`. See [Keys and indexes](../guides/copy-between-servers.md#keys-and-indexes). |
| `--no-check-constraints` | off (created) | Do not recreate check constraints on tables copy creates. See [Check constraints](../guides/copy-between-servers.md#check-constraints). |
| `--foreign-keys` | off | Also recreate the foreign keys of the tables copy creates, after every table is loaded. Use with `--snapshot`; bcpJson warns if you don't. See [Foreign keys](../guides/copy-between-servers.md#foreign-keys). |
| `--triggers` | off | Also recreate the DML triggers of the tables copy creates, after every table is loaded. See [Triggers](../guides/copy-between-servers.md#triggers). |

Tables that already exist keep their own indexes, constraints and triggers. copy does not add, change or remove them; `--load-as-heap` drops their indexes for the load and recreates the same ones.

## Running

| Option | Default | Description |
|---|---|---|
| `--max-tasks` | unlimited (`-1`; `0` also means unlimited) | Most tables, or parts, copied at once. See [Parallelism](common.md#parallelism). |
| `--retries` | `3` | Retries when opening a connection fails transiently. Copies themselves are not retried. `0` turns retries off. See [Retries](common.md#retries). |
| `--log-file` | | Also append every console message to this file. See [Logging](common.md#logging). |

## Not implemented

| Option | Default | Description |
|---|---|---|
| `--source-query` | | Not implemented for copy; accepted and ignored with a warning. Use `export --source-query` and `import` instead. |

## Batches

copy writes batches of 10,000 rows. The table lock is held while each batch is written.
