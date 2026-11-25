# export

Writes each selected table to its own file, as JSON (read through SqlClient) or BCP character format (written by the `bcp` utility), or writes the first result set of one query to a single file.

```powershell
bcpjson export --source-server sql01 --source-database Sales --file-format json --export-path D:\exports\Sales
```

How-to: [Back up a database](../guides/back-up-a-database.md), [Large tables](../guides/large-tables.md), [Recover a failed run](../guides/recover-a-failed-run.md).

## Source

| Option | Default | Description |
|---|---|---|
| `--source-server` | `localhost` | SQL Server instance to read from. |
| `--source-database` | | Database to export. Required unless `--source-query` is used; required with `--snapshot`. |
| `--source-login`, `--source-password` | empty | SQL login. Leave both out for Windows Authentication. See [Connecting](common.md#connecting). |
| `--trust-server-certificate` | off | Accept the server's TLS certificate without validation (typical for local instances with self-signed certificates). |
| `--nolock` | off | Read tables `WITH (NOLOCK)` (JSON export). Avoids blocking writers but can return dirty, inconsistent reads; unnecessary with `--snapshot`. |
| `--snapshot` | off | Read every table from a database snapshot of the source, so the whole export reflects one moment. Needs `CREATE DATABASE` permission and SQL Server 2016 SP1 or later; the snapshot is dropped at the end. See [Point-in-time exports](../guides/back-up-a-database.md#point-in-time-exports-with---snapshot). |

## Tables

| Option | Default | Description |
|---|---|---|
| `--include-tables` | all user tables | Comma-separated `schema.table` names to export. `*` is a wildcard. See [Table filters](common.md#table-filters). |
| `--include-schemas` | | Comma-separated schemas to export. `*` is a wildcard. |
| `--exclude-tables` | | Comma-separated `schema.table` names to leave out. `*` is a wildcard. |
| `--exclude-schemas` | | Comma-separated schemas to leave out. `*` is a wildcard. |
| `--split-tables` | off | Cut the named tables into key ranges exported in parallel: comma-separated `schema.table[:parts]`, `*` wildcard, 2 to 200 parts, default 8; the first matching entry wins. Cannot be combined with `--source-query`. See [Large tables](../guides/large-tables.md). |

## Query export

| Option | Default | Description |
|---|---|---|
| `--source-query` | | A Transact-SQL query or procedure call to export instead of tables. Only the first result set is written, to `<output-name>.json` or `.bcp`. With `--snapshot` it runs in the snapshot database. Query exports are not retried, because the query may have side effects. |
| `--output-name` | `query` | Base file name for `--source-query`. |

## Output

| Option | Default | Description |
|---|---|---|
| `--file-format` | `bcp` | `json` or `bcp`. BCP needs the `bcp` utility on `PATH`. |
| `--export-path` | current directory | Folder for the files; created if missing. |
| `--compress` | `none` | `none` or `gzip`. gzip writes `.json.gz` / `.bcp.gz`; JSON is compressed while it is written, BCP data after `bcp` has written it (the export briefly needs room for one table's uncompressed file). Format files and the manifest stay plain. See [Compressed exports](../guides/back-up-a-database.md#compressed-exports). |
| `--compression-level` | `fastest` | gzip level with `--compress gzip`: `fastest`, `optimal` or `smallest`. |

## Running

| Option | Default | Description |
|---|---|---|
| `--max-tasks` | unlimited (`-1`; `0` also means unlimited) | Most tables, or parts of split tables, exported at once; the largest start first. See [Parallelism](common.md#parallelism). |
| `--retries` | `3` | Retries after a transient failure: opening a connection, and exporting a table, which is redone from the start. `0` turns retries off. See [Retries](common.md#retries). |
| `--resume` | off | Continue an interrupted export into the same `--export-path`: tables (and parts) the manifest records with intact files are skipped. Must use the same `--file-format`, `--compress`, `--source-database` and `--source-query`. See [Recover a failed run](../guides/recover-a-failed-run.md#resume-an-interrupted-export). |
| `--log-file` | | Also append every console message to this file. See [Logging](common.md#logging). |

## Not implemented

| Option | Default | Description |
|---|---|---|
| `-k` | off | Not implemented; reserved. JSON exports always write NULL values explicitly. |

## Output files

- **JSON:** `<schema>.<table>.json` (`.json.gz` with `--compress gzip`). A split table gets `<schema>.<table>.part0001.json`, `.part0002.json` and so on instead. The format is described in [JSON format](json-format.md).
- **BCP:**
  - `<schema>.<table>.bcp`: pipe-delimited character data, `.bcp.gz` with `--compress gzip`.
  - `<schema>.<table>-c.fmt`: the format file, which import needs. Older exports' `-c.xml` files are still accepted.
  - `_output.log` and `_error.log` from bcp.

  The parts of a split table are `<schema>.<table>.part0001.bcp` and so on. They share one format file, and each part has its own pair of logs.
- **`bcpjson-manifest.json`:** what was exported, with each file's size and SHA-256. See [The export manifest](manifest.md).
- Every file is first written as `<name>.partial` and renamed when complete. See [Complete files only](../guides/recover-a-failed-run.md#complete-files-only).

JSON exports are ordered by primary key, or by every sortable column when there is no primary key, so repeated exports of unchanged data are identical. BCP exports are not ordered.
