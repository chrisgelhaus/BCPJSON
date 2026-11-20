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
| `export` | Writes each selected table to its own file, in **JSON** (via SqlClient) or native **BCP** character format (via `bcp.exe`). Can also export the result of a single `--source-query`. |
| `import` | Loads JSON or BCP files produced by `export` back into existing tables. |
| `copy` | Copies tables server-to-server with `SqlBulkCopy`, creating target tables if needed. |

- Tables are processed in parallel.
- Select tables with include/exclude lists for tables and schemas, with `*` wildcards. With no include filters, every user table is selected.
- JSON exports are ordered by primary key (or by every sortable column if there is no PK), so repeated exports of unchanged data are identical.
- Windows Authentication by default; SQL logins supported.
- Exit code `0` on success, `1` if anything reported an error — suitable for scheduled jobs.

## Requirements

- Windows. The BCP paths call `bcp`; they also run on Linux with mssql-tools18, which is how CI tests them.
- [.NET 8 SDK](https://dotnet.microsoft.com/download) to build, or the .NET 8 runtime to run a framework-dependent build.
- For `--file-format bcp` and for `copy` of tables with geography/geometry/hierarchyid columns: the SQL Server **bcp utility** on `PATH` (installed with SQL Server or the *Microsoft Command Line Utilities for SQL Server*).
- SQL Server 2008 or later.

## Build

```powershell
git clone https://github.com/chrisgelhaus/BCPJSON.git
cd BCPJSON
dotnet build -c Release
dotnet test                                           # unit tests, no database needed
dotnet publish src/BcpJson -c Release -r win-x64      # single-file bcpjson.exe in src\BcpJson\bin\Release\net8.0\win-x64\publish
```

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
| `--trust-server-certificate`, `--log-file` | As for export. |

Identity values and NULLs are preserved. Computed and `rowversion` columns are skipped and recalculated by the server.

### Copy

```powershell
bcpjson copy --source-server PROD01 --source-database Sales `
             --target-server DEV01  --target-database Sales_Copy `
             --include-schemas dbo --drop-target --max-tasks 4
```

Copy accepts the same table/schema filters as export, plus `--drop-target`, `--truncate-target`, `--max-tasks` (default unlimited) and `--nolock`. Missing target tables are created from the source definition (columns, nullability, identity, defaults, computed columns). Tables with geography, geometry or hierarchyid columns are moved with `bcp` through a temporary folder.

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

- **Passwords on the command line.** bcp.exe only accepts SQL passwords via `-P`, so they are visible in the process list while bcp runs. Prefer Windows Authentication.
- **BCP character format uses `|` as the field terminator**; data containing `|` or line breaks will not round-trip through BCP files. Use JSON for such tables.
- BCP exports (`bcp out`) are not ordered, so BCP files are not reliably diff-able; JSON files are.
- geography/geometry exported to JSON lose their SRID (re-imported as 4326 / 0).
- `sql_variant` values are re-imported as `nvarchar`.
- `copy --source-query` is not supported.
- `copy` creates missing target tables without primary keys or indexes; add them afterwards if needed.
- `-k` on export is reserved; JSON always writes NULLs.

## Project layout

The solution `BcpJson.sln` holds the app in `src/BcpJson/` and the xUnit tests in `tests/BcpJson.Tests/`. Files in `src/BcpJson/`:

| File | Contents |
|---|---|
| `Program.cs` | Entry point, verb dispatch, console/log output, external process runner |
| `Program.Export.cs` | JSON and BCP export, per-table metadata query |
| `Program.Import.cs` | JSON (SqlBulkCopy) and BCP import |
| `Program.Copy.cs` | Server-to-server copy (SMO + SqlBulkCopy) |
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

## License

Licensed under the [Apache License, Version 2.0](LICENSE). Copyright 2019-2026 Chris Gelhaus.

You are free to use, modify and redistribute bcpJson, including commercially. If you redistribute it or a derivative, keep the copyright notice and include the [NOTICE](NOTICE) file, as the license requires.
