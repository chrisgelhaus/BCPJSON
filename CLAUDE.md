# CLAUDE.md

This file gives Claude Code guidance for working in this repository.

## What this is

**bcpJson** is a .NET 8 console tool for moving SQL Server data fast. It was inspired by Facebook's approach to backing up (and test-restoring) MySQL at scale: unload every table into a structured, diff-able file format so backups can be verified and compared. Author: Chris Gelhaus. Remote: `https://github.com/chrisgelhaus/BCPJSON` (default branch `master`).

## Current status

See `docs/ROADMAP.md` for what has been done, decisions made, and the ordered next steps. Update it when a step is completed.

## Build and run

```powershell
dotnet build BcpJson.sln                          # or the VS Code "build" task (.vscode/tasks.json)
dotnet test BcpJson.sln                           # unit tests (no database needed)
dotnet run --project src/BcpJson -- export --help # verb help
dotnet publish src/BcpJson -c Release             # single-file exe (PublishSingleFile=true)
```

- Target framework: `net8.0`. Assembly name `bcpjson`; debug output `src/BcpJson/bin/Debug/net8.0/bcpjson.dll` (used by `.vscode/launch.json`).
- **CI:** `.github/workflows/ci.yml` builds with warnings as errors and runs unit tests on Ubuntu and Windows, then an Ubuntu integration job against a SQL Server 2022 service container runs `tests/integration/roundtrip.sh` (`setup`, `json`, `bcp`, `copy`): it builds `BcpJsonSource` from `schema.sql` + `data.sql` and diffs JSON exports after each round trip. Add awkward data there when fixing a data-fidelity bug. The scripts can be run against any SQL Server with `SQL_SERVER`, `SQL_PASSWORD` and `BCPJSON` set.
- **Runtime prerequisite:** `bcp` (Windows `bcp.exe`, or mssql-tools18 on Linux) must be on `PATH` for the BCP export/import paths and for copying tables with CLR-type columns. Windows Auth is the default when no login/password is given.
- **Tests:** `tests/BcpJson.Tests` (xUnit) covers the pure helpers; it sees `internal` members via `InternalsVisibleTo`. Make a helper `internal` (not `private`) when it needs a test. The owner has **no local SQL Server**: anything needing a database runs in the GitHub Actions integration job (SQL Server service container).

## CLI verbs (CommandLineParser 2.9)

| Verb | Options class | Entry | Notes |
|---|---|---|---|
| `export` | `ExportOptions.cs` | `ExportData` → `ExportToBCP` / `ExportToJSON` / `ExportQueryTo*` | `--file-format bcp\|json`, table/schema include/exclude filters (`*` wildcards; no include filter = all user tables), `--source-query` + `--output-name`, `--nolock` |
| `import` | `ImportOptions.cs` | `ImportData` → `ImportFromBCP` / `ImportFromJSON` | Target table must exist. JSON import streams with `JsonTextReader` + `SqlBulkCopy`; CLR-type columns go through a `#bcpjson_stage` temp table and `::Parse()`. BCP import uses the `<schema>.<table>-c.fmt` format file (falls back to legacy `-c.xml`) with `-E -k`. |
| `copy` | `CopyOptions.cs` | `CopyData` → `BulkCopyTables` → `CopyTable` | SMO creates/drops/truncates the target, `SqlBulkCopy` moves data; tables with geography/geometry/hierarchyid fall back to bcp export+import via a temp folder. |

All verbs share `--trust-server-certificate` and `--log-file`. `Main` returns exit code 1 if any `InfoLevel.Error` message was posted.

## Code layout

`BcpJson.sln` at the root; the app is `src/BcpJson/` (namespace `bcpJson`, all files flat in that folder) and the tests are `tests/BcpJson.Tests/`. `Program` is a partial class split by verb:

- `Program.cs` — `Main`, verb dispatch, `ExecuteProcess` (bcp.exe), thread-safe `PostToConsole` (+ log file, error counting).
- `Program.Export.cs` — JSON/BCP export, `TableMetadataSql` (column list + deterministic sort order), `WriteJsonRows`, bcp argument/row-count helpers.
- `Program.Import.cs` — BCP and JSON import, `ReadJsonValue` type conversion.
- `Program.Copy.cs` — server-to-server copy.
- `Program.Sql.cs` — `TableRef`, `BuildSourceObjectIDList` (filters → T-SQL), `GetSourceTables`, `GetTableObject`, `QuoteName`/`SqlLiteral`/`LikeLiteral`.
- `SqlConnectionFactory.cs` — connection strings via `SqlConnectionStringBuilder`; `TryOpen` for validation.
- `SqlBulkCopyHelper.cs` — rows-copied via reflection (member name differs across SqlClient versions).
- `CustomSelect.sql` — reference copy of `TableMetadataSql`; not loaded at runtime, keep in sync.

### Export format
- JSON: `<schema>.<table>.json`, a JSON array, one row object per line (line-diffable). Ordered by PK, else by all sortable columns. CLR types (system_type_id 240) are written with `.ToString()`.
- BCP: `bcp format` (non-XML; Linux bcp has no `-x`) → `<schema>.<table>-c.fmt`, `bcp out` → `<schema>.<table>.bcp` (char mode, `|` delimiter) plus `_error.log` / `_output.log`; row counts are parsed from the output log.

## Rules for changes

- Quote identifiers with `QuoteName()` and literals with `SqlLiteral()`; parameterize values where possible. Never concatenate raw user input into SQL.
- Shared counters inside `Parallel.ForEach` must use `Interlocked`.
- Report problems with `PostToConsole(..., InfoLevel.Error)` so the exit code reflects them; catch per table/file so one failure doesn't stop the run.
- Set `CommandTimeout = 0` / `BulkCopyTimeout = 0` for data-moving commands.
- Run `dotnet test BcpJson.sln` before committing; add unit tests for new pure logic. Behaviour that touches SQL Server is only proven by the CI round-trip (export → import → compare) job.

## Remaining known issues

- bcp requires SQL passwords on the command line (`-P`).
- BCP char format with `|` delimiter can't round-trip data containing `|` or newlines; `bcp out` output is unordered.
- JSON loses geography/geometry SRID; `sql_variant` re-imports as nvarchar.
- `copy --source-query` and export `-k` are accepted but not implemented.
- `copy` creates target tables without primary keys or indexes.
- Linux bcp lacks `-x` and `-o`; don't reintroduce them (use non-XML format files and `ExecuteProcess(..., outputLogFile)`).

## Conventions

- C# files are UTF-8 with BOM and **CRLF** line endings; keep them that way.
- `Nullable` is disabled and `ImplicitUsings` is enabled.
- Console output goes through `PostToConsole(message, InfoLevel)` — reuse it rather than `Console.WriteLine`.
- Never commit connection strings or credentials; test against a local instance.
