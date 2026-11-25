# CLAUDE.md

This file gives Claude Code guidance for working in this repository.

## What this is

**bcpJson** is a .NET 8 console tool for moving SQL Server data fast. It was inspired by Facebook's approach to backing up (and test-restoring) MySQL at scale: unload every table into a structured, diff-able file format so backups can be verified and compared. Author: Chris Gelhaus. Remote: `https://github.com/chrisgelhaus/BCPJSON` (default branch `master`, protected: work on a branch and merge through a PR once the five required CI checks pass: two builds and the SQL Server 2019/2022/2025 round trips).

## Current status

See `docs/ROADMAP.md` for what has been done, decisions made, and the ordered next steps. Update it when a step is completed.

## Build and run

```powershell
dotnet build BcpJson.sln                          # or the VS Code "build" task (.vscode/tasks.json)
dotnet test BcpJson.sln                           # unit tests (no database needed)
dotnet run --project src/BcpJson -- export --help # verb help
dotnet publish src/BcpJson -c Release -r win-x64 -o publish  # self-contained single-file exe (needs -r)
```

- Target framework: `net8.0`. Assembly name `bcpjson`; debug output `src/BcpJson/bin/Debug/net8.0/bcpjson.dll` (used by `.vscode/launch.json`).
- **CI:** `.github/workflows/ci.yml` builds with warnings as errors and runs unit tests on Ubuntu and Windows, then an Ubuntu integration job (matrix: SQL Server 2019, 2022 and 2025 service containers; the job name "Round trip against SQL Server <version>" is a required check, so renaming it means updating branch protection) runs `tests/integration/roundtrip.sh` (`setup`, `json`, `bcp`, `copy`, `snapshot`, `consistency`, `resilience`, `compression`, `split`) against the published linux-x64 single-file executable: it builds `BcpJsonSource` from `schema.sql` + `data.sql` and diffs JSON exports after each round trip. Add awkward data there when fixing a data-fidelity bug. The scripts can be run against any SQL Server with `SQL_SERVER`, `SQL_PASSWORD` and `BCPJSON` set. The `split` mode also builds the side database `BcpJsonSplitOdd` (temporal and sparse-column-set tables); the CI step "Bulk load lock modes" runs `tests/integration/lock_probe.ps1`, which measures the locks concurrent TABLOCK loads take (heap: BU, parallel; clustered rowstore: Sch-M/X; clustered columnstore: S+X, so it is loaded sequentially, `LoadPlanning.ClusteredColumnstoreLoadsInParallel`). `.github/workflows/benchmark.yml` (manual, must be on master to dispatch; `tests/integration/benchmark.sh`) times split exports and the load options on a large table; it is not a required check.
- **Releases:** `.github/workflows/release.yml` runs on `v*` tags that are on master: it reruns ci.yml (`workflow_call`), publishes and smoke-tests win-x64, win-arm64 (`windows-11-arm` runner), linux-x64, linux-arm64 (`ubuntu-24.04-arm`) and osx-arm64 (`macos-latest`; only round trips on linux-x64 touch SQL Server), then creates the GitHub Release with packages, `SHA256SUMS` and a provenance attestation. Manual runs are dry runs (artifacts only). The version comes from the tag via `-p:Version`; the csproj `<Version>` is only the development default.
- **Security & dependencies:** Dependabot (`.github/dependabot.yml`) opens weekly update PRs; they merge only with green CI like any other PR. CodeQL runs as GitHub's default setup (no workflow file). Secret scanning push protection is on. Vulnerability reports come in privately per `SECURITY.md`.
- **Runtime prerequisite:** `bcp` (Windows `bcp.exe`, or mssql-tools18 on Linux) must be on `PATH` for the BCP export/import paths and for copying tables with CLR-type columns. Windows Auth is the default when no login/password is given.
- **Tests:** `tests/BcpJson.Tests` (xUnit) covers the pure helpers; it sees `internal` members via `InternalsVisibleTo`. Make a helper `internal` (not `private`) when it needs a test. The owner has **no local SQL Server**: anything needing a database runs in the GitHub Actions integration job (SQL Server service container).

## CLI verbs (CommandLineParser 2.9)

| Verb | Options class | Entry | Notes |
|---|---|---|---|
| `export` | `ExportOptions.cs` | `ExportData` → `ExportToBCP` / `ExportToJSON` / `ExportQueryTo*` | `--file-format bcp\|json`, table/schema include/exclude filters (`*` wildcards; no include filter = all user tables), `--source-query` + `--output-name`, `--nolock`, `--split-tables pattern[:parts]` (see **Key-range splitting**) |
| `import` | `ImportOptions.cs` | `ImportData` → `ImportFromBCP` / `ImportFromJSON` | Target table must exist. JSON import streams: `JsonRowReader` (a `DbDataReader` over `JsonTextReader`, values via `ReadJsonValue`, decimals as `SqlDecimal`) feeds `SqlBulkCopy.WriteToServer`, committing every `--batch-size` rows (default `DefaultBatchSize` 50,000; 0 = one transaction); no `DataTable`. CLR-type columns go through a `#bcpjson_stage` temp table and `ClrFromText` (`::Parse()`, or `STGeomFromText(wkt, srid)` for `SRID=n;` text). Order hints: JSON export records the PK it sorts by as `orderedBy` in the manifest (`ReadKeyColumns`); import (`SortKeyOf`) adds `ColumnOrderHints` only when `ManifestKeyColumn.SameOrder` says the target's clustered key is identical (columns, types, directions, collations), never on the staging path: SQL Server fails a load whose rows break the hint. BCP import uses the `<schema>.<table>-c.fmt` format file (falls back to legacy `-c.xml`) with `-E -k` plus `-q` (QUOTED_IDENTIFIER ON, required for tables with filtered/XML/spatial indexes) via `BcpInArguments`: with `-q` bcp needs the name unbracketed as `schema.table`, table first and direction second; `-b` from `--batch-size`. `.gz` files: JSON is decompressed while streaming, BCP to a temp folder first; `ImportBaseName` strips `.gz` for the table name. `PartFiles.GroupImportFiles` groups `<table>.partNNNN.<ext>` files per table (a second group for the same table is refused); `--disable-indexes` and `--load-as-heap` (see **Key-range splitting**). |
| `copy` | `CopyOptions.cs` | `CopyData` → `BulkCopyTables` → `CopyTable` | SMO creates/drops/truncates the target, `SqlBulkCopy` moves data; tables with geography/geometry/hierarchyid fall back to bcp export+import via a temp folder. On tables it created, `CreateTargetIndexes` then scripts every source index with SMO (`NoFileGroup`) in `IndexCreationRank` order; `--no-indexes` skips it. `CreateTargetCheckConstraints` follows per table (`--no-check-constraints` skips it). With `--foreign-keys`, `CreateTargetForeignKeys` scripts the FKs of all created tables after the parallel loop, one at a time; `--drop-target` first runs `DropForeignKeysOwnedBy` so re-runs aren't blocked. `--triggers` runs `CreateTargetTriggers` last (enabled state and `sp_settriggerorder` applied explicitly). `--split-tables`: ranges become parallel source-to-target `SqlBulkCopy` streams; a table copy creates is a heap until its indexes are built after all parts load, and indexes/constraints are skipped (error `copy incomplete`) if any part failed. A partitioned source is created on the target's partition scheme of the same name when it exists (its function must take the partitioning column's type, or table creation fails). |
| `verify` | `VerifyOptions.cs` | `VerifyExport` → `CheckExport` | No database. Checks every file of `bcpjson-manifest.json` (exists, size, SHA-256) and that `completed` is set; unlisted data files and `.partial` leftovers are warnings. |

All verbs share `--trust-server-certificate`, `--log-file` and `--max-tasks` (via `Parallelism()`). Loads (JSON/BCP import, copy) use TABLOCK by default through `LoadOptions()` / `BcpLoadHints()`; `--no-table-lock` turns it off. `export` and `copy` take `--snapshot`: `CreateSnapshot()` makes a database snapshot, `srcDatabase` is swapped to it (so connection strings, bcp args and SMO all follow), and it is dropped in a `finally`. `Main` returns exit code 1 if any `InfoLevel.Error` message was posted.

**Resilience:** every connection comes from `SqlConnectionFactory.Create()`, which attaches SqlClient's exponential retry provider for `Open()` (`--retries`, default 3, set via `ApplyRetries` in each `Valid()`; `PoolBlockingPeriod=NeverBlock` so retries really reach the server). Commands are never retried by SqlClient. Table exports (JSON and BCP, idempotent) are additionally wrapped in `WithRetries` (transient SQL errors / any bcp failure); query exports, imports and copy loads are not. Exports write `<file>.partial` and `CommitPartial` renames on success (`DiscardPartial` on failure). `ExportData` opens an `ExportManifest` (new, or the existing one with `--resume` after `ResumeConflict`), each finished table is `Record`ed (saved atomically every time), and `Completed` is set only when the run had no errors. `TablesToExport` drops tables the manifest shows intact (size check) under `--resume`. Import skips the manifest and `.partial` files (`IsImportableFile`).

**Compression:** `export --compress none|gzip` (default none: plain files stay diffable) and `--compression-level fastest|optimal|smallest` (default fastest), helpers in `FileCompression`. JSON is gzipped while written (`FileCompression.CreateText`); BCP data is compressed after bcp finishes (`CommitData`). File names come from `ExportOptions.DataFileName`. The manifest stores `compression` (missing = none) and `ResumeConflict` refuses a different setting.

## Code layout

`BcpJson.sln` at the root; the app is `src/BcpJson/` (namespace `bcpJson`, all files flat in that folder) and the tests are `tests/BcpJson.Tests/`. `Program` is a partial class split by verb:

- `Program.cs` — `Main`, verb dispatch, `ExecuteProcess` (bcp.exe), thread-safe `PostToConsole` (+ log file, error counting).
- `Program.Export.cs` — JSON/BCP export, `TableMetadataSql` (column list + deterministic sort order), `WriteJsonRows`, bcp argument/row-count helpers.
- `Program.Import.cs` — BCP and JSON import, `ReadJsonValue` type conversion.
- `Program.Copy.cs` — server-to-server copy.
- `Program.Snapshot.cs` — `--snapshot`: `DatabaseSnapshot` (IDisposable, drops on dispose), `SnapshotName`, `SnapshotFilePath` (Windows/Linux server paths), `BuildCreateSnapshotSql`, `CreateSnapshot`.
- `Program.Sql.cs` — `TableRef`, `BuildSourceObjectIDList` (filters → T-SQL), `GetSourceTables`, `GetTableObject`, `QuoteName`/`SqlLiteral`/`LikeLiteral`.
- `SqlConnectionFactory.cs` — connection strings via `SqlConnectionStringBuilder`; `Create` (connection with retry provider; always use it instead of `new SqlConnection`); `TryOpen` for validation.
- `Program.Resilience.cs` — `WithRetries`, `IsTransientSqlError`, `RetryDelay`, `BcpFailedException`, `.partial` helpers.
- `FileCompression.cs` — gzip helpers for `--compress` and `.gz` import.
- `JsonRowReader.cs` — streaming `DbDataReader` over a JSON export, used by JSON import.
- `ExportManifest.cs` — `ExportManifest` / `ManifestTable` / `ManifestFile` (`bcpjson-manifest.json`, camelCase JSON, SHA-256 via `ManifestFile.Describe`).
- `Program.Verify.cs` — the `verify` verb.
- `SplitRequest.cs` — `--split-tables` entries (`pattern[:parts]`, parts 2-200, default 8, first match wins) and the no-`--snapshot` warning.
- `KeyRange.cs` — one key range (bounds as text, `TextLiteral()` into `CONVERT(...)`: the only way a boundary reaches SQL) and its WHERE predicate.
- `RangePlanner.cs` / `KeyInterpolation.cs` — pure range choice: histogram steps (numbers and date/time keys are interpolated inside steps, other types use step keys) or partition boundaries grouped by row count.
- `Program.Split.cs` — `TableSplit` and `PlanSplit` (reads the schema, returns ranges or a "not split" reason). The parts themselves run in `Program.Export.cs` and `Program.Copy.cs`.
- `PartFiles.cs` — `<table>.partNNNN` names, grouping files by table on import, `SelectImportFiles` (manifest-guided file choice, whole/part mix refusal), `Leftovers` (export's leftover-file warning).
- `WorkQueue.cs` — `RunLargestFirst` (parts and tables on one pool) and `TableWork` (`before` once ahead of a table's first item, `after` once after its last, also after failure).
- `LoadPlanning.cs` — pure rules: `SequentialReason` (parallel or one after another from the target's shape and the table lock) and `IndexesToDisable` / `RebuildOrder`.
- `Program.Indexes.cs` — `--disable-indexes`: read, disable and rebuild the target's indexes.
- `Program.HeapLoad.cs` / `HeapLoadScript.cs` — `--load-as-heap`: `PlanHeapLoad` (catalog facts, `LoadPlanning.DecideHeapLoad`, SMO scripts, recovery script), `PrepareHeapLoad` / `FinishHeapLoad` (`TableWork` before/after), the `bcpjson_load_as_heap_NNN` markers, script text and names.
- `SqlBulkCopyHelper.cs` — rows-copied via reflection (member name differs across SqlClient versions).
- `CustomSelect.sql` — reference copy of `TableMetadataSql`; not loaded at runtime, keep in sync.

### Export format
- JSON: `<schema>.<table>.json` (`.json.gz` with `--compress gzip`), a JSON array, one row object per line (line-diffable). Ordered by PK, else by all sortable columns. CLR types (system_type_id 240) are written with `.ToString()`; geography/geometry with a non-default SRID (not 4326 / 0) as `SRID=n;<wkt>`. Decimals go through `WriteJsonDecimal`: values that fit System.Decimal are written as before, larger ones (up to 38 digits) as `SqlDecimal` text; JSON import reads decimal columns as `SqlDecimal` (`ParseSqlDecimal`); SqlBulkCopy keeps all digits only because a non-SqlDataReader `IDataReader` returning `SqlDecimal` is treated as a SQL type (don't switch to `decimal`).
- BCP: `bcp format` (non-XML; Linux bcp has no `-x`) → `<schema>.<table>-c.fmt`, `bcp out` → `<schema>.<table>.bcp` (char mode, `|` delimiter) plus `_error.log` / `_output.log`; row counts are parsed from the output log.

### Key-range splitting

- **Request and ranges:** `--split-tables` (export, copy) picks tables; `PlanSplit` returns ranges or a reason, and a table that cannot be split is exported whole with the warning `{table}: not split: {reason}.` (heap or unpartitioned columnstore, unsupported key type, varchar code page differing from the database's, no histogram, too few distinct values, NULL partition boundary, unsafe column name, sparse column set, graph table, planning error; copy never splits CLR-spatial tables). Partitioned tables split at partition boundaries (never sub-split); others on the first column of the clustered rowstore index.
- **Files:** `<schema>.<table>.part0001.<ext>[.gz]`; BCP parts share `<table>-c.fmt` and log per part, and select the non-hidden columns in column order. Part names are what import groups by, so a real table named `...partNNNN` is misread.
- **Manifest:** the table stays one entry; its `split` object records kind (`histogram`/`partition`), column, type, descending, nullable, partitionFunction, rangeRight, partCount, boundaries (CONVERT text) and parts (rows, files, bounds; no partition numbers). `verify` reports `{table}: k of n parts exported.`. `--resume` reuses the recorded split (warning `{table}: --resume keeps its recorded split (n parts).`) and redoes only missing or damaged parts.
- **Stale files:** export never deletes; without `--resume` it warns `{table}: {file} is left from an earlier export; delete it (import ignores it while the manifest is present).` for data files of the table it will not rewrite. Import with a manifest in the folder (`PartFiles.SelectImportFiles`) skips files of a known table that its entry does not list (`{table}: skipping {file}: not part of the export in {manifest}.`) and refuses an incomplete split (`{table}: the export is incomplete (k of n parts); finish it with export --resume.`); tables the manifest does not know load as before. With or without a manifest, a whole file next to part files is refused (`{table}: both {file} and part files are present; remove the files of the older export.`). Unmatched `--split-tables` entries warn `--split-tables {pattern}: no table matched.` (export and copy).
- **Import load modes** (`LoadPlanning.SequentialReason`, logged as `{table}: n parts, loading in parallel.` / `... one after another ({reason}).`): table lock off = parallel; staged CLR columns, a clustered rowstore index, enabled nonclustered indexes or a clustered columnstore = sequential; a heap with no enabled index = parallel. Tables are truncated once before their first part. `--disable-indexes` disables nonclustered indexes except the PK and FK-referenced ones, rebuilds them in a `finally` (failures reported by name with the `ALTER INDEX` command), skips memory-optimized tables (dropping a clustered index re-enables the disabled nonclustered indexes on SQL Server 2019/2022/2025, CI probe, so disabling them first does not work with `--load-as-heap`). BCP exports are unordered, so BCP imports get no `ORDER` hint. `--batch-size 0` is one transaction per part.
- **`--load-as-heap`:** split tables only (2+ files); `DecideHeapLoad` picks heap or falls back with `not loaded as a heap: {reason}`; SMO scripts every index, the recovery script is written at planning, markers are set, all indexes dropped, parts load in parallel, missing indexes are recreated after the last part even after a failure; any import refuses a marked table; the script (UTF-8 with BOM) is rerunnable, starts with `USE` and its own `SET QUOTED_IDENTIFIER ON` etc. (no `sqlcmd -I` needed), checks `DB_NAME()` in every batch (a failed `USE` changes nothing elsewhere), keeps control characters out of its comments (`CommentText`), and drops the markers in one final batch only when every saved index exists (else `RAISERROR`); hypothetical indexes are skipped.
- **Tests:** the pure parts (`SplitRequest`, `RangePlanner`, `KeyRange`, `PartFiles`, `LoadPlanning`) have unit tests; everything against a server is proven by `roundtrip.sh split`.

## Rules for changes

- Quote identifiers with `QuoteName()` and literals with `SqlLiteral()`; parameterize values where possible. Never concatenate raw user input into SQL.
- Shared counters inside `Parallel.ForEach` must use `Interlocked`.
- Report problems with `PostToConsole(..., InfoLevel.Error)` so the exit code reflects them; catch per table/file so one failure doesn't stop the run.
- Set `CommandTimeout = 0` / `BulkCopyTimeout = 0` for data-moving commands.
- Run `dotnet test BcpJson.sln` before committing; add unit tests for new pure logic. Behaviour that touches SQL Server is only proven by the CI round-trip (export → import → compare) job.

## Remaining known issues

- bcp requires SQL passwords on the command line (`-P`).
- BCP char format with `|` delimiter can't round-trip data containing `|` or newlines; `bcp out` output is unordered.
- `sql_variant` re-imports as nvarchar through JSON.
- `copy --source-query` and export `-k` are accepted but not implemented.
- `copy` recreates keys, indexes and check constraints (after the load) and, on request, foreign keys and triggers; no views/procedures/functions/DDL triggers. Indexes land on the target's default filegroup; a partitioned source is created on the target's same-named partition scheme if there is one (its function must accept the partitioning column's type, or creation fails), otherwise unpartitioned.
- Split tables: a dominant leading-key value cannot be split and string keys may yield fewer parts than asked; a real table named `x.partNNNN` is read as a part on import; tables with a sparse column set are never split, and loading one through JSON import or copy is untested.
- Imports don't check constraints during the bulk load, so target FKs/check constraints become untrusted.
- Linux bcp lacks `-x` and `-o`; don't reintroduce them (use non-XML format files and `ExecuteProcess(..., outputLogFile)`).

## Conventions

- C# files are UTF-8 with BOM and **CRLF** line endings; keep them that way.
- `Nullable` is disabled and `ImplicitUsings` is enabled.
- Console output goes through `PostToConsole(message, InfoLevel)` — reuse it rather than `Console.WriteLine`.
- Never commit connection strings or credentials; test against a local instance.
