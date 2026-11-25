# Roadmap and working notes

Handoff notes so any Claude session (chat, web or terminal) can pick up where the last one stopped.

## Done (branch `refactor/fixes-and-json-import`, merged to master via PR #13 on 2026-11-20)

- Split `Program.cs` into `Program.*.cs` partial files per verb; removed dead code.
- Fixed: password option names (trailing space), SQL-login support in bcp/SMO paths, thread-safe row totals, identifier quoting/escaping, 30 s command timeouts on exports, broken `SqlBulkCopyHelper`, copy of computed/rowversion/identity columns, PK sort order, `--log-file` and `--source-query` (export) now implemented.
- New: JSON import, `--trust-server-certificate`, exit code 1 on any error, export with no include filter = all tables.
- Removed packages: DotNetZip (unused), System.Data.SqlClient (consolidated on Microsoft.Data.SqlClient). sqlcmd.exe no longer required.
- README rewritten; CLAUDE.md added.
- `dotnet build -c Release` verified on the dev machine (SDK 8.0.134): 0 warnings, 0 errors; `export --help` runs.
- `.gitattributes` added and line endings renormalized (index LF; `.cs`/`.md`/`.json`/etc. CRLF on checkout, `.yml`/`.gitignore` LF).
- **Round trips verified in CI** (PR #13, run 44819751435): JSON (7/7 files identical, 120,019 rows), BCP (3/3 `plain` files identical) and copy (7/7 identical ignoring row order) against SQL Server 2022 on Linux.
- Fixes found by CI: Linux bcp has no `-x` (exports now write non-XML `-c.fmt`; import still accepts old `-c.xml`) and no `-o` (bcp stdout is captured in-process into the same `_output.log`).
- Merged master (PRs #6–#12, Codex): already covered by the refactor except `--max-tasks` for export/import, which was ported via `Parallelism()`.

## Done after v2.1.0 (released in v2.2.0 and v2.3.0)

- Snapshot consistency test (`roundtrip.sh consistency`, CI step "Snapshot consistency under concurrent writes"): a writer (`writer.sql`) moves value between `inv.A` and `inv.B` in `BcpJsonLive` (`consistency.sql`); `export --snapshot` must add up exactly while transfers commit, and an export without `--snapshot` must drift (control).
- `copy --foreign-keys` (opt-in): after all tables are copied, recreates the FKs of created tables with SMO (WITH CHECK when trusted in the source; disabled/untrusted state mirrored), one at a time. `--drop-target` first drops FKs owned by the tables being recreated, so re-runs work. Rejected with `--no-indexes`; warns without `--snapshot`. CI: one FK of every kind in the test schema, FK definitions compared (`foreign_keys.sql`), plus a `--drop-target` re-run. Released in v2.2.0.
- Check constraints (default on, `--no-check-constraints`) and triggers (opt-in `--triggers`, created last so they never fire on copied rows; enabled state and FIRST/LAST order applied explicitly). CI: one of each kind in the schema, definitions compared via `check_constraints.sql` / `triggers.sql` (generic `compare_definitions` in roundtrip.sh), and `rt.ChildAudit` proves no trigger fired during copy or import. Released in v2.3.0.

## Done after v2.3.0 (released in v2.3.1)

- 38-digit decimals: JSON export used to fail (`OverflowException`) on `decimal`/`numeric` values beyond System.Decimal (28-29 significant digits or scale > 28). Values that fit are written exactly as before (existing exports unchanged); larger ones are written from `SqlDecimal` text. JSON import buffers decimal columns as `SqlDecimal` and parses the number text, so all digits survive. CI: `plain.Decimals` (max/min of decimal(38,0), (38,10), (38,19), (38,38), (29,2), the 2^96 boundary) round-trips through JSON, BCP and copy, checked with a hash of SQL Server's own text (`compare_decimals`), not only via bcpjson exports.

## Done after v2.3.1 (released in v2.4.0)

- Resilience (improvements 1-3 from the 2026-11-21 review):
  - **Retries:** `--retries` (default 3) on every verb. Connection opens retry transient errors via SqlClient's exponential retry provider (`SqlConnectionFactory.Create`; `PoolBlockingPeriod=NeverBlock`). Table exports (JSON and BCP) are retried as a whole; query exports, import and copy loads are not (not idempotent).
  - **Atomic files:** exports write `<file>.partial` and rename when complete; import ignores `.partial` files.
  - **Manifest + verify + resume:** `bcpjson-manifest.json` per export (tool, format, server, database, snapshot, start/completion, per-table rows and file size/SHA-256), saved after every table. New `verify` verb checks a folder against it without a database. `export --resume` skips tables already intact (same format/database/query required).
  - CI: `roundtrip.sh resilience` checks the manifest, catches a same-size edit and a missing file with `verify`, resumes a simulated crash (skipped files untouched, no `.partial` left, data identical), refuses a mismatched resume, and proves connection retry by renaming a database into place 20 s after the export starts (SqlClient's own single login retry after 10 s is not enough) (control: `--retries 0` fails). BCP and snapshot exports are verified too.
- The remaining ideas from that review (order hints, Arm64/macOS builds, SRIDs in JSON) are done: see "Done after v2.5.1".

## Done after v2.5.1 (released in v2.6.0)

- **SRIDs in JSON.** geography/geometry with a non-default SRID are exported as `SRID=n;<wkt>` (the PostGIS convention) and imported with `STGeomFromText(wkt, n)`. Default SRIDs (4326 / 0) are written as before, so older exports still diff cleanly and import unchanged. CI data has SRIDs 4269, 4258, 27700 and 3857, Z and M values.
- **Order hints for JSON import.** The manifest records the primary key each JSON table export is sorted by (`orderedBy`). Import adds `SqlBulkCopy.ColumnOrderHints` when the target's clustered key is identical (columns, types, directions, collations), so SQL Server skips the sort. Not for `copy` (it loads heaps and builds indexes afterwards) or the CLR staging path. SQL Server rejects a load that breaks the hint, hence the strict match; sql_variant keys never match. CI: `rt.OrderedKey` (case-sensitive collation, descending column) must get the hint; a target clustered on another column must not.
- **linux-arm64 and osx-arm64 release packages**, built and smoke-tested on `ubuntu-24.04-arm` and `macos-latest`. No database tests there (SQL Server containers are x64-only). The macOS binary is ad-hoc signed by the SDK but not notarized (signing is deferred), so the README explains removing the quarantine flag. No osx-x64: GitHub is retiring Intel macOS runners.

## Done after v2.5.0 (released in v2.5.1)

- CI integration job runs on SQL Server 2019, 2022 and 2025 (matrix over `mcr.microsoft.com/mssql/server:<version>-latest`; first run: 2019 CU32, 2022 CU28, 2025 CU11). The job name for 2022 is unchanged, so the existing required check still applies.
- Found by 2019: `copy --drop-target` dropped target tables in parallel, the drops deadlocked and the victims were not copied. Target tables are now dropped one at a time (`DropTargetTables`) before the parallel copy.

## Done after v2.4.0 (released in v2.5.0)

- Compression and streaming JSON import (ideas 5 and 6 of the 2026-11-21 review; options weighed and defaults chosen on 2026-11-22):
  - **`--compress none|gzip`, default none.** Plain files stay diffable, which is the point of the format, and existing scripts keep working. gzip over zstd (needs a third-party package on .NET 8) and brotli (slow at good levels) because everything can read it. JSON is compressed while written; BCP after bcp finishes (bcp only writes plain files). Format files and the manifest stay plain. The manifest records the setting and `--resume` refuses a mismatch.
  - **`--compression-level fastest|optimal|smallest`, default fastest:** most of the size reduction without slowing an export.
  - **Import** recognises `.gz` by extension (no option): JSON decompresses while streaming, BCP to the temp folder first.
  - **Streaming JSON import, no switch:** `JsonRowReader` feeds SqlBulkCopy directly instead of buffering 50,000 rows in a `DataTable`. Memory stays at one row and parsing overlaps sending. The old path had no advantage, so no option keeps it. Checked in SqlClient's source that a custom `IDataReader` returning `SqlDecimal` keeps 38 digits.
  - **`import --batch-size`, default 50,000** (as before for JSON; bcp import was 10,000), 0 = one transaction per file.
  - CI: `roundtrip.sh compression`. gzip JSON export matches the plain export after `zcat`; `.gz` import with 1,000-row batches and with `--batch-size 0`; gzip BCP export and import at `smallest`; verify on compressed exports; resume of a gzip export and refusal to resume it uncompressed. The `json` step covers the streaming import with plain files.

## Done after v2.6.0 (PR #45, key-range splitting of large tables; released in v2.7.0)

Design: `docs/superpowers/specs/2026-11-23-key-range-splitting-design.md` (with an "As built" section). Documented in the README, "Splitting large tables".

- **`--split-tables pattern[:parts]`** on `export` (JSON and BCP) and `copy`: chosen tables are cut into 2-200 key ranges (default 8) handled as parallel tasks; unnamed tables produce exactly the old files. Ranges come from partition boundaries (partitioned tables) or the clustered key's statistics histogram, interpolated inside steps for number and date/time keys. Tables that cannot be split are exported whole with a warning.
- **Parts:** `<schema>.<table>.part0001.<ext>[.gz]`; the manifest keeps one entry per table with a `split` object; `verify` reports missing parts; `--resume` and retries work part by part.
- **Import:** parts load in parallel or one after another depending on the target (`LoadPlanning`); **`--disable-indexes`** disables nonclustered indexes (not the PK or FK-referenced ones), rebuilds them afterwards even after a failed load.
- **Copy:** parallel source-to-target range streams; copy now creates a partitioned table on a same-named partition scheme when the target has one.
- **CI:** `roundtrip.sh split`, a lock probe that measured the lock modes of concurrent TABLOCK loads (heap BU, clustered rowstore Sch-M/X, clustered columnstore S+X), and a manual benchmark workflow (`benchmark.yml`).

## Done after v2.7.0 (`import --load-as-heap`, PR #48; released in v2.8.0)

Design: `docs/superpowers/specs/2026-11-23-load-as-heap-design.md` (with an "As built" section). Documented in the README, "Splitting large tables".

- **`import --load-as-heap`** (JSON and BCP, split tables only): drops every index of a table with a clustered index, loads the parts in parallel into the heap, then recreates the clustered index and the others, also after a failed load. Tables that do not qualify fall back to `--disable-indexes` with `not loaded as a heap: <reason>`. Benchmark (5,000,000 rows): 53.9 s indexed, 30.1 s `--disable-indexes`, 20.4 s heap load plus index builds.
- **Why every index is dropped:** the first design disabled the nonclustered indexes and dropped only the clustered one. A CI probe (run 45540422748) showed that dropping a clustered index rebuilds and re-enables disabled nonclustered indexes on SQL Server 2019, 2022 and 2025, so the owner chose to drop and recreate all of them.
- **Safety net:** a rerunnable recovery script `<schema>.<table>.load-as-heap.sql` in `--source-path` (written at planning) and one `bcpjson_load_as_heap_NNN` extended property per index on the table; every import refuses a marked table. Both are removed when all indexes exist again. The script sets its own session options and drops the markers only when every index exists.
- **CI:** `roundtrip.sh split` cases (a) to (i) on SQL Server 2019, 2022 and 2025: index definitions identical before and after (`heap_indexes.sql`), BCP parts, a failed load, duplicate keys in the clustered and in a unique index, `--target-table`, an unwritable folder, a leftover script, and the recovery script run by sqlcmd (markers kept while the key fails, removed once it succeeds).
- **Follow-ups after v2.8.0 (PR #49, released in v2.8.1):** the recovery script checks `DB_NAME()` in every batch and says so when it runs in another database (renamed or restored target); control characters in names no longer reach its comments; README recommends `sqlcmd -x`. CI now proves the script's own `SET QUOTED_IDENTIFIER ON` (a filtered index recreated by sqlcmd without `-I`), the wrong-database refusal, and that a hypothetical index is skipped. `benchmark.sh` has the row `import indexed, --load-as-heap`; it has not been run (the workflow runs only from master).
- **Later (still in the backlog):** import creating missing tables, `copy --load-as-heap`, dropping foreign keys of referencing tables, clustered columnstore targets.

## Backlog: large tables, later

From the design's "Later" list:

- **Staging tables for large history tables:** load parts into staging tables and move them in over time. First step: **partition switching** (one indexed staging table per partition, loaded in parallel, then `SWITCH`).
- **Partition-scoped work:** `TRUNCATE TABLE ... WITH (PARTITIONS (...))`, and exporting only chosen partitions (incremental backups of history tables).
- **Sub-splitting a dominant partition**, and copying partition functions, schemes and filegroups to the target.
- **Option 2** (load into a heap, then create the clustered index): **done, see "Done after v2.7.0" above** (`import --load-as-heap`). Benchmark run 45326185184 (2026-11-24, 5,000,000 rows, bigint identity clustered PK + two nonclustered indexes, 8 JSON parts, SQL Server 2022, 4 cores):

  | Step | Seconds | Rows/s |
  |---|---:|---:|
  | Export JSON, unsplit / 8 parts | 8.4 / 6.3 | 595k / 794k |
  | Export BCP, unsplit / 8 parts | 8.3 / 5.4 | 602k / 926k |
  | Import into the indexed table (parts one after another) | 53.9 | 93k |
  | Import with `--disable-indexes` | 30.1 | 166k |
  | Import into a bare heap (parallel) | 12.3 | 407k |
  | Option 2: heap load 12.9 + clustered PK 3.2 + two nonclustered 1.7 + 2.6 | 20.4 | 245k |

  Option 2 is 33% faster than `--disable-indexes` and 2.6x a plain load; at 1,000,000 rows the two were even (5.0 s against 4.8 s). One run on a small runner with presorted keys: the ratios are indicative.
- **Later, from the load-as-heap design:** import creating missing tables from table definitions saved by export (such tables would then load heap first, like `copy`); `copy --load-as-heap` and `copy --disable-indexes` for existing targets; dropping and recreating the foreign keys of referencing tables (rejected 2026-11-23); clustered columnstore targets.
- `--split-above` (automatic size threshold), per-table `--disable-indexes`, and `--disable-indexes` for `copy` into existing tables.
- **Multi-column ranges** for skewed leading keys (one dominant value), and splitting unpartitioned heaps.
- Splitting by row count, if a use case appears.

## Backlog: Entra ID authentication

Deferred on 2026-11-22. Sign-in with Microsoft Entra ID for Azure SQL Database, Managed Instance and Entra-enabled SQL Server. Findings so far:

- Today every verb chooses SQL auth (login given) or Windows auth (none); the same choice reaches the connection string (`SqlConnectionFactory.Build`), bcp (`BcpConnectionArgs`: `-U/-P` or `-T`) and SMO in `copy` (built on our `SqlConnection`).
- SqlClient 7 no longer contains Entra sign-in: it needs the extra package `Microsoft.Data.SqlClient.Extensions.Azure` (Azure.Identity/MSAL), which makes the single-file executables larger (size not measured yet).
- bcp's `-G` covers only Entra password and integrated sign-in, not `az login`/default credentials, managed identity or service principals. Those methods would work for JSON export/import and copy's SqlBulkCopy loads, but BCP export/import and copy's bcp fallback (geography/geometry/hierarchyid tables) would need a clear error.
- Entra password sign-in is being retired (mandatory MFA for user accounts).
- Likely shape: `--source-authentication` / `--target-authentication` with SqlClient's methods (default: today's behaviour), and ActiveDirectoryDefault as the recommended Entra method.
- Open decision: CI cannot test Entra against the SQL Server container. Either unit tests only (connection strings, bcp arguments), or an Azure SQL database (free offer) reached from GitHub Actions through OIDC federation, which needs one-time setup in Azure.

## Releases

- v2.0.0 (2026-11-20): first tagged release. v2.0.1: SqlClient 7, SMO 181, SqlServer.Types 170. **v2.1.0** (2026-11-21, `c154790`): copy recreates keys and indexes; BCP import fix below. **v2.2.0** (`0a172f5`): `copy --foreign-keys`, snapshot consistency test. **v2.3.0** (2026-11-21, `03fd363`): copy recreates check constraints (default on) and triggers (`--triggers`). **v2.3.1** (2026-11-21, `5b37f35`): JSON export/import keep all 38 digits of `decimal`/`numeric` (export used to fail beyond 28-29 digits). **v2.4.0** (2026-11-21, `bb075a3`): `--retries` (connection and table-export retries), atomic `.partial` export files, `bcpjson-manifest.json` with the new `verify` verb and `export --resume`. **v2.5.0** (2026-11-22, `ac59a22`): `export --compress gzip` / `--compression-level`, streaming JSON import, `import --batch-size`. **v2.5.1** (2026-11-22, `696caa1`): CI round trips on SQL Server 2019, 2022 and 2025 (all required); `copy --drop-target` drops target tables one at a time (parallel drops deadlocked on 2019). **v2.6.0** (2026-11-23, `7cce4ab`): SRIDs kept in JSON (`SRID=n;<wkt>`), column order hints for JSON import, linux-arm64 and osx-arm64 packages. **v2.7.0** (2026-11-23, `383dc74`): `--split-tables` (key-range splitting for export, import and copy, partition-aware), `import --disable-indexes`, manifest-guided import of part files. **v2.8.0** (2026-11-25, `54ba6ef`): `import --load-as-heap` (split tables load in parallel into a heap, indexes recreated afterwards, recovery script and markers). **v2.8.1** (2026-11-25, `d12804c`): the load-as-heap recovery script checks `DB_NAME()` in every batch and refuses to run in another database, control characters stay out of its comments; CI proves its `SET` options, the wrong-database refusal and that hypothetical indexes are skipped.

## Done in v2.1.0 (PR #28)

- **BCP import fix:** `bcp in` now runs with `-q` (QUOTED_IDENTIFIER ON); before, imports into tables with filtered/XML/spatial indexes failed. With `-q` bcp needs the table unbracketed as `schema.table` (names with `.` or `"` fall back to brackets without `-q`). Linux exports no longer leave a stray `nul` file.
- System-named constraints (declared without a name) get a new system name in the copy; explicitly named ones keep their names.
- `copy` recreates primary keys, unique constraints and all index kinds on tables it creates, after the load (SMO scripting without filegroups, clustered first, XML/spatial after the PK). On by default; `--no-indexes` opts out. CI adds one index of every kind to the test schema, compares index definitions source vs copy (`tests/integration/indexes.sql`) and now compares copied data exactly (same order thanks to the PKs).
- Not copied in v2.1.0: foreign keys, check constraints, triggers (added in v2.2.0 and v2.3.0).

## Done (branch `feature/tablock-and-snapshot`)

- TABLOCK by default for JSON import, BCP import (`-h "TABLOCK"`) and copy (previously only JSON import); `--no-table-lock` on import and copy for live targets or several loads into one clustered table.
- `--snapshot` on export and copy: read every table from a database snapshot so a long run is one point in time (needs CREATE DATABASE, SQL Server 2016 SP1+, not Azure SQL DB). CI covers JSON/BCP export and copy with it and checks no snapshot is left behind.

## Decisions

- Loads take a table lock by default (fastest; minimal logging); opting out is explicit.
- Point-in-time exports are opt-in (`--snapshot`) because they need CREATE DATABASE permission and are unavailable on Azure SQL DB.

- CI/testing runs on GitHub Actions (repo is public, so standard runners are free). SQL Server for integration tests = service containers (`mcr.microsoft.com/mssql/server:2019-latest`, `2022-latest`, `2025-latest`).
- Releases are published as GitHub Releases from version tags.
- No local SQL Server on the dev machine; all database testing happens in GitHub Actions.
- All changes reach `master` through a PR with green CI (branch protection, step 6).

## Next steps (in order)

1. ~~Smoke test locally~~ — dropped (no local SQL Server); covered by the CI integration job in step 5.
2. ~~Repo hygiene~~ — done: `.gitattributes`, Apache-2.0 `LICENSE` + `NOTICE`.
3. ~~Restructure for tests~~ — done: app in `src/BcpJson/` (exe still `bcpjson`), `tests/BcpJson.Tests` (xUnit 2.9), `BcpJson.sln`, `InternalsVisibleTo`.
4. ~~Unit tests (no DB)~~ — done: 68 tests for `BuildSourceObjectIDList`, `QuoteName`/`SqlLiteral`/`LikeLiteral`/`TableRef`, `ReadJsonValue` (every CLR type, NULLs, precision, offsets, invariant culture), `SqlConnectionFactory.Build`, CLI parsing for all verbs.
5. ~~CI workflow~~ — done and green: `.github/workflows/ci.yml` (build + 77 unit tests on ubuntu and windows; integration job with SQL Server 2022 container + mssql-tools18 running `tests/integration/roundtrip.sh`). Code calls `bcp` instead of `bcp.exe`.
   - Follow-ups (all done): actions bumped off Node 20 (step 8); decimals beyond 28-29 significant digits (after v2.3.0); copy creates primary keys/indexes (v2.1.0).
6. ~~Branch protection~~ — done (classic protection on `master`): the five CI checks are required (two builds plus the round trips on SQL Server 2019, 2022 and 2025; pinned to the GitHub Actions app), branch must be up to date before merging, no force-push or deletion. No required reviews and admins may bypass (single maintainer). If a CI job is renamed, update the required check names too or merges will block.
7. ~~Release workflow~~ — done: `.github/workflows/release.yml` on `v*` tags on master. Reruns CI; self-contained single-file executables for win-x64, win-arm64 and linux-x64, each smoke-tested on its own OS; zip/tar.gz + `SHA256SUMS` + build-provenance attestation; GitHub Release with generated notes (pre-release for tags containing `-`). A manual run is a dry run. CI's integration job now tests the published linux-x64 single-file executable.
   - **v2.0.0 released 2026-11-20** (tag on `f956680`): https://github.com/chrisgelhaus/BCPJSON/releases/tag/v2.0.0. Download, `SHA256SUMS` and `gh attestation verify` checked after publishing.
   - Fixed on the way: `--help`/`--version` exited 1 and wrote to stderr; they now exit 0 and write to stdout (usage errors: stderr, exit 1). The release smoke test enforces this.
8. ~~Security/maintenance~~ — done:
   - Repo settings (via API, not in git): Dependabot alerts + security updates (already on), secret scanning + push protection, private vulnerability reporting, CodeQL **default setup** for `csharp` and `actions` with the `extended` query suite. Non-provider patterns and validity checks need paid Secret Protection on personal repos, so they stay off.
   - `.github/dependabot.yml`: weekly NuGet (minor/patch grouped, majors separate) and GitHub Actions (grouped) updates; `SECURITY.md` with private reporting instructions and usage cautions.
   - First CodeQL scan (63 C# + 23 Actions rules): 0 findings.
   - All dependencies upgraded through PRs with green CI (#20–#26): GitHub Actions (checkout v7, setup-dotnet v6, upload-artifact v7, download-artifact v8, attest-build-provenance v4; release workflow dry-run tested), **Microsoft.Data.SqlClient 5.1.3 → 7.1.2 together with SMO 170.18 → 181.46**, Microsoft.SqlServer.Types 160 → 170, Newtonsoft.Json 13.0.4, Test SDK 18, xunit runner 4, coverlet 10. SQL Server round trips identical throughout; `SqlBulkCopyHelper` now has a unit test so a future SqlClient rename of the rows-copied counter fails the build.
   - Merging several Dependabot PRs: branch protection requires each to be up to date, so merge one at a time and let CI rerun on the updated branch (`gh pr update-branch`); Dependabot also rebases its PRs on its own.
   - Released in v2.0.1.
9. **Later:** winget manifest, `dotnet tool` package on NuGet.org, GHCR container image, Windows code signing (Azure Trusted Signing).
