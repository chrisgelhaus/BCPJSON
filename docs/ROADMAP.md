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
- Remaining ideas from that review: `SqlBulkCopy` column order hints, Entra ID authentication, SQL Server 2019/2025 in CI, linux-arm64/macOS builds, SRID-preserving spatial JSON.

## Done after v2.4.0 (released in v2.5.0)

- Compression and streaming JSON import (ideas 5 and 6 of the 2026-11-21 review; options weighed and defaults chosen on 2026-11-22):
  - **`--compress none|gzip`, default none.** Plain files stay diffable, which is the point of the format, and existing scripts keep working. gzip over zstd (needs a third-party package on .NET 8) and brotli (slow at good levels) because everything can read it. JSON is compressed while written; BCP after bcp finishes (bcp only writes plain files). Format files and the manifest stay plain. The manifest records the setting and `--resume` refuses a mismatch.
  - **`--compression-level fastest|optimal|smallest`, default fastest:** most of the size reduction without slowing an export.
  - **Import** recognises `.gz` by extension (no option): JSON decompresses while streaming, BCP to the temp folder first.
  - **Streaming JSON import, no switch:** `JsonRowReader` feeds SqlBulkCopy directly instead of buffering 50,000 rows in a `DataTable`. Memory stays at one row and parsing overlaps sending. The old path had no advantage, so no option keeps it. Checked in SqlClient's source that a custom `IDataReader` returning `SqlDecimal` keeps 38 digits.
  - **`import --batch-size`, default 50,000** (as before for JSON; bcp import was 10,000), 0 = one transaction per file.
  - CI: `roundtrip.sh compression`. gzip JSON export matches the plain export after `zcat`; `.gz` import with 1,000-row batches and with `--batch-size 0`; gzip BCP export and import at `smallest`; verify on compressed exports; resume of a gzip export and refusal to resume it uncompressed. The `json` step covers the streaming import with plain files.

## TODO: extremely large tables

Decide how bcpJson should handle very large tables (hundreds of millions of rows, or files of tens to hundreds of GB). Today the table is the unit of work everywhere, so one huge table:

- is exported and imported by a single thread (parallelism is across tables), so it sets the length of the whole run;
- is redone from the start when a transient error triggers a retry, and `--resume` can only skip whole tables;
- becomes one file, which is slow to copy, hash (`verify`) and diff; BCP with `--compress gzip` also needs room for the uncompressed file;
- on a heap without a primary key is sorted by every column for the JSON export, which can need a huge sort in tempdb;
- with `import --batch-size 0` becomes one transaction, which needs log space for the whole file.

Options to weigh, with a default for each, as for compression:

- **Split by key range:** `<schema>.<table>.part0001.json` per range of the clustered key (or PK). Parts export and import in parallel, retry and resume one by one, and show up in the manifest as one table with several files. Open questions: picking ranges (statistics histogram, `NTILE`, fixed key steps), tables without a usable key, and keeping parts in key order so joined parts equal today's single file.
- **Split by size:** start a new part every N rows or N MB while streaming. Simpler, and works without a key, but parts still come from one reader, so it helps retry, resume and file size rather than speed.
- **Thresholds:** split only tables above a size such as `--split-above 10GB`, so small tables keep one file and existing exports don't change.
- Import and `copy` sides: load the parts of one table in parallel (TABLOCK into a heap allows parallel bulk loads; a clustered index does not). For copy, possibly per-range `SqlBulkCopy` streams.

## Releases

- v2.0.0 (2026-11-20): first tagged release. v2.0.1: SqlClient 7, SMO 181, SqlServer.Types 170. **v2.1.0** (2026-11-21, `c154790`): copy recreates keys and indexes; BCP import fix below. **v2.2.0** (`0a172f5`): `copy --foreign-keys`, snapshot consistency test. **v2.3.0** (2026-11-21, `03fd363`): copy recreates check constraints (default on) and triggers (`--triggers`). **v2.3.1** (2026-11-21, `5b37f35`): JSON export/import keep all 38 digits of `decimal`/`numeric` (export used to fail beyond 28-29 digits). **v2.4.0** (2026-11-21, `bb075a3`): `--retries` (connection and table-export retries), atomic `.partial` export files, `bcpjson-manifest.json` with the new `verify` verb and `export --resume`. **v2.5.0** (2026-11-22, `ac59a22`): `export --compress gzip` / `--compression-level`, streaming JSON import, `import --batch-size`.

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

- CI/testing runs on GitHub Actions (repo is public, so standard runners are free). SQL Server for integration tests = service container (`mcr.microsoft.com/mssql/server:2022-latest`).
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
6. ~~Branch protection~~ — done (classic protection on `master`): the three CI checks are required (pinned to the GitHub Actions app), branch must be up to date before merging, no force-push or deletion. No required reviews and admins may bypass (single maintainer). If a CI job is renamed, update the required check names too or merges will block.
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
