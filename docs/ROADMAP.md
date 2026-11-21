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

## Done after v2.1.0

- Snapshot consistency test (`roundtrip.sh consistency`, CI step "Snapshot consistency under concurrent writes"): a writer (`writer.sql`) moves value between `inv.A` and `inv.B` in `BcpJsonLive` (`consistency.sql`); `export --snapshot` must add up exactly while transfers commit, and an export without `--snapshot` must drift (control).

## Releases

- v2.0.0 (2026-11-20): first tagged release. v2.0.1: SqlClient 7, SMO 181, SqlServer.Types 170. **v2.1.0** (2026-11-21, `c154790`): copy recreates keys and indexes; BCP import fix below.

## Done in v2.1.0 (PR #28)

- **BCP import fix:** `bcp in` now runs with `-q` (QUOTED_IDENTIFIER ON); before, imports into tables with filtered/XML/spatial indexes failed. With `-q` bcp needs the table unbracketed as `schema.table` (names with `.` or `"` fall back to brackets without `-q`). Linux exports no longer leave a stray `nul` file.
- System-named constraints (declared without a name) get a new system name in the copy; explicitly named ones keep their names.
- `copy` recreates primary keys, unique constraints and all index kinds on tables it creates, after the load (SMO scripting without filegroups, clustered first, XML/spatial after the PK). On by default; `--no-indexes` opts out. CI adds one index of every kind to the test schema, compares index definitions source vs copy (`tests/integration/indexes.sql`) and now compares copied data exactly (same order thanks to the PKs).
- Not copied (possible future options): foreign keys, check constraints, triggers.

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
   - Follow-ups: bump actions off Node 20 (`checkout`/`setup-dotnet`/`upload-artifact` v4 warn); `decimal` values beyond 28-29 significant digits are untested (likely overflow in SqlClient); copy does not create primary keys/indexes on the target.
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
   - **Not yet released:** these upgrades are on master but not in v2.0.0. Tag v2.0.1 (or v2.1.0) when ready.
9. **Later:** winget manifest, `dotnet tool` package on NuGet.org, GHCR container image, Windows code signing (Azure Trusted Signing).
