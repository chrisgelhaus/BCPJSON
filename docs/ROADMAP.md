# Roadmap and working notes

Handoff notes so any Claude session (chat, web or terminal) can pick up where the last one stopped.

## Done (branch `refactor/fixes-and-json-import`, 2026-11-20)

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

## Decisions

- CI/testing runs on GitHub Actions (repo is public, so standard runners are free). SQL Server for integration tests = service container (`mcr.microsoft.com/mssql/server:2022-latest`).
- Releases are published as GitHub Releases from version tags.
- No local SQL Server on the dev machine; all database testing happens in GitHub Actions.

## Next steps (in order)

1. ~~Smoke test locally~~ — dropped (no local SQL Server); covered by the CI integration job in step 5.
2. ~~Repo hygiene~~ — done: `.gitattributes`, Apache-2.0 `LICENSE` + `NOTICE`.
3. ~~Restructure for tests~~ — done: app in `src/BcpJson/` (exe still `bcpjson`), `tests/BcpJson.Tests` (xUnit 2.9), `BcpJson.sln`, `InternalsVisibleTo`.
4. ~~Unit tests (no DB)~~ — done: 68 tests for `BuildSourceObjectIDList`, `QuoteName`/`SqlLiteral`/`LikeLiteral`/`TableRef`, `ReadJsonValue` (every CLR type, NULLs, precision, offsets, invariant culture), `SqlConnectionFactory.Build`, CLI parsing for all verbs.
5. ~~CI workflow~~ — done and green: `.github/workflows/ci.yml` (build + 77 unit tests on ubuntu and windows; integration job with SQL Server 2022 container + mssql-tools18 running `tests/integration/roundtrip.sh`). Code calls `bcp` instead of `bcp.exe`.
   - Follow-ups: bump actions off Node 20 (`checkout`/`setup-dotnet`/`upload-artifact` v4 warn); `decimal` values beyond 28-29 significant digits are untested (likely overflow in SqlClient); copy does not create primary keys/indexes on the target.
6. **Branch protection** on `master`: require CI to pass.
7. **Release workflow** (`.github/workflows/release.yml`) on `v*` tags: tag-driven version (MinVer or `-p:Version`), `dotnet publish` single-file for win-x64, win-arm64 (+ linux-x64: bcp paths now verified on Linux), zip + SHA-256 checksums, `actions/attest-build-provenance`, GitHub Release with generated notes.
8. **Security/maintenance:** Dependabot for `nuget` and `github-actions`, CodeQL for C#, secret scanning + push protection. Then upgrade Microsoft.Data.SqlClient and SMO under test.
9. **Later:** winget manifest, `dotnet tool` package on NuGet.org, GHCR container image, Windows code signing (Azure Trusted Signing).
