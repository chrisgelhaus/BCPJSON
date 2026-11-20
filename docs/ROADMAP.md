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
- **Round trip not yet run.** The dev machine has no SQL Server, bcp or Docker, and none will be installed: the CI integration job (step 5) is the first round trip.

## Decisions

- CI/testing runs on GitHub Actions (repo is public, so standard runners are free). SQL Server for integration tests = service container (`mcr.microsoft.com/mssql/server:2022-latest`).
- Releases are published as GitHub Releases from version tags.
- No local SQL Server on the dev machine; all database testing happens in GitHub Actions.

## Next steps (in order)

1. ~~Smoke test locally~~ — dropped (no local SQL Server); covered by the CI integration job in step 5.
2. ~~Repo hygiene~~ — done: `.gitattributes`, Apache-2.0 `LICENSE` + `NOTICE`.
3. ~~Restructure for tests~~ — done: app in `src/BcpJson/` (exe still `bcpjson`), `tests/BcpJson.Tests` (xUnit 2.9), `BcpJson.sln`, `InternalsVisibleTo`.
4. ~~Unit tests (no DB)~~ — done: 68 tests for `BuildSourceObjectIDList`, `QuoteName`/`SqlLiteral`/`LikeLiteral`/`TableRef`, `ReadJsonValue` (every CLR type, NULLs, precision, offsets, invariant culture), `SqlConnectionFactory.Build`, CLI parsing for all verbs.
5. **CI workflow** (`.github/workflows/ci.yml`): .NET 8, `actions/checkout@v4`, `actions/setup-dotnet@v4`, build Release with warnings as errors, unit tests; integration job with SQL Server 2022 service container + mssql-tools18 that creates a sample DB with awkward types, runs export(json) → import → export, and diffs the two exports. Change `bcp.exe` → `bcp` so bcp paths run on Linux too.
6. **Branch protection** on `master`: require CI to pass.
7. **Release workflow** (`.github/workflows/release.yml`) on `v*` tags: tag-driven version (MinVer or `-p:Version`), `dotnet publish` single-file for win-x64, win-arm64 (+ linux-x64 once bcp is cross-platform), zip + SHA-256 checksums, `actions/attest-build-provenance`, GitHub Release with generated notes.
8. **Security/maintenance:** Dependabot for `nuget` and `github-actions`, CodeQL for C#, secret scanning + push protection. Then upgrade Microsoft.Data.SqlClient and SMO under test.
9. **Later:** winget manifest, `dotnet tool` package on NuGet.org, GHCR container image, Windows code signing (Azure Trusted Signing).
