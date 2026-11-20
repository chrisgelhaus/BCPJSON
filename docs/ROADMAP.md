# Roadmap and working notes

Handoff notes so any Claude session (chat, web or terminal) can pick up where the last one stopped.

## Done (branch `refactor/fixes-and-json-import`, 2026-11-20)

- Split `Program.cs` into `Program.*.cs` partial files per verb; removed dead code.
- Fixed: password option names (trailing space), SQL-login support in bcp/SMO paths, thread-safe row totals, identifier quoting/escaping, 30 s command timeouts on exports, broken `SqlBulkCopyHelper`, copy of computed/rowversion/identity columns, PK sort order, `--log-file` and `--source-query` (export) now implemented.
- New: JSON import, `--trust-server-certificate`, exit code 1 on any error, export with no include filter = all tables.
- Removed packages: DotNetZip (unused), System.Data.SqlClient (consolidated on Microsoft.Data.SqlClient). sqlcmd.exe no longer required.
- README rewritten; CLAUDE.md added.
- **Not yet compiled** — the refactor was written without a .NET SDK. First task: `dotnet build`, fix any errors, then a manual export → import → export round trip.

## Decisions

- CI/testing runs on GitHub Actions (repo is public, so standard runners are free). SQL Server for integration tests = service container (`mcr.microsoft.com/mssql/server:2022-latest`).
- Releases are published as GitHub Releases from version tags.

## Next steps (in order)

1. **Build and smoke test** on the dev machine (`dotnet build -c Release`).
2. **Repo hygiene:** add `.gitattributes` (`*.cs text eol=crlf`, etc.) and renormalize, which clears the line-ending-only diffs in `.github/workflows/dotnet.yml` and `.vscode/*`. Add a LICENSE (MIT or Apache-2.0, owner's choice).
3. **Restructure for tests:** move the app to `src/BcpJson/`, add `tests/BcpJson.Tests` (xUnit) and a `.sln`; `InternalsVisibleTo` for helpers.
4. **Unit tests (no DB):** `BuildSourceObjectIDList` (literals, wildcards, escaping, "no include = all"), `QuoteName`/`SqlLiteral`/`LikeLiteral`, `ReadJsonValue` for every SQL type, `SqlConnectionFactory.Build`, CLI parsing.
5. **CI workflow** (`.github/workflows/ci.yml`): .NET 8, `actions/checkout@v4`, `actions/setup-dotnet@v4`, build Release with warnings as errors, unit tests; integration job with SQL Server 2022 service container + mssql-tools18 that creates a sample DB with awkward types, runs export(json) → import → export, and diffs the two exports. Change `bcp.exe` → `bcp` so bcp paths run on Linux too.
6. **Branch protection** on `master`: require CI to pass.
7. **Release workflow** (`.github/workflows/release.yml`) on `v*` tags: tag-driven version (MinVer or `-p:Version`), `dotnet publish` single-file for win-x64, win-arm64 (+ linux-x64 once bcp is cross-platform), zip + SHA-256 checksums, `actions/attest-build-provenance`, GitHub Release with generated notes.
8. **Security/maintenance:** Dependabot for `nuget` and `github-actions`, CodeQL for C#, secret scanning + push protection. Then upgrade Microsoft.Data.SqlClient and SMO under test.
9. **Later:** winget manifest, `dotnet tool` package on NuGet.org, GHCR container image, Windows code signing (Azure Trusted Signing).
