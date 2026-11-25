# Contributing to bcpJson

## Build from source

You need the [.NET 8 SDK](https://dotnet.microsoft.com/download).

```powershell
git clone https://github.com/chrisgelhaus/BCPJSON.git
cd BCPJSON
dotnet build -c Release
dotnet test                                           # unit tests, no database needed
dotnet publish src/BcpJson -c Release -r win-x64 -o publish   # self-contained single-file publish\bcpjson.exe
```

Use `-r win-arm64`, `-r linux-x64`, `-r linux-arm64` or `-r osx-arm64` for the other platforms.

## Tests

- **Unit tests:** `dotnet test BcpJson.sln` runs the xUnit tests in `tests/BcpJson.Tests`. They need no database. `DocsTests` also checks the user docs:
  - every option has a row on its verb's page in `docs/reference`;
  - every `--option` written in the docs exists;
  - relative links and their anchors resolve;
  - the user docs link only to files that ship in the release package.
- **Round trips:** `tests/integration/roundtrip.sh` exports, imports and copies a test database, then compares JSON exports after each round trip. It runs in CI against SQL Server 2019, 2022 and 2025. To run it against any SQL Server, set `SQL_SERVER`, `SQL_PASSWORD` and `BCPJSON`. When fixing a data-fidelity bug, add the awkward data there.
- **Pull requests:** `master` is protected. Work on a branch and merge through a pull request once the required checks pass: two builds and the three round trips.

## Documentation

- **Where it lives:**
  - `docs/guides`: one page per job a DBA wants to do;
  - `docs/reference`: one page per verb, plus common concepts, the manifest, the JSON format and the known limitations.
- **New options:** a new option needs a row on its verb's reference page.
- **Links:** pages shipped in the release package link to other repository files (this file, `SECURITY.md`, `docs/ROADMAP.md`) by full GitHub URL, so the links also work in an unpacked package.

## Releasing

Releases are built by [`.github/workflows/release.yml`](.github/workflows/release.yml) from a version tag on `master`:

```powershell
git checkout master; git pull
git tag v2.1.0            # or v2.1.0-beta.1 for a pre-release
git push origin v2.1.0
```

The workflow:
1. checks that the tag is on `master`;
2. reruns the full CI suite (unit tests and the SQL Server round trips);
3. publishes and smoke-tests the executable for each platform, on that platform;
4. creates the GitHub Release with generated notes, the packages, `SHA256SUMS` and a signed build-provenance attestation.

The version shown by `bcpjson --version` comes from the tag. Each package carries the README and the `docs/guides` and `docs/reference` folders.

To try the workflow without releasing anything, run it manually (*Actions → Release → Run workflow*). Everything is built and tested, and the packages are attached to the run as artifacts, but no release is created.

## Project layout

The solution `BcpJson.sln` holds the app in `src/BcpJson/` and the xUnit tests in `tests/BcpJson.Tests/`. Files in `src/BcpJson/`:

| File | Contents |
|---|---|
| `Program.cs` | Entry point, verb dispatch, console/log output, external process runner |
| `Program.Export.cs` | JSON and BCP export, per-table metadata query |
| `Program.Import.cs` | JSON (SqlBulkCopy) and BCP import |
| `JsonRowReader.cs` | Streams a JSON export into SqlBulkCopy as a data reader |
| `FileCompression.cs` | gzip reading and writing for `--compress` |
| `Program.Copy.cs` | Server-to-server copy (SMO + SqlBulkCopy) |
| `Program.Snapshot.cs` | Database snapshots for `--snapshot` |
| `Program.Resilience.cs` | Retries for transient failures, `.partial` files |
| `Program.Verify.cs` | The `verify` verb |
| `ExportManifest.cs` | `bcpjson-manifest.json`: read, write, file hashes, resume checks |
| `Program.Split.cs` | Split planning: `TableSplit` and `PlanSplit` (the split exports and copies run in `Program.Export.cs` and `Program.Copy.cs`) |
| `Program.Indexes.cs` | `--disable-indexes`: disabling and rebuilding indexes |
| `Program.HeapLoad.cs`, `HeapLoadScript.cs` | `--load-as-heap`: planning, dropping and recreating indexes; recovery script and markers |
| `SplitRequest.cs`, `KeyRange.cs`, `KeyInterpolation.cs`, `RangePlanner.cs` | `--split-tables` parsing, key ranges and boundary selection |
| `PartFiles.cs`, `LoadPlanning.cs`, `WorkQueue.cs` | Part file names, parallel or sequential loading, per-table work ordering |
| `Program.Sql.cs` | Table selection, name quoting, shared SQL helpers |
| `ExportOptions.cs`, `ImportOptions.cs`, `CopyOptions.cs`, `VerifyOptions.cs` | Command-line options for each verb |
| `SqlConnectionFactory.cs` | Connection string building and connection tests |
| `SqlBulkCopyHelper.cs` | Row-count helper for SqlBulkCopy |
| `CustomSelect.sql` | Reference copy of the JSON export metadata query |

The plans and design notes behind larger changes are in `docs/superpowers`, and `docs/ROADMAP.md` records what has been done and what is next.
