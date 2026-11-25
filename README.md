# BCPJSON

A fast, simple command-line tool for getting data **out of**, **into**, and **between** Microsoft SQL Server databases.

## Why

After reading about how Facebook backs up its MySQL fleet at scale, and just as importantly routinely *tests* those backups by restoring them, I wanted to see whether part of that model could be replicated for SQL Server. BCPJSON unloads every table into a structured, deterministic file format so that:

- exports are **diff-able**: two exports of the same table can be compared with ordinary line-based diff tools;
- exports can be **restored** into another database to prove the backup is good;
- the whole process stays **simple**: one executable, no agents, no SSIS packages.

## Features

| Verb | What it does |
|---|---|
| `export` | Writes each selected table to its own file, in **JSON** (via SqlClient) or **BCP** character format (via the `bcp` utility), or the result of one query to a single file. |
| `import` | Loads JSON or BCP files written by `export` into existing tables. |
| `copy` | Copies tables server to server with `SqlBulkCopy`, creating target tables, keys and indexes as needed. |
| `verify` | Checks an export folder against its manifest (size and SHA-256 of every file); no database needed. |

- **Diff-able JSON:** JSON exports are ordered by primary key, so repeated exports of unchanged data are identical. See [JSON format](docs/reference/json-format.md).
- **Parallel:** tables run in parallel, and `--max-tasks` sets how many at once. With no include filter, every user table is selected. See [Common concepts](docs/reference/common.md).
- **Fast loads:** imports and copies take a table lock, which allows minimal logging. See [Load into a live database](docs/guides/load-into-a-live-database.md) for when to turn it off.
- **Point-in-time exports:** `--snapshot` reads every table from a database snapshot. See [Back up a database](docs/guides/back-up-a-database.md#point-in-time-exports-with---snapshot).
- **Resilient runs:** transient failures are retried, files appear only when complete, every export has a manifest with SHA-256 hashes, and interrupted exports continue with `--resume`. See [Recover a failed run](docs/guides/recover-a-failed-run.md).
- **Very large tables:** `--split-tables` cuts tables into key ranges that export, load and copy in parallel. `--disable-indexes` and `--load-as-heap` speed up loads into indexed tables. See [Large tables](docs/guides/large-tables.md).
- **Compression:** `--compress gzip` makes files several times smaller, and import reads them directly. See [Compressed exports](docs/guides/back-up-a-database.md#compressed-exports).
- **Flat memory:** JSON import streams rows straight into the bulk load, however large the table.
- **Authentication:** Windows Authentication by default; SQL logins are supported.
- **Exit codes:** `0` on success, `1` if anything reported an error, so bcpJson suits scheduled jobs.

## Requirements

- **Operating system:** Windows (x64 or Arm64), Linux (x64 or Arm64) or macOS (Apple Silicon). Release downloads are self-contained, so no .NET install is needed.
- **bcp utility:** `--file-format bcp`, and `copy` of tables with geography, geometry or hierarchyid columns, need the SQL Server **bcp utility** on `PATH`. It is installed with SQL Server or the *Microsoft Command Line Utilities for SQL Server*.
- **SQL Server:** 2008 or later. Every release is tested against SQL Server 2019, 2022 and 2025. `--snapshot` needs SQL Server 2016 SP1 or later (any edition), and is not available on Azure SQL Database.

## Install

Download the package for your platform from the [Releases page](https://github.com/chrisgelhaus/BCPJSON/releases):

| Platform | Package |
|---|---|
| Windows x64 | `bcpjson-<version>-win-x64.zip` |
| Windows Arm64 | `bcpjson-<version>-win-arm64.zip` |
| Linux x64 | `bcpjson-<version>-linux-x64.tar.gz` |
| Linux Arm64 | `bcpjson-<version>-linux-arm64.tar.gz` |
| macOS (Apple Silicon) | `bcpjson-<version>-osx-arm64.tar.gz` |

Each package holds a single self-contained executable (`bcpjson.exe` / `bcpjson`), `LICENSE`, `NOTICE`, this README and the `docs/guides` and `docs/reference` folders. Unpack it anywhere and add the folder to `PATH`.

Every release is round-trip tested against SQL Server on Linux x64. The Arm64 and macOS builds are built and smoke-tested on their own platforms. SQL Server containers run only on x64, so those builds are not tested against a database in CI.

**macOS:** the executable is not signed with an Apple Developer ID or notarized, so macOS blocks a copy downloaded with a browser ("cannot be opened because the developer cannot be verified"). After checking the download against `SHA256SUMS`, remove the quarantine flag with `xattr -d com.apple.quarantine bcpjson`. Files downloaded with `curl` are not quarantined. For BCP files and spatial-type copies, install `bcp` with Homebrew (`brew install mssql-tools18` from Microsoft's `microsoft/mssql-release` tap).

To check a download, compare it with `SHA256SUMS` from the same release, or verify where and how it was built. Verifying the build needs the [GitHub CLI](https://cli.github.com/):

```powershell
Get-FileHash .\bcpjson-2.0.0-win-x64.zip -Algorithm SHA256        # compare with SHA256SUMS
gh attestation verify .\bcpjson-2.0.0-win-x64.zip --repo chrisgelhaus/BCPJSON
```

## Quick start

```powershell
# Export every table of Sales to JSON, all from the same moment
bcpjson export --source-server sql01 --source-database Sales --file-format json --export-path D:\exports\Sales --snapshot

# Load it into an existing database with the same tables
bcpjson import --file-format json --source-path D:\exports\Sales --source-file "*.json" --target-server sql02 --target-database Sales --truncate-target
```

`bcpjson --help` and `bcpjson <verb> --help` list the options.

## I want to…

- [Back up a database](docs/guides/back-up-a-database.md) to JSON or BCP files, consistently and compressed if needed.
- [Restore an export](docs/guides/restore-an-export.md) into an existing database.
- [Copy tables between servers](docs/guides/copy-between-servers.md), with keys, indexes, constraints and triggers.
- [Move very large tables](docs/guides/large-tables.md) in parallel key ranges, and load them fast into indexed tables.
- [Recover a failed run](docs/guides/recover-a-failed-run.md): retries, verify, resume, and the load-as-heap recovery script.
- [Load into a live database](docs/guides/load-into-a-live-database.md) without blocking the applications using it.

## Reference

- Options of each verb: [export](docs/reference/export.md), [import](docs/reference/import.md), [copy](docs/reference/copy.md), [verify](docs/reference/verify.md)
- [Common concepts](docs/reference/common.md): connecting, table filters, parallelism, retries, logging, exit codes
- [The export manifest](docs/reference/manifest.md)
- [JSON format](docs/reference/json-format.md)
- [Known limitations](docs/reference/limitations.md)

To build bcpJson or contribute, see [CONTRIBUTING.md](https://github.com/chrisgelhaus/BCPJSON/blob/master/CONTRIBUTING.md).

## Resources

- [Under the hood: Automated backups](https://engineering.fb.com/2013/01/14/web/under-the-hood-automated-backups/): Facebook's case for logical (mysqldump-style) backups
- [Continuous MySQL backup validation: Restoring backups](https://engineering.fb.com/2016/10/28/data-infrastructure/continuous-mysql-backup-validation-restoring-backups/): continuously proving backups by restoring them
- [bcp utility](https://learn.microsoft.com/sql/tools/bcp-utility)
- [SqlBulkCopy](https://learn.microsoft.com/dotnet/api/microsoft.data.sqlclient.sqlbulkcopy)

## Security

Please report vulnerabilities privately, not in public issues. See [SECURITY.md](https://github.com/chrisgelhaus/BCPJSON/blob/master/SECURITY.md) for how, and for security notes on running bcpJson.

## License

Licensed under the [Apache License, Version 2.0](LICENSE). Copyright 2019-2026 Chris Gelhaus.

You are free to use, modify and redistribute bcpJson, including commercially. If you redistribute it or a derivative, keep the copyright notice and include the [NOTICE](NOTICE) file, as the license requires.
