# Common concepts

What `export`, `import`, `copy` and `verify` have in common. Each verb's page lists its own options; this page explains the shared behaviour behind them.

## Connecting

- **Windows Authentication** is the default: leave out the login and password options (`--source-login` / `--source-password`, `--target-login` / `--target-password`).
- **SQL logins** are supported by giving both. bcp takes a SQL password only on its command line (`-P`), so it is visible in the process list while bcp runs; prefer Windows Authentication. See [Known limitations](limitations.md).
- `--trust-server-certificate` accepts the server's TLS certificate without validation, which local instances with self-signed certificates usually need.
- `--file-format bcp`, and `copy` of tables with geography, geometry or hierarchyid columns, need the SQL Server `bcp` utility on `PATH`.

## Table filters

`export` and `copy` select tables with four options, each taking a comma-separated list:

- `--include-tables` and `--exclude-tables` take `schema.table` names.
- `--include-schemas` and `--exclude-schemas` take schema names.

`*` is a wildcard in all four, for example `"Sales.*History"`. With no include filter, every user table is selected; exclude filters then remove tables from that set. `--split-tables` entries use the same `*` wildcard.

## Parallelism

Tables are processed in parallel. `--max-tasks` limits how many run at once:
- `export` and `copy`: tables;
- `import`: files;
- split tables: each part counts as one task.

The default `-1` (or `0`) means unlimited. The largest tables and parts start first.

## Retries

`--retries` (default `3`, `0` turns it off) is available on `export`, `import` and `copy`.

- **Opening a connection** is retried when SQL Server reports a transient error:
  - network errors;
  - deadlock victims;
  - Azure SQL throttling and failover;
  - "cannot open database" while a database comes online.

  The waits grow exponentially (about 1, 2, 4 and 8 s, at most 30 s). Each retry is logged as a warning.
- **Exporting a table** is retried as a whole. JSON exports retry on a transient error. BCP exports retry on any bcp failure, because bcp reports no error numbers. Exporting only rewrites the table's file, so starting over is safe. Waits: 2, 4, 8 and 16 s, then 30 s.
- **Not retried:**
  - `--source-query` exports, because the query may be a procedure with side effects;
  - the data loads of `import` and `copy`, because a load may have committed some batches before failing, and replaying it could duplicate rows. Rerun those with `--truncate-target` or `--drop-target`.

## Logging

Every verb writes progress, warnings and errors to the console. `--log-file <path>` also appends them to a file.

## Exit codes

- `0`: the run succeeded. Warnings do not change the exit code.
- `1`: at least one error was reported, or the command line was invalid.

Each table or file is handled on its own, so one failure does not stop the others. The run still ends with `1`, which makes bcpJson suitable for scheduled jobs.
