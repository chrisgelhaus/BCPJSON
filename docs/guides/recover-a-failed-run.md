# Recover a failed run

Use this guide when an export, import or copy stopped partway, an export looks damaged, or a table was left without its indexes.

Long runs against busy or remote servers hit network drops, failovers and throttling. bcpJson is built to survive them, to never leave a half-written file that looks complete, and to let you check and finish an export later.

## Steps

1. **Read the errors.** Every failure is reported by table or file, and the run exits with `1`. The other tables carry on, so usually only a few need attention. With `--log-file` the messages are also in the log.
2. **For an export:** check the folder, then finish it.

   ```powershell
   bcpjson verify --path D:\exports\Sales
   bcpjson export --source-server sql01 --source-database Sales --file-format json --export-path D:\exports\Sales --resume
   ```

3. **For an import or copy:** fix the cause, then rerun the failed tables with `--truncate-target` (or `--drop-target` for copy). Loads are not resumed or retried, because some batches may already be committed.

## Retries

Transient failures are retried before you ever see them, as described in [Retries](../reference/common.md#retries):
- opening a connection is retried, on every verb;
- exporting a table is retried as a whole.

Raise `--retries` on a flaky link:

```powershell
bcpjson export --source-server sql01 --source-database Sales --file-format json --export-path D:\exports\Sales --retries 8
```

## Complete files only

Every export file is first written as `<name>.partial`, and renamed to its real name only when it is complete. For BCP, the data and format files are renamed together. A crash, a killed process or a failed table never leaves a truncated `.json` or `.bcp` file behind. If an export fails, a file from an earlier export keeps its previous content. `import` ignores `.partial` files, and the next export overwrites them.

## Verify an export

`verify` checks an export folder against its manifest without touching a database. Every file must exist with its recorded size and SHA-256, and the export must have completed:

```powershell
bcpjson verify --path D:\exports\Sales
```

**Errors:** it exits `1` and names each problem:
- a missing file;
- a changed size or hash, from corruption, an accidental edit or an incomplete copy to another disk;
- an export that never completed.

**Warnings:**
- extra `.json` or `.bcp` files the manifest doesn't list, for example tables left from an older export;
- leftover `.partial` files.

Run it after copying an export to archive storage, and before restoring from it. See [verify](../reference/verify.md).

## Resume an interrupted export

If an export stops partway (a crash, a reboot, a table that kept failing), rerun the same command with `--resume`:

```powershell
bcpjson export --source-server sql01 --source-database Sales --file-format json --export-path D:\exports\Sales --resume
```

- **What is skipped:** tables the manifest records, whose files still have the recorded size. Everything else is exported.
- **Same settings:** the run must use the same `--file-format`, `--compress`, `--source-database` and `--source-query` as the manifest, otherwise it stops with an error.
- **Snapshots:** with `--snapshot`, each run takes its own snapshot, so tables from the earlier run reflect an earlier point in time, and a warning says so. Use a fresh export when one point in time matters more than saving the work already done.
- **Split tables** resume part by part:
  - Parts the manifest records, with their files intact, are kept; only the missing or damaged ones are exported (`<table>: <k> part(s) to go.`).
  - The recorded split is kept. The run says `<table>: --resume keeps its recorded split (<n> parts).` when your `--split-tables` differs or is missing, because new boundaries would not match the parts already written.
  - A transient error therefore redoes one part, not the table.

## The load-as-heap recovery script

`import --load-as-heap` drops a table's indexes and recreates them after the load (see [Load as a heap](large-tables.md#load-as-a-heap---load-as-heap)). If the run is interrupted, or an index cannot be recreated, two safeguards remain:
- **The script:** `<schema>.<table>.load-as-heap.sql`, written into `--source-path` when the import was planned.
- **The markers:** one extended property on the table per index, named `bcpjson_load_as_heap_001`, `_002` and so on in creation order, each holding that index's statement.

Both are removed only when every index exists again. The script file is never read as data; `--source-file "*"` skips it.

**Run the recovery script.**

```powershell
sqlcmd -S sql02 -x -f 65001 -i D:\exports\Sales\dbo.OrderHistory.load-as-heap.sql
```

- **What it does:** it switches to the target database and sets the session options index statements need (`QUOTED_IDENTIFIER ON` and the others, for filtered indexes). Then it creates only the indexes that are missing. Finally, only if every index exists, it drops the markers. Otherwise it raises `Not every index exists; the markers are kept. Fix the cause and run this script again.` and keeps them. Running it again is safe.
- **sqlcmd flags:** the script is UTF-8 with a byte order mark. `-f 65001` makes sqlcmd read non-ASCII names correctly. `-x` stops a `$(` in a name from being taken for a sqlcmd variable.
- **Database check:** every batch checks that it runs in the database the import loaded. If that database has since been renamed or restored under another name, the script says so and changes nothing. Replace the database name in it (the `USE` line and the `DB_NAME()` checks) and run it again.
- **Duplicate keys:** if the failure was duplicate keys, fix the data first, or the key will fail again.
- **Tested:** CI runs the recovery script with sqlcmd, without `-I`, on SQL Server 2019, 2022 and 2025. The runs cover a filtered index and a run against the wrong database.

**The next import refuses a marked table.** Every import, with or without `--load-as-heap`, checks its target tables for the markers. A marked table is an error, and is skipped unchanged:

`<table>: an earlier --load-as-heap run left it without some of its indexes; run the script in <file> (it recreates what is missing and removes the markers; if the file is lost, the statements are in the extended properties bcpjson_load_as_heap_*, drop those after running them), then import again.`

- **Which folder:** the path in the message is built from the current `--source-path`. If you import from another folder, use the script in the folder of the earlier run.
- **Lost script:** if the script file is lost, the statements are in the table's extended properties `bcpjson_load_as_heap_*`. Run them, then drop the properties.

**A script without markers** comes from a run that stopped before dropping anything. It only warns: `<file> is left over and <table> has no markers; if all its indexes exist, delete it.` Before deleting it, check that the indexes in the script all exist.

## If something goes wrong

- **`--resume` stops with an error:** the command differs from the manifest in format, compression, database or query. Use the original options, or export into a new folder.
- **A rebuilt index failed after `--disable-indexes`:** the error names the index and the `ALTER INDEX ... REBUILD` command to run once the cause (usually duplicate keys) is fixed.
- **A snapshot database was left behind:** see [Back up a database](back-up-a-database.md#if-something-goes-wrong).

## See also

- [Retries](../reference/common.md#retries)
- [The export manifest](../reference/manifest.md)
- [verify](../reference/verify.md)
