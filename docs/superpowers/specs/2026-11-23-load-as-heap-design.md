# Loading split tables as heaps (`import --load-as-heap`)

Status: implemented in PR #48 (see "As built" at the end); approved 2026-11-23; revised 2026-11-24 after the CI probe (owner chose to drop every index, see "Revision"). Follows `2026-11-23-key-range-splitting-design.md`, which deferred this as "option 2".

## Why

Large restores usually load into tables that already have their keys and indexes ("shape B"). A table with a clustered rowstore index can only take its parts one after another, even with `--disable-indexes`. Loading the parts in parallel into a heap and building the clustered index afterwards is faster. Benchmark run 45326185184 (5,000,000 rows, bigint identity clustered PK and two nonclustered indexes, 8 JSON parts, SQL Server 2022 Developer, 4 cores):

| Load | Seconds |
|---|---:|
| Plain import, indexes in place | 53.9 |
| `--disable-indexes` (option 1) | 30.1 |
| Heap load 12.9 + clustered PK 3.2 + nonclustered 1.7 + 2.6 (option 2) | 20.4 |

At 1,000,000 rows the two options were even (5.0 s against 4.8 s), so the gain grows with size.

## Goals

- An opt-in `import --load-as-heap` that, for each split table loaded into a target with a clustered rowstore index, drops all its indexes, loads all parts in parallel into the heap, then recreates the clustered index and then the other indexes.
- The target ends with exactly the keys and indexes it had: same names, columns, order, uniqueness, compression, partitioning and filegroup.
- Recreating happens even when the load fails, as `--disable-indexes` rebuilds today, and anything that cannot be recreated is reported by index name with the script that fixes it.
- A run that dies between drop and recreate (killed process, lost machine) can be repaired by hand from a script file, and the next import notices it.

## Non-goals

- Changing `copy`. It already loads tables it creates as heaps and builds keys and indexes afterwards (`CreateTargetIndexes`). `copy` into an existing table is not touched here (see "Brand-new and existing targets").
- Dropping foreign keys of other tables. Owner's decision (2026-11-23, "A"): a table that other tables' foreign keys reference is not loaded as a heap.
- Clustered columnstore targets. Their loads are sequential for a different reason (lock modes, `LoadPlanning.ClusteredColumnstoreLoadsInParallel`), and rebuilding one is a different trade-off.
- Unsplit tables: with a single file there is nothing to load in parallel.

## Anti-goals

- A target left without its primary key with no error and no way back. Every drop is preceded by a saved script and markers on the table; every failure to recreate is an error naming the index and the script.
- Touching any table other than the one being loaded.

## Approaches considered

1. **Script every index, drop them all, load as a heap, recreate. Chosen (2026-11-24).** SMO scripts every index and key constraint from the target before anything changes; the nonclustered ones are dropped first, then the clustered one; afterwards they are created again clustered first, as `copy` does for tables it creates. This is exactly what the benchmark measured.
1a. **Script the clustered index only, disable the nonclustered ones. The first choice, rejected by the CI probe.** On SQL Server 2019, 2022 and 2025, dropping the clustered index rebuilds every nonclustered index, disabled ones included, and they come back enabled (CI run 45540422748). Dropping the clustered index first and disabling afterwards was possible on an empty table, but depended on whether creating the clustered index rebuilds disabled indexes too, and would report a duplicate in a unique nonclustered index as a failure of the clustered index.
2. **Disable the clustered index instead of dropping it.** Not possible: a table whose clustered index is disabled cannot be read or written, so it cannot be loaded.
3. **Load a new heap table and swap it in by renaming.** This is the staging-table design that was put off: twice the space, and permissions, triggers, constraints, foreign keys, statistics and dependent objects to move. Not now.

## Design

### 1. Option

`import --load-as-heap` (bool, default off). Tables it does not load as heaps are loaded as with `--disable-indexes`. Help text: "For split tables loaded into a table with a clustered index: drop its indexes, load the parts in parallel into the heap, then recreate the indexes. Other tables load as with --disable-indexes."

It applies to JSON and BCP imports alike, and to a table only when the import has two or more files for it (part files). A table with one file is loaded as with `--disable-indexes`. The table is always the *target* table (after `--target-table` mapping): names, markers and scripts below use the target's schema and name.

### 2. When a table is loaded as a heap

All of these must hold; otherwise the table falls back to `--disable-indexes` with the warning `{table}: not loaded as a heap: {reason}; disabling indexes instead.` The rules live in a pure function in `LoadPlanning` (for example `HeapLoadBlocker(...)` returning the reason or null) so they have unit tests.

| Condition | Reason text |
|---|---|
| The target has a clustered rowstore index (type 1) | none: a heap needs no change and a clustered columnstore is out of scope, so they keep today's behaviour without a warning; columnstore gets `clustered columnstore` |
| Table lock on (not `--no-table-lock`) | `--no-table-lock` |
| No staged CLR columns (JSON import of geography/geometry/hierarchyid) | `CLR columns` |
| Not memory-optimized | `memory-optimized` |
| No foreign key of any table (including itself) references any key or unique index of the table | `referenced by foreign key {name}` |
| No XML or spatial index (they require a clustered primary key and cannot exist on a heap) | `XML or spatial index {name}` |
| No index is disabled before the run (dropping and recreating it would enable it) | `disabled index {name}` |
| No indexed view references the table (schema-bound views) | `indexed view {name}` |
| Not system-versioned temporal, and not a history table | `temporal table` |
| Change tracking off; not published for replication; CDC off | `change tracking` / `replication` / `change data capture` |
| No FILESTREAM column | `FILESTREAM` |
| No full-text index on the table (its key index cannot be dropped or disabled freely) | `full-text index` |
| Target empty, checked at planning (skipped with `--truncate-target`, which empties it) | `not empty` |
| The recovery script can be written (section 5) | `cannot write {path}: {message}` |
| Every index statement fits its marker (section 5, at most 3,500 characters each) | `index definition too long: {name}` |

The checks run at planning, because the load mode and the work items are fixed there. "Before" checks emptiness once more right before the drop: if rows arrived meanwhile, it changes nothing on the target and fails the table with the error `{table}: rows were added after planning; not loaded.` (the parts are not loaded). The empty-table rule keeps the cost predictable (no rewrite of existing rows when indexes are dropped or created) and matches the restore use case. Duplicate keys in the loaded data are reported when the key or unique index is recreated, against that index.

### 3. Steps for one table

Before the first part (`TableWork` before):

1. Truncate when `--truncate-target`.
2. Check the table is still empty (section 2).
3. Set the markers on the table (section 5). The recovery script was written at planning.
4. Drop the nonclustered indexes (`DROP INDEX`, or `ALTER TABLE ... DROP CONSTRAINT` for a nonclustered primary key or unique constraint; nothing references them, by section 2), then the clustered one. A partitioned clustered index leaves the heap on the same partition scheme. The heap has no index, so parts load in parallel (`LoadPlanning.SequentialReason` returns null).
5. Log `{table}: {k} index(es) dropped; loading {n} parts in parallel as a heap.`

The index statements are scripted with SMO from the target database at planning: constraints as `ALTER TABLE ... ADD CONSTRAINT`, others as `CREATE ... INDEX`, keeping filegroup or partition scheme (do not set `NoFileGroup`: it is the same database), data compression, fill factor, filters, included columns and other options. They are kept in creation order: the clustered index first, then the others in `IndexCreationRank` order (shared with `copy`).

The parts load as for any heap: in parallel, TABLOCK, no order hint.

After the last part, whether or not the load succeeded (`TableWork` after), deciding only from what exists now:

1. If the clustered index is missing, run its statement. On failure: error `{table}: Could not recreate {name}: {reason}. Run the script in {path}.`, and stop here: nonclustered indexes built on the heap would be rebuilt again when the clustered index comes back.
2. Create each missing nonclustered index from its statement, in order. A failure is the same error for that index; the others are still created.
3. When every index exists again, remove the markers, delete the recovery script and log `{table}: clustered index {name} and {k} other index(es) recreated.` Otherwise keep both.

If "before" fails part way, "after" still runs and creates whatever is missing.
### 4. Messages and exit code

Every failure to drop or recreate is an `InfoLevel.Error` (exit code 1). Fallbacks are warnings. The index statements are also written to the log at Info level before the drop, so a run with `--log-file` keeps them even if the script file is lost.

### 5. Recovery script

- Name `<schema>.<table>.load-as-heap.sql`, in the source folder (`--source-path`). If it cannot be written there (read-only folder), the table falls back (section 2): the safety net is a condition, not a best effort.
- Content: a comment saying what happened and when, and that the script is rerunnable; `USE [database];` and `GO`; `SET QUOTED_IDENTIFIER ON; SET ANSI_NULLS ON; SET ANSI_PADDING ON; SET ANSI_WARNINGS ON; SET CONCAT_NULL_YIELDS_NULL ON; SET ARITHABORT ON; SET NUMERIC_ROUNDABORT OFF;` and `GO`, so no tool option (such as `sqlcmd -I`) is needed; then every index statement in creation order, each wrapped in `IF NOT EXISTS (SELECT 1 FROM sys.indexes ...) BEGIN ... END` so only missing indexes are created, each followed by `GO`; then one final batch that drops the markers (each `IF EXISTS (...) EXEC sys.sp_dropextendedproperty ...`) only when every saved index exists (`IF (SELECT COUNT(*) FROM sys.indexes WHERE object_id = OBJECT_ID(...) AND name IN (...)) = n BEGIN ... END`), else `RAISERROR(N'Not every index exists; the markers are kept. Fix the cause and run this script again.', 16, 1)`. sqlcmd and SSMS carry on after a failed batch, so without the guard a failed index would still lose its markers. Names reach the script as `N'...'` literals with quotes doubled. The file is UTF-8 with a byte order mark. Hypothetical indexes are neither scripted, dropped nor recreated.
- Deleted only after every index is back.
- Import never reads it as data: `IsImportableFile` skips the `.load-as-heap.sql` suffix (a `--source-file *` pattern would otherwise pick it up).
- **Markers on the target.** Before the drop, the table gets one extended property per index, `bcpjson_load_as_heap_001`, `_002`, ... in creation order, whose value is that index's statement (nvarchar, hence the 3,500-character limit in section 2; `sql_variant` holds at most 8,000 bytes). They are removed only together with the script file, and together they are a complete copy of it. So an interrupted run is recognised from the target itself, whatever folder the next import reads from and even if the script file is gone.
- **Next import.** Every import (with or without `--load-as-heap`) reads the markers of each target table at planning (any extended property named `bcpjson_load_as_heap_%`). A marked table is an error and is skipped, nothing changed: `{table}: an earlier --load-as-heap run left it without some of its indexes; run the script in {file} (it recreates what is missing and removes the markers; if the file is lost, the statements are in the extended properties bcpjson_load_as_heap_*, drop those after running them), then import again.` The message never suggests deleting the script. A recovery script file in the source folder for a table with no marker is only a warning (`{file} is left over and {table} has no markers; if all its indexes exist, delete it.`; it cannot tell whether the indexes exist, so it asks the reader to check).

### 6. Brand-new and existing targets

| Target | What happens |
|---|---|
| `copy` into a table it creates | Already heap first: loads all parts in parallel, then `CreateTargetIndexes` builds the keys and indexes. No change. |
| `import` into an empty table just created with its keys and indexes | The main case for `--load-as-heap`. |
| `import` into a table that does not exist | Not supported: an export holds no table definitions, so import reports `Target table does not exist`. Making this possible means export saving each table's definition and import creating missing tables; such tables would then load heap first like `copy`. A separate feature, listed under "Later". |
| `copy` into an existing table (no `--drop-target`) | Unchanged: loads into its existing indexes (one part after another for a clustered target). `copy --load-as-heap` would reuse this design's code; listed under "Later" with `copy --disable-indexes`. |

### 7. Documentation

README "Splitting large tables": the option, the fallback list, the recovery script, the edition note (outside Enterprise and Developer, SQL Server builds an index on one thread, so the gain is smaller than the benchmark), and the 5,000,000-row numbers. CLAUDE.md import row and ROADMAP.

## Tests

- Unit (`LoadPlanningTests`): every row of the section 2 table, including precedence (first reason reported); recovery-script naming, content and marker names; `IsImportableFile`.
- CI (`roundtrip.sh split`):
  - a split table with a clustered primary key, a nonclustered unique constraint, a filtered index and page compression, imported with `--load-as-heap`: data identical to the source, and the index definitions (`sys.indexes`, key columns, compression, `is_disabled = 0`) identical before and after;
  - a partitioned clustered table: same checks, and the heap stayed on the partition scheme;
  - a table referenced by a foreign key: falls back with the warning, data identical;
  - a failed load (one part file corrupted on purpose): exit code 1, and afterwards the primary key and all indexes exist;
  - a leftover recovery script: the table is refused and left unchanged;
  - duplicate keys in the data: the key cannot be recreated, the error names it and the script, the script file and the markers stay; a second import of that table is refused even from another folder;
  - duplicates in a unique nonclustered index: the clustered index comes back, the error names the unique index;
  - one BCP case (a split table imported with `--file-format bcp --load-as-heap`);
  - a full-text indexed table falls back (only where the CI image has full-text search; otherwise unit tests only).
- Benchmark: `benchmark.sh` gains a row "import indexed, --load-as-heap" so the option is measured by bcpJson itself, not by hand.

## Left to the builder

- Exact SMO scripting options for the indexes (the CI comparison proves that filegroup, partition scheme, compression and filters come back).
- How the section 2 conditions are read (one catalog query per table is enough).
- Where `--load-as-heap` sits next to `--disable-indexes` in the import planning code.

## Later

- Import creating missing tables from table definitions saved by export (owner's question, 2026-11-23).
- `copy --load-as-heap` and `copy --disable-indexes` for existing targets.
- Dropping and recreating foreign keys of referencing tables (rejected 2026-11-23).
- Clustered columnstore targets.

## Calls made in writing this

Not decided by the owner; correct any of them.

- `--load-as-heap` applies to JSON and BCP imports and to split tables only; the other tables load as with `--disable-indexes`.
- An index already disabled before the run makes the table fall back (recreating it would enable it).
- The target must be empty; rows arriving between planning and the drop fail the table without changing it.
- The safety net is two-fold: a script file next to the data and one extended property per index on the table; an import refuses a marked table.
- Full-text indexed tables fall back.

## Revision (2026-11-24)

The first version disabled the nonclustered indexes and dropped only the clustered index. The CI probe showed that dropping a clustered index rebuilds and re-enables disabled nonclustered indexes on SQL Server 2019, 2022 and 2025 (run 45540422748). The owner chose to drop and recreate every index instead (approach 1 above); the probe was removed, as nothing depends on that behaviour any more.

## As built

Where the implementation differs from the text above (the code and README win):

- **The recovery script is written at planning**, in `PlanHeapLoad`, not in "before" (section 3, step 3): "cannot write" is a planning-time fallback, which needs the write attempt then. "Before" deletes the script when it refuses because rows were added.
- **The script is rerunnable (section 5, as revised in the final review).** It starts with `USE` and its own `SET` options (no `sqlcmd -I` needed); each index statement sits in `IF NOT EXISTS (SELECT 1 FROM sys.indexes ...) BEGIN ... END`; the markers are dropped in one final batch only when every saved index exists, else `RAISERROR` keeps them. It is written as UTF-8 with a BOM (`HeapLoadScript.Write`). Follow-up (PR after v2.8.0): every batch checks `IF DB_NAME() = N'<database>'`, and a batch right after the `SET` block raises `This script is for database %s, which is not the current database; nothing was changed. ...` when the `USE` did not take (renamed or restored database), so nothing runs against a same-named table elsewhere; names in the header comments go through `CommentText` (control characters become `?`); the README recommends `sqlcmd -x` (no variable substitution of `$(` in names).
- **Interrupted-run message wording** is the one in section 5 as revised: "{table}: an earlier --load-as-heap run left it without some of its indexes; run the script in {file} (it recreates what is missing and removes the markers; if the file is lost, the statements are in the extended properties bcpjson_load_as_heap_*, drop those after running them), then import again." `{file}` is built from the current `--source-path`, which may not be the folder the script was written to.
- **No success line when nothing was created.** `{table}: clustered index {name} and {k} other index(es) recreated.` is posted only if "after" created at least one index; when "before" stopped ahead of the drop, markers and script are cleaned up silently. For a table with one index it reads "and 0 other index(es)".
- **An SMO scripting failure is an error that skips the table**; it is not a fallback reason. A failure to write the script is a fallback (`cannot write {path}: {message}`); the exception message loses its trailing full stop (`...denied; disabling indexes instead.`). If a planning read after the script was written fails, the script is deleted before the table is skipped.
- **"Rows were added" is reported per part.** The exception thrown in "before" goes through `TableWork`, so it is posted as `{table}: preparation failed: {table}: rows were added after planning; not loaded.` once per part file.
- **Two messages not in the design:** `{table}: every index exists, but its extended properties bcpjson_load_as_heap_* could not be removed: {reason}. Remove them before the next import.` (error) and `Could not delete {path}: {message}` (warning).
- **Reason order:** `DecideHeapLoad` checks `--no-table-lock`, `CLR columns` and `memory-optimized` before `clustered columnstore`, and skips `not empty` with `--truncate-target`. A target with neither a clustered rowstore nor a clustered columnstore index is a heap: no message, no change. A foreign key counts when it references the table at all (self-references included), not only a key or unique index.
- **Fallback tables** use the `--disable-indexes` logic under `--load-as-heap` too, so a memory-optimized fallback also logs `{table}: memory-optimized; --disable-indexes ignored.` The "left over" warning can be posted for a table that this run then plans as a heap and overwrites the file of.
- **Extra connections:** SMO scripting opens its own connection per planned table; it runs only after every other rule has passed. The database name (for SMO and the script's `USE`) is the planning connection's.
- **CI as built:** case (i) imports into `BcpJsonSplitHeapBcp` with `--truncate-target` and without `--load-as-heap` (in `BcpJsonSplitHeap`, split.Text is referenced by a foreign key, so TRUNCATE is refused); case (c) exports with `--exclude-tables split.TextRef`; case (f) imports from a copy of the Descending parts so the script stays out of the shared folder. Case (f1) runs the recovery script with sqlcmd without `-I` and `-b`: while the duplicates remain, the markers stay and the `RAISERROR` text is printed; after a TRUNCATE the second run recreates the key and removes the markers, and the next import (f2) is not refused. Follow-up: case (f) adds the filtered index `IX_Desc_Filt`, which (f1)'s second run must recreate under sqlcmd defaults (proving the script's own `SET QUOTED_IDENTIFIER ON`), (f1) first runs a copy whose `USE` names `master` (message, markers unchanged), sqlcmd runs with `-x`, and case (g) gives split.BigOther a hypothetical index (`STATISTICS_ONLY = -1`) that is skipped (still `1 index(es) dropped`). `heap_indexes.sql` also compares fill factor, IGNORE_DUP_KEY, page/row locks and STATISTICS_NORECOMPUTE (IX_Big_Note has non-default ones). Unit tests cover `DecideHeapLoad`, the script text and names and `IsImportableFile`; "before" and "after" are proven only by `roundtrip.sh split`.
- **Benchmark:** `benchmark.sh` has the row "import indexed, --load-as-heap" (database `BenchE`); it has not been run, because `benchmark.yml` can be dispatched only from master. The numbers in "Why" come from the hand-made sequence.
