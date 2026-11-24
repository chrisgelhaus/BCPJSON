# Loading split tables as heaps (`import --load-as-heap`)

Status: draft for the owner's review (builder check done; its questions were settled as noted in "Calls made in writing this"). Follows `2026-11-23-key-range-splitting-design.md`, which deferred this as "option 2".

## Why

Large restores usually load into tables that already have their keys and indexes ("shape B"). A table with a clustered rowstore index can only take its parts one after another, even with `--disable-indexes`. Loading the parts in parallel into a heap and building the clustered index afterwards is faster. Benchmark run 45326185184 (5,000,000 rows, bigint identity clustered PK and two nonclustered indexes, 8 JSON parts, SQL Server 2022 Developer, 4 cores):

| Load | Seconds |
|---|---:|
| Plain import, indexes in place | 53.9 |
| `--disable-indexes` (option 1) | 30.1 |
| Heap load 12.9 + clustered PK 3.2 + nonclustered 1.7 + 2.6 (option 2) | 20.4 |

At 1,000,000 rows the two options were even (5.0 s against 4.8 s), so the gain grows with size.

## Goals

- An opt-in `import --load-as-heap` that, for each split table loaded into a target with a clustered rowstore index, drops the clustered index, loads all parts in parallel into the heap, then recreates the clustered index and rebuilds the other indexes.
- The target ends with exactly the keys and indexes it had: same names, columns, order, uniqueness, compression, partitioning and filegroup.
- Recreating happens even when the load fails, as `--disable-indexes` does today, and anything that cannot be recreated is reported with the statement that fixes it.
- A run that dies between drop and recreate (killed process, lost machine) can be repaired by hand from a script file, and the next import notices it.

## Non-goals

- Changing `copy`. It already loads tables it creates as heaps and builds keys and indexes afterwards (`CreateTargetIndexes`). `copy` into an existing table is not touched here (see "Brand-new and existing targets").
- Dropping foreign keys of other tables. Owner's decision (2026-11-23, "A"): a table that other tables' foreign keys reference is not loaded as a heap.
- Clustered columnstore targets. Their loads are sequential for a different reason (lock modes, `LoadPlanning.ClusteredColumnstoreLoadsInParallel`), and rebuilding one is a different trade-off.
- Unsplit tables: with a single file there is nothing to load in parallel.

## Anti-goals

- A target left without its primary key with no error and no way back. Every drop is preceded by a saved script; every failure to recreate is an error with the exact statement.
- Touching any table other than the one being loaded.

## Approaches considered

1. **Script, drop, load as a heap, recreate. Chosen.** SMO scripts the clustered index or constraint from the target before anything changes; nonclustered indexes are disabled, so their definitions stay in the database and they come back with `ALTER INDEX ... REBUILD` as today.
2. **Disable the clustered index instead of dropping it.** Not possible: a table whose clustered index is disabled cannot be read or written, so it cannot be loaded.
3. **Load a new heap table and swap it in by renaming.** This is the staging-table design that was put off: twice the space, and permissions, triggers, constraints, foreign keys, statistics and dependent objects to move. Not now.

## Design

### 1. Option

`import --load-as-heap` (bool, default off). It implies `--disable-indexes` for every table it applies to and for every table it falls back on. Help text: "For split tables loaded into a table with a clustered index: drop the clustered index, load the parts in parallel into the heap, then recreate it and rebuild the other indexes. Implies --disable-indexes."

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
| No indexed view references the table (schema-bound views) | `indexed view {name}` |
| Not system-versioned temporal, and not a history table | `temporal table` |
| Change tracking off; not published for replication; CDC off | `change tracking` / `replication` / `change data capture` |
| No FILESTREAM column | `FILESTREAM` |
| No full-text index on the table (its key index cannot be dropped or disabled freely) | `full-text index` |
| Target empty, checked at planning (skipped with `--truncate-target`, which empties it) | `not empty` |
| The recovery script can be written (section 5) | `cannot write {path}: {message}` |
| The clustered index statement fits the marker (section 5, at most 3,500 characters) | `clustered index definition too long` |

The checks run at planning, because the load mode and the work items are fixed there. "Before" checks emptiness once more right before the drop: if rows arrived meanwhile, it changes nothing on the target and fails the table with the error `{table}: rows were added after planning; not loaded.` (the parts are not loaded). The empty-table rule keeps the cost predictable (no rewrite of existing rows when the clustered index is dropped or rebuilt) and matches the restore use case. Duplicate keys in the loaded data are reported when the key is recreated, as unique-index rebuild failures are today.

### 3. Steps for one table

Before the first part (`TableWork` before):

1. Truncate when `--truncate-target`.
2. Script the clustered index with SMO from the target database: as `ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY/UNIQUE CLUSTERED` when it is a constraint, else `CREATE CLUSTERED INDEX`. Keep the filegroup or partition scheme (do not set `NoFileGroup`: it is the same database), data compression, fill factor and other index options.
3. Write the recovery script and set the marker on the table (section 5).
4. Disable all enabled nonclustered indexes, *including* a nonclustered primary key or unique constraint (nothing references them, by section 2). This differs from `--disable-indexes`, which keeps the primary key, so the heap has no enabled index and parts load in parallel (`LoadPlanning.SequentialReason` returns null).
5. Drop the clustered index (`ALTER TABLE ... DROP CONSTRAINT` or `DROP INDEX`). A partitioned clustered index leaves the heap on the same partition scheme.
6. Log `{table}: clustered index {name} dropped; loading {n} parts in parallel as a heap.`

The parts load as for any heap: in parallel, TABLOCK, no order hint.

After the last part, whether or not the load succeeded (`TableWork` after):

1. If the clustered index does not exist, run the saved script. On failure: error `{table}: Could not recreate {name}: {reason}. Run the script in {path}.`, and stop here (nonclustered indexes stay disabled; rebuilding them on a heap would be wasted work and the script rebuilds them too).
2. Rebuild the disabled indexes with the existing `RebuildIndexes` (same order and error messages).
3. When everything succeeded, remove the marker, delete the recovery script and log `{table}: clustered index {name} recreated; rebuilt {k} index(es).`

If step 2-5 of "before" fails part way, "after" still runs and puts back what was changed (it checks what exists, as `RebuildDisabledIndexes` does today).

### 4. Messages and exit code

Every failure to drop, recreate or rebuild is an `InfoLevel.Error` (exit code 1). Fallbacks are warnings. The clustered index script is also written to the log at Info level before the drop, so a run with `--log-file` keeps it even if the script file is lost.

### 5. Recovery script

- Name `<schema>.<table>.load-as-heap.sql`, in the source folder (`--source-path`). If it cannot be written there (read-only folder), the table falls back (section 2): the safety net is a condition, not a best effort.
- Content: a comment saying what happened and when, then the clustered index statement, then `ALTER INDEX ... REBUILD` for every index the run disabled, in `RebuildOrder`.
- Deleted only after the clustered index and all disabled indexes are back.
- Import never reads it as data: `IsImportableFile` skips the `.load-as-heap.sql` suffix (a `--source-file *` pattern would otherwise pick it up).
- **Marker on the target.** Before the drop, the table gets the extended property `bcpjson_load_as_heap` whose value is the clustered index statement (nvarchar, hence the 3,500-character limit in section 2; `sql_variant` holds at most 8,000 bytes). It is removed only together with the script file. So an interrupted run is recognised from the target itself, whatever folder the next import reads from and even if the script file is gone.
- **Next import.** Every import (with or without `--load-as-heap`) reads the marker of each target table at planning. A marked table is an error and is skipped, nothing changed: `{table}: an interrupted --load-as-heap run left it without its clustered index; run the script in {file} (or the statement in its extended property bcpjson_load_as_heap) and ALTER INDEX ... REBUILD the disabled indexes, then import again.` The message never suggests deleting the script. A recovery script file in the source folder for a table with no marker is only a warning (`{file} is left over; the target is whole, delete it.`).

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

- Unit (`LoadPlanningTests`): every row of the section 2 table, including precedence (first reason reported); which indexes are disabled under `--load-as-heap` (nonclustered primary key included); recovery-script naming and `IsImportableFile`.
- CI (`roundtrip.sh split`):
  - a split table with a clustered primary key, a nonclustered unique constraint, a filtered index and page compression, imported with `--load-as-heap`: data identical to the source, and the index definitions (`sys.indexes`, key columns, compression, `is_disabled = 0`) identical before and after;
  - a partitioned clustered table: same checks, and the heap stayed on the partition scheme;
  - a table referenced by a foreign key: falls back with the warning, data identical;
  - a failed load (one part file corrupted on purpose): exit code 1, and afterwards the primary key and all indexes exist and are enabled;
  - a leftover recovery script: the table is refused and left unchanged;
  - duplicate keys in the data: the key cannot be recreated, error names the script, the script file and the marker stay; a second import of that table is refused even from another folder;
  - one BCP case (a split table imported with `--file-format bcp --load-as-heap`);
  - a full-text indexed table falls back (only where the CI image has full-text search; otherwise unit tests only).
- First CI step of the build (a probe like `lock_probe.ps1`): on 2019, 2022 and 2025, dropping the clustered index leaves disabled nonclustered indexes disabled and not rebuilt, and recreating it leaves them disabled. The step order in section 3 relies on this; if a version behaves differently, the design comes back for a decision.
- Benchmark: `benchmark.sh` gains a row "import indexed, --load-as-heap" so the option is measured by bcpJson itself, not by hand.

## Left to the builder

- Exact SMO scripting options for the clustered index (verify on 2019/2022/2025 that filegroup, partition scheme and compression come back).
- How the section 2 conditions are read (one catalog query per table is enough).
- Where `--load-as-heap` sits next to `--disable-indexes` in the import planning code.

## Later

- Import creating missing tables from table definitions saved by export (owner's question, 2026-11-23).
- `copy --load-as-heap` and `copy --disable-indexes` for existing targets.
- Dropping and recreating foreign keys of referencing tables (rejected for now, "B").
- Clustered columnstore targets.

## Calls made in writing this

Not decided by the owner; correct any of them.

- `--load-as-heap` applies to JSON and BCP imports, to split tables only, and implies `--disable-indexes`.
- Under it, a nonclustered primary key or unique constraint is disabled too (nothing references it), so the heap loads in parallel.
- The target must be empty; rows arriving between planning and the drop fail the table without changing it.
- The safety net is two-fold: a script file next to the data and an extended property on the table holding the clustered index statement; an import refuses a marked table.
- Full-text indexed tables fall back.
- The SQL Server behaviour the step order depends on (disabled indexes stay disabled across drop and create of the clustered index) is checked by a CI probe before anything else is built.
