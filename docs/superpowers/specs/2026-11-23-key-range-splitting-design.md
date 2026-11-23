# Key-range splitting for large tables: design

Status: draft, 2026-11-23. Replaces the "TODO: extremely large tables" section of `docs/ROADMAP.md` once approved.

## Why

Today one table is one unit of work for `export`, `import` and `copy`; `--max-tasks` runs *tables* in parallel. A table of hundreds of millions of rows is therefore read, written, compressed, hashed and loaded by a single thread, and it sets the length of the whole run. A transient error restarts it from the beginning and `--resume` can only skip whole tables.

The goal is **speed for very large tables**: split a chosen table into ranges of its clustered key so the ranges are exported, loaded and copied as separate tasks, in parallel wherever the target allows it.

Restores happen in two shapes, and both must work:

- **A. Into an existing, fully indexed table** (the model `import` assumes today).
- **B. Load first, index afterwards** (the more common case for large restores). The data goes in while indexes are out of the way, and the indexes are built at the end, as `copy` already does for tables it creates.

A third shape must also work: a **target with no indexes or constraints at all** (a bare heap), which is the fastest case and needs no index handling.

## Goals

1. `export` (JSON and BCP) can split selected tables into key-range parts, written by parallel tasks.
2. `import` loads the parts of a table in parallel when the target allows it (no enabled rowstore index, see "Load modes"), and as one ordered stream otherwise.
3. A new `import --disable-indexes` implements shape B: disable the target's nonclustered indexes, load, rebuild them.
4. `copy` uses the same ranges as parallel source-to-target streams, with no files in between.
5. Splitting is chosen **per table**. Tables that are not split produce exactly the files they produce today.
6. Parts retry, resume and verify one by one.

## Non-goals (this design)

- Splitting by row count or file size (no speed gain; revisit only if a use case appears).
- An automatic size threshold (`--split-above 10GB`). Per-table selection comes first.
- Dropping the clustered index to load into a heap ("option 2"). Only if the benchmark below shows it winning.
- Staging tables, including loading large history tables from staging tables over time. Saved for later (see "Later").
- Splitting unpartitioned heaps or unpartitioned clustered columnstore tables, or ranges over more than the first key column. Partitioned tables of every kind *are* in scope (section 11).
- Splitting `--source-query` exports, and `copy` tables that go through bcp (geography/geometry/hierarchyid).

## Anti-goals

- **Missing or duplicated rows at range edges.** Ranges must cover the key space completely and without overlap, whatever the boundaries are and whatever the data does.
- **Changing the output of tables that are not split.** Same file names, same bytes, same manifest entries.
- **Leaving a target without usable indexes after a failure.** Disabled indexes are always rebuilt (or reported by name as still disabled), even when the load failed.
- **Promising parallelism SQL Server will not give.** Where loads would only queue behind each other's locks, run them in order instead.

## Constraints

- .NET 8, Microsoft.Data.SqlClient 7, SMO, Linux bcp (no `-x`, no `-o`). SQL Server 2019, 2022 and 2025 are tested in CI. `sys.dm_db_stats_histogram` is available on all of them.
- The rules in `CLAUDE.md` apply: `QuoteName`/`SqlLiteral`, parameters where possible, `Interlocked` in parallel code, errors through `PostToConsole(..., InfoLevel.Error)` so the exit code is 1, timeouts 0 for data movement, per-table (now per-part) error isolation.
- The owner has no local SQL Server: everything database-related is proven only by the GitHub Actions round trips.

## Approaches considered (for shape B)

1. **Disable the nonclustered indexes, keep the clustered index. Chosen.** SQL Server keeps every index definition, so a failed run loses nothing. The rebuild is one statement per index, and primary keys, unique constraints and foreign keys stay in place. The clustered index takes rows in key order (with the v2.6.0 order hint), so it needs no sort, and with TABLOCK the load can be minimally logged. It works on every edition. Cost: a table with a clustered rowstore index loads as one stream, so its ranges do not load in parallel. A heap target whose nonclustered indexes are disabled *does* load in parallel.
2. **Drop to a heap, load ranges in parallel, recreate all indexes and keys.** Fastest load. But building the clustered index afterwards sorts the whole table (time and tempdb), and outside Enterprise edition index builds are single-threaded. Incoming foreign keys must be dropped and recreated, and a failure leaves the table without keys unless saved scripts are replayed. Deferred: a benchmark decides whether it is worth adding as an opt-in.
3. **Load into a separate staging table and switch it in.** Twice the space, strict `SWITCH` rules (identical indexes, constraints, permissions, foreign keys), and the same final sort as option 2. Dropped for now. Staging tables come back under "Later" for a different purpose.

## Design

### 1. Choosing tables to split: `--split-tables`

`export` and `copy` get:

```
--split-tables <pattern>[:<parts>][,<pattern>[:<parts>]...]
```

- `<pattern>` is `schema.table` with `*` wildcards, matched like `--include-tables`. A table must also pass the include/exclude filters to be exported at all.
- `<parts>` is the number of ranges, 2 to 200. **Default: 8** when omitted. (8 is a reasonable degree of parallelism for one table on typical servers; 200 is the histogram's step limit.)
- The first matching pattern wins. Example: `--split-tables "dbo.History:32,Sales.Order*"`.
- A table may get fewer parts than asked when its histogram has fewer usable steps. The console says how many it got.
- `import` has no split option: it recognises parts from file names and the manifest.

### 2. Which tables can be split

A table can be split when it has a **clustered rowstore index** (`sys.indexes.type = 1`) whose **first key column** has a type that can be compared and passed as a parameter: integer types, `decimal`/`numeric`, `money`/`smallmoney`, date and time types, `char`/`varchar`/`nchar`/`nvarchar`, `uniqueidentifier`, `binary`/`varbinary`, `bit`. That column is the **split column**.

Partitioned tables follow section 11 instead. A table that matches `--split-tables` but cannot be split (unpartitioned heap or clustered columnstore, CLR or `sql_variant` split column, empty table, or no histogram) is exported **as one file, as today**, with a warning that says why.

When the clustered index is also the primary key (the common case), the parts are in exactly the order of today's single JSON file, so the parts joined in order equal the unsplit export. When the clustered index is not the primary key, each part is still sorted by the primary key as today, but the joined parts are not one sorted sequence. This is correct, just less diffable, and it is documented.

### 3. Range boundaries

Boundaries come from the **statistics histogram of the clustered index** (`sys.dm_db_stats_histogram(object_id, 1)`; the clustered index's statistics have `stats_id` 1). Each step gives `range_high_key`, `range_rows` and `equal_rows`. Cumulative row counts give the approximate rows up to each step key. For N parts, choose the N−1 step keys closest to `k × total / N`, keeping them distinct and ascending.

- No table scan is needed. Stale statistics make parts uneven, never wrong.
- Ranges are half-open on the split column:
  - part 1: `col < @b1`, plus `OR col IS NULL` when the column is nullable (NULLs sort first);
  - part k: `col >= @b(k-1) AND col < @bk`;
  - part N: `col >= @b(N-1)`.

  The first and last parts are open-ended, so rows outside the histogram's range are never lost.
- Boundaries are passed as **parameters typed like the column** (type, length, precision, scale). The comparison then uses the column's own collation, so string ranges follow the same order as the index.
- For a **descending** split column, parts are numbered in index order (part 1 holds the highest values), so the joined parts still follow the key order.
- With one dominant leading key value (for example one huge tenant in a `(TenantId, Id)` key), that value cannot be split. This is a known limitation; multi-column ranges are under "Later".

### 4. Consistency

Each part is read in its own transaction, at a slightly different moment. Without `--snapshot`, a row whose split-column value changes during the export can be missed or read twice, and parts reflect different moments. When any table is split and `--snapshot` is not given, `export` and `copy` print a warning recommending `--snapshot`. With `--snapshot`, all parts read the same database snapshot and are exact.

### 5. Export

- **Work items.** Every part of every split table, and every unsplit table, is one work item in a single pool of `--max-tasks` workers. Items are ordered **largest first**, using estimated rows: histogram rows for parts, `sys.partitions` for whole tables. That way the long-running parts of big tables start at once and small tables fill the gaps.
- **JSON.** Each part runs today's table query plus the range predicate (and `ORDER BY` the usual sort key) and writes `<schema>.<table>.part0001.json`, or `.json.gz` with `--compress gzip`. Each part file is a complete JSON array in today's row-per-line format. Compression happens inside each part's task, so gzip CPU is parallel too.
- **BCP.** Each part uses `bcp "<select with range predicate>" queryout <schema>.<table>.part0001.bcp`. bcp cannot take parameters, so boundary values are rendered as **typed T-SQL literals** (for example `CONVERT(datetime2(7), N'2026-01-01T00:00:00.0000000', 126)`, `N'...'` strings through `SqlLiteral`, `0x...` binary). One format file per table (`<schema>.<table>-c.fmt`), as today.
- **Retries.** Each part is wrapped in `WithRetries` and writes `.partial` then commits, as tables do today. A retry redoes one part, not the table.
- **Part numbering.** Four digits (`part0001` to `part0200`), so names sort in part order.

### 6. Manifest

A split table keeps one entry in `tables`:

- `name`, `rows` (sum of parts), `exported`, and `files` (all part files plus the format file for BCP). `verify` and `IsIntact` work unchanged on `files`.
- `orderedBy` as today (the sort key), so import can still give order hints per part.
- New `split` object:

  ```json
  "split": {
    "column": "HistoryId", "type": "bigint", "descending": false, "nullable": false,
    "boundaries": [100000000, 200000000],
    "parts": [ { "part": 1, "rows": 99876543, "files": ["dbo.History.part0001.json"] }, ... ]
  }
  ```

  Boundary values are written with the same JSON value writer as the data (strings, ISO dates, base64 binary, decimals as for data) and read back with `ReadJsonValue`, so they round-trip exactly.
- Each finished part updates its table's entry under the manifest's lock (a new `RecordPart`) and saves atomically, as `Record` does now. Parts of the same table finish concurrently, so this must merge, not replace.
- Unsplit tables have no `split` key, so their manifests are unchanged.

### 7. Resume

`--resume` reads `split` from the existing manifest and **reuses its boundaries** (data may have changed since), re-exporting only parts that are missing or fail the size check. A table is skipped only when all its parts are intact. Whenever the manifest records a split for a table, the manifest wins: a different part count in `--split-tables`, or no `--split-tables` at all, gets a warning and the recorded split is used. A table recorded unsplit is not split on resume either.

### 8. Import: finding parts

- `ImportBaseName` strips `.partNNNN` (after `.gz`), so all parts map to one table. Files are grouped by table and ordered by part number.
- When the manifest is present, it confirms the grouping and the split metadata. Without a manifest, parts are still grouped by name.
- `verify`'s check for unlisted data files (`IsExportDataFile`) and `IsImportableFile` recognise part files the same way.
- `--source-file` patterns work as today (`*.json` matches the parts).

### 9. Import: per-table steps and load modes

For each target table, in this order:

1. **Truncate** once, if `--truncate-target`, before any part loads. Today truncation runs per file, which with parts would wipe earlier parts.
2. **Disable indexes**, if `--disable-indexes`. The option applies to every imported table, split or not: it speeds up any large load into an indexed table. Disable every enabled **nonclustered** index (rowstore, columnstore, XML, spatial) except:
   - the primary key;
   - any index referenced by a foreign key (`sys.foreign_keys.key_index_id`), because disabling it would disable those foreign keys too.

   Unique indexes and unique constraints that are not referenced *are* disabled: uniqueness is then checked when they are rebuilt, and duplicates make the rebuild fail (reported as an error). The clustered index is never disabled. The disabled indexes are logged by name before loading.
3. **Choose the load mode** from the target as it is now (after step 2):

   | Target now | TABLOCK (default) | Mode |
   |---|---|---|
   | Heap with **no enabled indexes** (bare table, or all nonclustered disabled) | yes | **Parallel**: all parts at once; bulk-update locks are compatible, loads can be minimally logged |
   | Clustered columnstore | yes | **Parallel**, *if* the CI check shows concurrent TABLOCK bulk loads into a CCI do not block each other; otherwise sequential |
   | Clustered rowstore index, or heap with enabled nonclustered indexes | yes | **Sequential**: parts in key order in one task, order hint per part (v2.6.0 rules) |
   | Any | no (`--no-table-lock`) | **Parallel**: row/page locks, fully logged; the user chose concurrency over minimal logging |

   The decision is a pure function of (index shape, table lock), with unit tests.
4. **Load** the parts in the chosen mode. Batches and `--batch-size` apply per part, so `--batch-size 0` is one transaction per part, not per table. This is documented.
5. **Rebuild** each disabled index (`ALTER INDEX ... REBUILD`; a primary XML index before its secondary XML indexes) in a `finally`, so it also runs after a failed load. An index that fails to rebuild is reported as an error by name and left disabled. The rebuild runs in the task of the table's last part.

BCP import follows the same steps and modes (`bcp in` per part with `-h "TABLOCK"`, and `-h "ORDER(...)"` in sequential mode when the order rules allow it).

**Scheduling.** The same single pool as export. Parallel-mode parts are separate items; a sequential-mode table is one item that loads its parts in order. Steps 1 and 2 run before a table's first item starts, and step 5 after its last finishes (per-table countdown with `Interlocked`). Items are ordered largest file first.

### 10. Copy

- `--split-tables` works as in export, and the ranges are computed on the source (or on the snapshot with `--snapshot`).
- `--drop-target` / `--truncate-target` and table creation (`PrepareTargetTable`) run once per table, before its parts.
- **Target created by copy** (a heap until `CreateTargetIndexes` runs after the load): every part is a parallel stream, a range reader on the source feeding its own `SqlBulkCopy` (`EnableStreaming`, TABLOCK). Indexes, check constraints, foreign keys and triggers follow after all parts, exactly as now.
- **Existing target:** the same load-mode table as import (section 9; no `--disable-indexes` for copy in this design).
- Tables copied through bcp (CLR types) are not split.

### 11. Partitioned tables

**What is different about them.** Each partition is stored as its own B-tree, heap or columnstore. A range predicate on the partitioning column makes SQL Server read only the matching partitions (partition elimination). So reading one partition is a contiguous scan even when the partitioning column is *not* the first clustered key column, and even for heaps and clustered columnstore tables. The boundaries are exact (`sys.partition_range_values`, RANGE LEFT or RIGHT) and so are the row counts per partition (`sys.partitions`). No statistics are needed.

Methods considered, and what this design does with each:

| Method | What it gives | Decision |
|---|---|---|
| **Partition-aligned ranges** for export and copy | Exact, cheap ranges; also makes partitioned heaps and columnstore tables splittable | **In this design** |
| Load modes for **partitioned targets** | Correct parallel/sequential choice, lock escalation guidance | **In this design** (rules below) |
| **Partition switching** from staging tables | Truly parallel loads *with* indexes: each staging table is loaded and indexed on its own, then switched in (metadata only) | **Later**, as the first part of the staging-tables work |
| **Partition-scoped truncate and partial exports** (`TRUNCATE TABLE ... WITH (PARTITIONS (...))`, export only chosen partitions) | Reload or back up only recent partitions of a history table | **Later** |
| **Sub-splitting one huge partition** (histogram ranges inside it) | Parallelism when most rows sit in one partition | **Later** |
| `copy` **recreating partition functions and schemes** | Partitioned targets from `copy` (today copy creates them unpartitioned, indexes without their ON clause) | **Later**; documented as a limitation now |

**Partition-aligned ranges (export and copy).**

- When a table chosen by `--split-tables` is partitioned, its ranges follow **partition boundaries instead of the histogram**. The split column is the partitioning column; its type rules are those of section 2, and a persisted computed partitioning column is fine.
- **Grouping.** Contiguous partitions are grouped into the requested number of parts by their exact row counts, in partition order. Empty partitions join a neighbour. A partition larger than its fair share becomes a part of its own (not sub-split in this design). Asking for at least as many parts as there are non-empty partitions gives one part per partition.
- **Predicates** are ranges on the partitioning column (`>=`/`<` or `>`/`<=`, following RANGE LEFT or RIGHT; NULLs go to partition 1), not `$PARTITION.fn(col) = n`, so partition elimination does not depend on the optimizer recognising the function. As in section 3, the first and last parts are open-ended.
- **Eligibility.** Partitioned heaps and partitioned clustered columnstore tables become splittable, which section 2 otherwise excludes.
- **Order of the joined parts.** The parts joined in order equal the unsplit export only when the partitioning column is the first column of the primary key. Otherwise each part is still sorted by the primary key.
- **Manifest.** `split.kind` is `"partition"` (the histogram kind is `"histogram"`), with the partition function name, `rangeRight`, the boundary values, and the partition numbers in each part. Resume reuses them as in section 7.

**Partitioned targets (import and copy into existing tables).** The load-mode table in section 9 applies unchanged, with these notes:

- A partitioned heap with no enabled indexes loads all parts in parallel with TABLOCK (bulk-update locks), like any heap.
- A partitioned clustered rowstore table with TABLOCK still loads sequentially: TABLOCK locks the whole table, not one partition.
- With `--no-table-lock`, parts load in parallel. When the target's `LOCK_ESCALATION` is `AUTO`, locks escalate to the partition rather than the table, so parts that land in different partitions don't block each other. With the default `TABLE`, escalation attempts fail while other parts hold locks, and the loads carry on with row and page locks. bcpJson reports the setting in the log and recommends `AUTO`; it does not change the table.
- `--disable-indexes` disables and rebuilds whole indexes, all partitions at once: a disabled index cannot be rebuilt one partition at a time.
- When the target is partitioned with the same boundaries as the manifest's split, each part maps to whole target partitions. This design only logs that fact. Partition switching and partition-scoped truncate, which build on it, are under "Later".

### 12. Benchmark (decides option 2 later)

A manually triggered workflow (`benchmark.yml`, `workflow_dispatch`, not a required check) on the SQL Server 2022 container:

- **Data:** a history-like table, about 5 million rows by default with an input to change it: `bigint` clustered primary key, `datetime2`, a few varchar and decimal columns, and two nonclustered indexes.
- **Measurements:** unsplit vs 8-part export (JSON and BCP), and import into:
  - (a) the indexed table;
  - (b) the same with `--disable-indexes`;
  - (c) a bare heap;
  - (d) a bare heap followed by index creation (option 2 by hand).
- **Output:** a timings table in the job summary.

GitHub runners have few cores, so the numbers show direction, not absolute speed. The option 2 decision uses them.

### 13. Tests

- **Unit tests:**
  - parsing `--split-tables` (patterns, part counts, defaults, invalid values; a name containing `:` uses the last `:` followed by digits);
  - choosing boundaries from histogram rows (even and skewed data, fewer steps than parts, duplicate keys, descending);
  - grouping partitions into parts (empty partitions, one dominant partition, more parts than partitions) and predicates for RANGE LEFT and RIGHT;
  - building range predicates (nullable, first and last parts) and BCP literals per type;
  - part file names and `ImportBaseName`;
  - grouping and ordering parts;
  - the load-mode decision;
  - choosing which indexes to disable;
  - manifest `split` round trip, `RecordPart` merging and resume selection.
- **Integration, new `roundtrip.sh split` step, required on all three versions:**
  - a table of about 200,000 rows split into 8 parts: JSON and BCP export, `verify`, and the joined JSON parts must equal the unsplit export;
  - import into the same schema (sequential, order hint), a bare heap (parallel), and the indexed table with `--disable-indexes` (afterwards no index is disabled and the data matches);
  - resume after deleting one part (only that part is redone);
  - copy with split into a new database, then compared;
  - a split request for a heap (one file plus a warning);
  - a nullable split column with NULLs;
  - `rt.OrderedKey` (case-sensitive collation, descending second column; a tiny table, so fewer parts than asked);
  - the CCI parallel-load lock check;
  - a partitioned table (RANGE RIGHT on a date, clustered primary key `(Id, Date)`, so the partitioning column is not the leading key column, with empty partitions and NULL-free data), and a partitioned heap: split export, joined-row comparison, parallel import into a partitioned heap and sequential import into the partitioned clustered table.

## Defaults summary

| Setting | Default |
|---|---|
| Split | off; per table through `--split-tables` |
| Parts when not given | 8 (allowed 2 to 200) |
| Boundaries | partition boundaries for partitioned tables, else the statistics histogram of the clustered index |
| `--disable-indexes` | off |
| Load mode | automatic from the target's shape and table lock (section 9) |
| Missing `--snapshot` with splitting | warning |

## Decided by the owner

- Split by key range only; no row-count or size splitting for now.
- Splitting is chosen per table.
- Shape B uses option 1 (disable nonclustered indexes, keep the clustered index). Option 2 waits for the benchmark.
- Targets with no indexes or constraints must be handled (parallel mode).
- Staging tables and history-table loading are out of scope for now.

## Calls made in this design (owner to confirm while reading)

- `--split-tables pattern[:parts]` as the option shape, and 8 parts by default.
- Boundaries from the statistics histogram, with no scan fallback: a table without a histogram is not split.
- `--no-table-lock` loads parts in parallel even into a clustered index (fully logged).
- `--disable-indexes` applies to all imported tables, skips the primary key and indexes referenced by foreign keys, and does disable other unique indexes.
- A warning, not an error, when splitting without `--snapshot`.
- On resume, the manifest's recorded split always wins.
- The benchmark is a manual workflow, not part of required CI.
- Partitioned tables are split along partition boundaries (grouped, never sub-split); switching, partition-scoped truncate, partial exports and partitioned `copy` targets wait.

## Left to the builder

- How the worker pool is implemented (Parallel.ForEach over ordered work items, or a channel-based scheduler), as long as the ordering and per-table before/after steps hold.
- The exact JSON shape of `split` beyond the fields listed, and console message wording.
- Literal rendering per type for BCP `queryout`, within the anti-goals (exact ranges).
- Rebuild options (`SORT_IN_TEMPDB`, `MAXDOP`): server defaults unless the benchmark suggests otherwise.

## Later

- **Staging tables for large history tables:** load parts into staging tables and move them into the history table over time (switch-in or batched inserts). Raised by the owner on 2026-11-23, saved for later. For partitioned targets, start with **partition switching**: load and index one staging table per partition in parallel, add the matching check constraint, `ALTER TABLE ... SWITCH TO ... PARTITION n`. This needs empty target partitions, identical columns and aligned indexes, the same filegroup and a trusted check constraint.
- **Partition-scoped work:** `TRUNCATE TABLE ... WITH (PARTITIONS (...))` to reload only some partitions, and exporting only chosen partitions (incremental backups of history tables).
- Sub-splitting a dominant partition, and `copy` recreating partition functions and schemes (and filegroups) on the target.
- **Option 2** (drop to a heap, recreate the clustered index), if the benchmark shows it faster.
- An automatic threshold (`--split-above`), per-table `--disable-indexes`, and `--disable-indexes` for `copy` into existing tables.
- Multi-column ranges for skewed leading keys, and splitting unpartitioned heaps.
- Splitting by row count, if a use case appears.
