# Large tables

Use this guide when a few big tables set the length of the whole run. It covers exporting, importing and copying them in parallel key ranges, and loading them into indexed targets quickly.

Normally one table is one unit of work: a single task reads it, writes its file and loads it. A table of hundreds of millions of rows therefore sets the length of the whole run, and a transient error redoes it from the start. `--split-tables` cuts the tables you name into **key ranges**, and each range is handled as its own task. Parts are exported, loaded and copied in parallel, and retried and resumed one by one. Tables you don't name are handled exactly as before.

## Steps

1. **Export the big table in parts.** Here a history table with an identity key is cut into 16 parts, exported 8 at a time:

   ```powershell
   bcpjson export --source-server sql01 --source-database Sales --file-format json --export-path D:\exports\Sales `
                  --include-tables dbo.OrderHistory --split-tables dbo.OrderHistory:16 --snapshot --max-tasks 8
   ```

   Use `--snapshot`. Without it the parts of one table are read at different moments, so rows that change during the run can be missed or read twice, and bcpJson warns about it.

2. **Restore it.** Load the parts in parallel, with the nonclustered indexes rebuilt once at the end:

   ```powershell
   bcpjson import --file-format json --source-path D:\exports\Sales --source-file "*.json" `
                  --target-server sql02 --target-database Sales --truncate-target --disable-indexes --max-tasks 8
   ```

   If the table has a clustered index and the target is empty, `--load-as-heap` is faster still. See [Load as a heap](#load-as-a-heap---load-as-heap).

## Choosing tables and parts

- **Syntax:** `--split-tables` takes comma-separated entries of the form `schema.table[:parts]`.
  - `*` is a wildcard, for example `dbo.*History:16`.
  - The number of parts is 2 to 200, and defaults to 8.
  - The first entry that matches a table wins.
  - An entry that ends in `:` or asks for a part count outside 2 to 200 is rejected.
- **Not with queries:** on `export` it cannot be combined with `--source-query`.
- **Parallelism:** `--max-tasks` limits how many parts, and unsplit tables, run at once. The largest are started first.
- **Unmatched entries:** an entry that matches none of the exported or copied tables is reported (`--split-tables <pattern>: no table matched.`), so a typo does not go unnoticed.

Splitting pays off for tables of tens of millions of rows or more, on a server that can feed several readers at once. For small tables it only adds files.

## How the ranges are chosen

- **Partitioned tables** are cut at their partition boundaries. Neighbouring partitions are grouped so each part holds about the same number of rows, using exact row counts read from the server. A single partition is never cut further, so a table with fewer partitions than the parts you asked for gets fewer parts.
- **Other tables** are cut on the first column of the clustered rowstore index, using the statistics histogram of that index.
  - **Number and date/time keys:** the boundaries are interpolated inside the histogram steps. So a key that grows evenly still gets the number of parts you asked for. An identity column is a typical example: the histogram describes it with only a handful of steps.
  - **Text, `uniqueidentifier` and binary keys** use the histogram's own step values, so they may get fewer parts than requested.

The ranges together cover the table exactly once, including NULL keys. Out-of-date statistics make the parts uneven in size, never wrong. Run `UPDATE STATISTICS` on a table whose parts come out very uneven.

## Tables that are not split

A table that cannot be split is exported whole, with a warning of the form `<table>: not split: <reason>.`, and the run carries on. The reasons are:
- a heap or clustered columnstore table that is not partitioned;
- a split column of a type that cannot be cut, or a `varchar` key whose code page differs from the database's;
- no statistics histogram (run `UPDATE STATISTICS`);
- too few distinct key values;
- a NULL partition boundary;
- a split column whose name contains `"`, `\` or control characters;
- a table with a sparse column set;
- a graph table;
- any error while planning.

A key where one value holds most of the rows cannot be cut inside that value, so such a table gets uneven parts. For `copy`, tables with geography, geometry or hierarchyid columns (copied through bcp) are never split.

## Part files

- **JSON:** a split table writes `<schema>.<table>.part0001.json` (`.json.gz` with `--compress gzip`), `.part0002.json` and so on, each holding one key range.
- **Row order:** when the split column leads the primary key, the parts in order hold exactly the rows of the single file. Otherwise, as with a partitioned table split on its partitioning column but sorted by another key, they hold the same rows in a different order.
- **BCP:** parts are `<schema>.<table>.part0001.bcp` and so on. They share one `<schema>.<table>-c.fmt`, and each part has its own `_error.log` and `_output.log`.

In the manifest the table stays one entry, with a `split` object recording how it was cut; see [The split object](../reference/manifest.md#the-split-object). `verify` reports `<table>: <k> of <n> parts exported.` when parts are missing.

**Stale files.** Export never deletes files. Re-exporting into a folder that holds an earlier export can leave data files behind, for example:
- fewer parts than before;
- other boundaries;
- a table that was split and now is not, or the other way round.

Export names each such file: `<table>: <file> is left from an earlier export; delete it (import ignores it while the manifest is present).` For how import chooses files when old ones are still there, see [How import uses the manifest](../reference/manifest.md#how-import-uses-the-manifest).

## Import the parts

`import` groups the files by table, so `--source-file "*.json"` picks up every part. It truncates each table once (with `--truncate-target`) before any part loads, then loads the parts into it.

Whether the parts can load **in parallel** depends on the target, because SQL Server lets several table-locked bulk loads run side by side only in some cases. bcpJson looks at the target and reports its choice per table: `<table>: <n> parts, loading in parallel.` or `<table>: <n> parts, loading one after another (<reason>).`

| Target table | With the table lock (default) | With `--no-table-lock` |
|---|---|---|
| Heap, or a table with no enabled index | Parallel (bulk-update locks do not conflict) | Parallel |
| Clustered rowstore index | One after another (`clustered index`; the table lock is exclusive) | Parallel |
| Heap with enabled nonclustered indexes | One after another (`enabled nonclustered indexes`) | Parallel |
| Clustered columnstore index | One after another (`columnstore`; measured in CI: a columnstore load takes S and X locks, not bulk-update) | Parallel |
| Table with geography/geometry/hierarchyid columns (JSON) | One after another (`CLR columns`; loaded through a staging table that always takes a table lock) | One after another |

- **Part order:** when parts load one after another they go in part (key) order. A clustered table therefore receives sorted rows when the split column is the leading column of its clustered key.
- **Partitioned targets:** for a partitioned target with `--no-table-lock`, bcpJson suggests `ALTER TABLE ... SET (LOCK_ESCALATION = AUTO)`. That lets parts in different partitions load without blocking each other.
- **Batches:** `--batch-size 0` is one transaction per part, not per table. BCP parts get no order hint, because BCP exports are unordered.

## Load first, index afterwards: `--disable-indexes`

**Applies to:** `import` (JSON and BCP), all tables of the run. **Default:** off.

The tables that matter most for a restore are those with many nonclustered indexes, and every index slows every row of the load. `--disable-indexes` is the "load first, index afterwards" restore:
- before a table's first part, it disables the table's nonclustered indexes;
- after the last part, it rebuilds them, once each.

The gain is that the load maintains fewer indexes. Whether the parts then load in parallel still follows the [table above](#import-the-parts). A heap whose indexes are all disabled loads its parts in parallel. A table with a clustered primary key, which is never disabled, still loads one part after another.

- **Not disabled:** the primary key, the clustered index, and any index a foreign key references, because disabling those would block the load or the keys. Unique indexes are disabled and checked again on rebuild, so duplicate rows fail the rebuild, not the load.
- **Always rebuilt:** the rebuild runs even when the load failed part way, so the table is not left with disabled indexes. An index that cannot be rebuilt is reported as an error by name, together with the `ALTER INDEX ... REBUILD` command to run by hand. The others are still rebuilt.
- **Memory-optimized tables** are skipped: `<table>: memory-optimized; --disable-indexes ignored.`
- **Duplicate targets:** two groups of files that resolve to the same target table (for example `T.json` and `dbo.T.json`) are refused.

On the CI benchmark, loading 1,000,000 rows into an indexed table took 4.8 s with `--disable-indexes`, against 7.6 s with the indexes in place. Loading the same data into a bare heap took 2.9 s. Splitting a 1,000,000-row export into 8 parts took 2.1 s instead of 2.5 s for JSON, and 1.0 s instead of 1.3 s for BCP. These ran on a GitHub runner, so they are indicative only, and the gains grow with the table, the server and the number of readers it can feed. A benchmark workflow ([`benchmark.yml`](https://github.com/chrisgelhaus/BCPJSON/blob/master/.github/workflows/benchmark.yml), run manually) reproduces these numbers on any row count.

## Load as a heap: `--load-as-heap`

**Applies to:** `import` (JSON and BCP), split tables only. **Default:** off.

A table with a clustered index takes its parts one after another, even with `--disable-indexes`. `--load-as-heap` does the following instead:
1. It scripts every index of the table with SMO from the target, before anything is dropped.
2. It drops **every** index: the clustered one and the nonclustered ones, primary key and unique constraints included.
3. It loads all parts in parallel into the resulting heap.
4. It creates the clustered index and the others again, with the same names, columns, uniqueness, filters, compression, fill factor and filegroup or partition scheme.

Use it to restore a big table into an empty target that already has its keys and indexes.

```powershell
bcpjson import --file-format json --source-path D:\exports\Sales --source-file "*.json" `
               --target-server sql02 --target-database Sales --load-as-heap --max-tasks 8
```

- **Which tables:** the option applies to a table only when the import has two or more files for it (part files). Every other table of the run, and every table that falls back as described below, loads as with `--disable-indexes`. A target that is already a heap needs no change and gets no message.
- **Messages:** before the parts load, bcpJson logs `<table>: <k> index(es) dropped; loading <n> parts in parallel as a heap.` Afterwards it logs `<table>: clustered index <name> and <k> other index(es) recreated.` The indexes are recreated even when the load failed.
- **Empty target only:** a table with rows falls back; with `--truncate-target` it is emptied first. Rows that arrive between planning and the drop fail the table: `<table>: rows were added after planning; not loaded.`
- **Hypothetical indexes**, left by the Database Engine Tuning Advisor, are neither dropped nor recreated.

### When a table falls back

The warning is `<table>: not loaded as a heap: <reason>; disabling indexes instead.` The first reason that applies is reported, in this order:

| Reason | Why |
|---|---|
| `--no-table-lock` | The heap load relies on the table lock. |
| `CLR columns` | JSON import of geography/geometry/hierarchyid goes through a staging table. |
| `memory-optimized` | Not supported. |
| `clustered columnstore` | Loaded one after another for a different reason; rebuilding one is a different trade-off. |
| `referenced by foreign key <name>` | Any foreign key, including the table's own, that references the table: its keys cannot be dropped. |
| `XML or spatial index <name>` | These need a clustered primary key and cannot exist on a heap. |
| `disabled index <name>` | Recreating it would enable it. |
| `indexed view <name>` | An indexed view references the table. |
| `temporal table` | System-versioned, or a history table. |
| `change tracking`, `replication`, `change data capture` | Enabled on the table. |
| `FILESTREAM` | The table has FILESTREAM data. |
| `full-text index` | The table has one. |
| `not empty` | The target has rows and `--truncate-target` is not set. |
| `index definition too long: <name>` | Each index statement is also saved in a marker and must fit in 3,500 characters. |
| `cannot write <path>: <message>` | The recovery script could not be written to `--source-path`. |

### If an index cannot be recreated

Before it drops anything, bcpJson takes two safeguards:
- a recovery script, `<schema>.<table>.load-as-heap.sql`, in `--source-path` (import) or `--recovery-path` (copy);
- one marker per index on the table, in extended properties.

After the last part it creates the clustered index first. If that fails it stops, because nonclustered indexes built on a heap would be rebuilt again when the clustered index comes back. Then it creates each missing nonclustered index.

A failure is an error: `<table>: Could not recreate <name>: <reason>. Run the script in <path>.` The other indexes are still created. The usual reason is duplicate keys in the loaded data, and the error names the key or unique index. What to do next is in [The load-as-heap recovery script](recover-a-failed-run.md#the-load-as-heap-recovery-script).

### Edition and benchmark

Building an index uses several threads only on Enterprise and Developer editions. On other editions SQL Server builds each index on one thread, so recreating the indexes takes longer and the gain over `--disable-indexes` is smaller than below.

The CI benchmark loaded 5,000,000 rows into a table with a bigint identity clustered primary key and two nonclustered indexes, from 8 JSON parts. It ran on SQL Server 2022 Developer on a 4-core runner, so the numbers are indicative only:
- loading into the indexed table took 53.9 s;
- `--disable-indexes` took 30.1 s;
- loading into a heap and building the indexes, done by hand, took 20.4 s: 12.9 s for the load, 3.2 s for the clustered key, and 1.7 s and 2.6 s for the nonclustered indexes.

At 1,000,000 rows the two options were even (5.0 s against 4.8 s), so the gain grows with the table. The benchmark workflow also has an `import indexed, --load-as-heap` row, but it has not been run yet, because the workflow runs only from master.

## Copy

With `--split-tables`, `copy` streams each key range from the source straight into the target, several at once and without files in between:

```powershell
bcpjson copy --source-server sql01 --source-database Sales --target-server sql02 --target-database Sales `
             --include-tables dbo.OrderHistory --split-tables dbo.OrderHistory:16 --drop-target --snapshot --max-tasks 16
```

- **New tables:** when `copy` creates the table, it first loads it as a heap, so the parts load in parallel. It builds the indexes after every part has arrived, as for any copy.
- **Existing tables:** the load-mode rules in [Import the parts](#import-the-parts) apply, unless `--load-as-heap` loads them as heaps (below).
- **Failed parts:** keys, indexes and check constraints are created only when every part succeeded. Otherwise an error says `<table>: copy incomplete (<k> of <n> parts failed)`, and the table needs to be copied again.

**Existing tables as heaps.** `copy --load-as-heap` works as [Load as a heap](#load-as-a-heap---load-as-heap) does for import, with the same rules for which tables qualify. Copy has no export folder, so `--recovery-path` names the folder for the recovery scripts. It is required with `--load-as-heap`, and created if missing.

```powershell
bcpjson copy --source-server sql01 --source-database Sales --target-server sql02 --target-database Sales `
             --include-tables dbo.OrderHistory --split-tables dbo.OrderHistory:16 --truncate-target --snapshot `
             --load-as-heap --recovery-path D:\bcpjson\recovery --max-tasks 16
```

- **Tables that don't qualify** load with their indexes, as without the option, and warn: `<table>: not loaded as a heap: <reason>; loading it with its indexes.` copy has no `--disable-indexes`.
- **No effect** on tables copy creates (they load as heaps anyway), on unsplit tables and on tables copied through bcp.
- **One folder per target database:** the script is named `<schema>.<table>.load-as-heap.sql`, without the database. Two copies into different databases with the same `--recovery-path` replace each other's scripts.

## If something goes wrong

- **A part failed during export:** each part is retried on its own, and `export --resume` redoes only the missing parts. See [Resume an interrupted export](recover-a-failed-run.md#resume-an-interrupted-export).
- **Import refuses a table:** see [How import uses the manifest](../reference/manifest.md#how-import-uses-the-manifest) for files left from older exports and unfinished exports.
- **An index could not be rebuilt or recreated:** see [Recover a failed run](recover-a-failed-run.md).

## See also

- [export](../reference/export.md), [import](../reference/import.md#loading-options), [copy](../reference/copy.md)
- [The split object](../reference/manifest.md#the-split-object)
- [Known limitations: split tables](../reference/limitations.md#split-tables)
