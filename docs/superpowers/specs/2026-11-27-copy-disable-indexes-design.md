# `copy --disable-indexes` design

Status: approved 2026-11-27. Short design (reuses `import --disable-indexes`).

## Intent

`import --disable-indexes` disables a target's nonclustered indexes during the load and rebuilds them afterwards; in benchmark run 45326185184 it loaded 5,000,000 rows in 30.1 s against 53.9 s for a plain load into the indexed table. `copy` into existing tables has no such option, and since v2.9.0 a `copy --load-as-heap` table that does not qualify (foreign-key referenced, columnstore, ...) loads with all its indexes. This adds `copy --disable-indexes` and makes it the `--load-as-heap` fallback, as on import.

## Goals

- `copy --disable-indexes` for existing target tables, split or not.
- `copy --load-as-heap`: tables that do not qualify, and unsplit tables, load as with `--disable-indexes` (as import does).
- One implementation of disable and rebuild, shared with import; import's behaviour and messages unchanged.

## Non-goals

- Tables copy creates: no effect (they load without indexes and get them afterwards).
- Per-table `--disable-indexes` (separate roadmap item).
- Requiring `--snapshot`: the primary key and foreign-key-referenced indexes stay enabled, so a row read twice is still rejected by the key or lands as an ordinary extra row; at worst a disabled unique index fails to rebuild, which is reported with its `ALTER INDEX ... REBUILD` command.

## Approaches

1. **Share import's disable/rebuild code (chosen).** `LoadPlanning.IndexesToDisable` / `RebuildOrder`, `DisableIndexes`, `RebuildIndexes`, `ReadIndexes`, `ReadTargetShape` already take a connection, an object id and a qualified name; only import's `RebuildDisabledIndexes` takes `ImportOptions`/`ImportTable`. Make that one take a connection string, object id, label and qualified name, and call it from copy's finish.
2. Copy the code into `Program.Copy.cs`. Rejected: two copies of the logic that decides which indexes a user's table loses during a load.

## Design

- **Option:** `copy --disable-indexes` (bool). HelpText as import's.
- **When:** an existing target (not created or recreated by this run), with `--disable-indexes`, or with `--load-as-heap` when the table is not loaded as a heap (not split, or `PlanHeapLoad` returned no plan). Memory-optimized targets: `{table}: memory-optimized; --disable-indexes ignored.` as on import.
- **Preparation** (`PrepareCopyTarget`, once before the first part, after copy's truncate): `IndexesToDisable(ReadIndexes(...))`, disable them, post `{table}: Disabled indexes: a, b.` (import's text). For a split table the load mode comes from `ReadTargetShape` with the disabled ids, so a heap whose nonclustered indexes are all disabled loads its parts in parallel; a clustered table still loads one part after another.
- **Finish** (`FinishCopiedTable`, once after the last part, also after failed parts and after a preparation that failed once indexes were disabled): rebuild the disabled ones in `RebuildOrder`; failures are errors with the command, the others are still rebuilt; `{table}: Rebuilt n index(es).`
- **`--load-as-heap` fallback message** changes from v2.9.0's `...; loading it with its indexes.` to import's `{table}: not loaded as a heap: {reason}; disabling indexes instead.`
- **Tests.** Unit: the option parses. CI (`roundtrip.sh split`): copy `--disable-indexes --truncate-target` into the existing tables of `BcpJsonSplitCopy` logs the disabled indexes and the rebuild for split.Big, every index ends enabled and identical (`heap_indexes` before/after), data identical; step 7's fallback greps use the new message.
- **Docs.** `docs/reference/copy.md` row; `docs/guides/large-tables.md` (copy section: `--disable-indexes`, the fallback); CLAUDE.md; roadmap.

## Left to the builder

- Name and file of the shared rebuild function; how `CopyTableContext` records the disabled indexes.
- Where in step 6/7 of `roundtrip.sh` the new copy run sits.
