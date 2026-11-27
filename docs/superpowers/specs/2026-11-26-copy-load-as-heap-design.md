# `copy --load-as-heap` design

Status: approved 2026-11-26. Short design (a contained change that reuses `import --load-as-heap`).

## Intent

`import --load-as-heap` drops a split table's indexes, loads the parts in parallel into the heap and recreates the indexes. In benchmark run 45326185184 (5,000,000 rows) this was 2.6x a plain load into the indexed table and 33% faster than `--disable-indexes`. `copy` already loads tables it creates heap first, but an existing target with a clustered index is loaded one part after another. This brings the import fast path to `copy` into existing tables.

## Goals

- `copy --load-as-heap` for existing targets of split tables.
- Same safety as import: recovery script written before anything is dropped, markers on the table, indexes recreated after the last part even after a failure, marked tables refused.
- One implementation of the drop-and-rebuild logic, shared by import and copy.

## Non-goals

- `copy --disable-indexes` (stays on the roadmap). A copy table that cannot load as a heap loads normally, not with disabled indexes.
- Unsplit tables, tables copy creates (already heap first, including those `--drop-target` replaces), and tables copied through bcp (never split): `--load-as-heap` has no effect on them.
- Any change to import's behaviour or messages.

## Approaches

1. **Share import's code through a small target description (chosen).** The heap-load functions take a description of the target (connection string, schema, table, object id, label, truncate, table lock, script folder) instead of `ImportOptions` / `ImportTable`. Copy decides in its per-table preparation (`PrepareCopyTarget`, `TableWork` before), the first point where it knows the target's state; the finish (`FinishCopiedTable`, `TableWork` after) recreates the indexes.
2. Copy the heap-load code into `Program.Copy.cs`. Rejected: two versions of the logic whose slip costs a user their indexes.
3. Decide while planning (`PlanCopyTable`), as import does. Rejected: copy plans on the source, before the target is dropped or created; with `--drop-target` the decision would concern a table about to disappear.

## Design (decided with the owner)

- **Options.** `copy --load-as-heap`; `copy --recovery-path <folder>`, required with `--load-as-heap` (`Valid()` refuses otherwise). The folder is created if missing. The recovery script is `HeapLoadScript.FileName(schema, table)` in that folder, the same script import writes.
- **When it applies.** An existing target (after `--drop-target` handling), table split into 2+ parts, `--load-as-heap` given. The decision is `LoadPlanning.DecideHeapLoad` with copy's facts: table lock (`--no-table-lock` disqualifies), no staged CLR columns (copy has none: CLR-spatial tables go through bcp and are not split), `--truncate-target`.
- **Order in the preparation.** Refuse a marked target; decide; script the indexes and write the recovery script; truncate (`--truncate-target`); check empty; set the markers; drop the indexes; load mode parallel. A failure before the drop deletes the script, as on import.
- **Finish.** After the last part, whether or not parts failed: recreate missing indexes (clustered first), remove the markers and delete the script only when every saved index exists; failures are reported with the script path. On a table copy created, nothing changes (keys, indexes, check constraints as today).
- **Messages.**
  - Not qualifying: `{table}: not loaded as a heap: {reason}; loading it with its indexes.` (warning), then today's load (sequential with a clustered index).
  - Every copy, with or without the option, refuses an existing target that carries markers, unless `--drop-target` drops it: the error names the markers and, when `--recovery-path` is given, the script path there.
  - Import's messages stay as they are.
- **Tests.**
  - Unit: `--load-as-heap` without `--recovery-path` is rejected; the option parses.
  - CI (`roundtrip.sh split`): copy into an existing indexed target with `--split-tables`, `--truncate-target`, `--load-as-heap`, `--recovery-path`; data identical (JSON export diff), every index back with the same definition, markers and script gone. A table referenced by a foreign key falls back with the warning. A hand-marked target (extended property) is refused.
- **Docs.** Rows for both options in `docs/reference/copy.md`; a section in `docs/guides/large-tables.md`; a note in `docs/guides/recover-a-failed-run.md`; CLAUDE.md and `docs/ROADMAP.md`.

## Left to the builder

- The exact shape and name of the shared target description, and how the marker refusal's message reads when no `--recovery-path` is given.
- Whether the shared functions move to a new file or stay in `Program.HeapLoad.cs`.
- How the CI step compares index definitions (for example, scripting `sys.indexes` / `sys.index_columns` before and after).
