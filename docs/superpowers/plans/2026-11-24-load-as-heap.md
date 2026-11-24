# import --load-as-heap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `import --load-as-heap`: split tables bound for a clustered rowstore target load in parallel into a heap, then get their clustered index and other indexes back, with a script file and a table marker as the safety net.

**Architecture:** Pure rules (which tables qualify, which indexes to disable, recovery script name and text) live in `LoadPlanning.cs` and a new `HeapLoadScript.cs` with unit tests. Everything that touches SQL Server lives in a new `Program.HeapLoad.cs` and is wired into the existing import planning (`PlanImportTable`) and the per-table `TableWork` before/after hooks. CI proves it in `roundtrip.sh split`, after a probe confirms the SQL Server behaviour the step order relies on.

**Tech Stack:** .NET 8, Microsoft.Data.SqlClient 7, SMO 181, CommandLineParser 2.9, xUnit; bash + sqlcmd + jq for CI; SQL Server 2019/2022/2025 containers.

**Spec:** `docs/superpowers/specs/2026-11-23-load-as-heap-design.md`

## Global Constraints

- CLAUDE.md rules: `QuoteName()` / `SqlLiteral()` for identifiers and literals, parameters where possible; `Interlocked` for shared counters; errors via `PostToConsole(..., InfoLevel.Error)` (exit code 1), fallbacks as `InfoLevel.Warning`; `CommandTimeout = 0` for index builds; connections only from `SqlConnectionFactory.Create()`.
- C# files UTF-8 with BOM and CRLF; `Nullable` disabled; helpers that need tests are `internal`.
- Option: `--load-as-heap` (bool, default false). Help text, verbatim: "For split tables loaded into a table with a clustered index: drop the clustered index, load the parts in parallel into the heap, then recreate it and rebuild the other indexes. Implies --disable-indexes."
- Applies to JSON and BCP imports, only to tables with two or more files; a table with one file is loaded as with `--disable-indexes`. All names use the *target* table (after `--target-table`).
- Recovery script name `<schema>.<table>.load-as-heap.sql` in `--source-path`; marker = extended property `bcpjson_load_as_heap` on the table, value = the clustered index statement (nvarchar); statement limit 3,500 characters.
- Messages, verbatim (placeholders in braces):
  - `{table}: not loaded as a heap: {reason}; disabling indexes instead.`
  - `{table}: clustered index {name} dropped; loading {n} parts in parallel as a heap.`
  - `{table}: Could not recreate {name}: {reason}. Run the script in {path}.`
  - `{table}: clustered index {name} recreated; rebuilt {k} index(es).`
  - `{table}: rows were added after planning; not loaded.`
  - `{table}: an interrupted --load-as-heap run left it without its clustered index; run the script in {file} (or the statement in its extended property bcpjson_load_as_heap) and ALTER INDEX ... REBUILD the disabled indexes, then import again.`
  - `{file} is left over; the target is whole, delete it.`
- Fallback reasons, verbatim, checked in this order: `--no-table-lock`, `CLR columns`, `memory-optimized`, `clustered columnstore`, `referenced by foreign key {name}`, `XML or spatial index {name}`, `indexed view {name}`, `temporal table`, `change tracking`, `replication`, `change data capture`, `FILESTREAM`, `full-text index`, `not empty`, `clustered index definition too long`, `cannot write {path}: {message}`. A target that is a heap (no clustered index) gets no message and no change.

## Review Focus

1. A target whose schema or table name contains `]`, `'`, a space or non-ASCII characters: every DROP/CREATE/ALTER and the extended-property calls must quote correctly (parameters for `sp_addextendedproperty` arguments), and the script file name must still be the plain `schema.table` text. Test: Task 2 `FileName` with `we]ird` and `Tab le`; Task 4 reviewer check that no statement concatenates a raw name.
2. `--target-table` mapping parts of `split.Big` into `split.BigOther` (clustered index that is not a constraint): the plain `DROP INDEX` / `CREATE CLUSTERED INDEX` path, with script and marker named after `split.BigOther`. Test: Task 4 case (g).
3. Anything failing part way through "before" (after the marker is set but before or during the drop): "after" still restores, deletes nothing it should keep, and the marker stays only if the clustered index is not back. Test: Task 4 case (e) (corrupt part) plus Task 3's rule that "after" decides from what exists, not from what "before" claims to have done.
4. Source folder read-only: the table falls back with `cannot write ...` and is loaded with `--disable-indexes`, target never left as a heap. Test: Task 4 case (h).
5. A leftover script file for a table that is whole (marker absent) never blocks an import, and the script is never loaded as data even with `--source-file '*'`. Test: Task 2 `IsImportableFile` and Task 4 case (i).

---

### Task 1: CI probe of disabled indexes across drop and create of the clustered index

**Files:**
- Create: `tests/integration/heap_probe.sql`
- Modify: `.github/workflows/ci.yml` (new step after "Bulk load lock modes")

**Interfaces:**
- Produces: CI step named `Clustered index drop keeps disabled indexes`, which runs `sqlcmd -S "$SQL_SERVER" -U sa -P "$SQL_PASSWORD" -C -b -I -i tests/integration/heap_probe.sql` with the same `if:` condition as the lock-probe step.

- [ ] **Step 1: Write `heap_probe.sql`.** Creates and finally drops database `bcpjson_heapprobe`. Case A: table with `PRIMARY KEY CLUSTERED (Id)`, a nonclustered index and a unique nonclustered index, 1,000 rows. Disable both nonclustered indexes; `ALTER TABLE ... DROP CONSTRAINT` the PK; assert both still exist in `sys.indexes` with `is_disabled = 1` and the table is a heap (`index_id = 0` row); insert 1,000 more rows; `ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY CLUSTERED (Id)`; assert both still `is_disabled = 1`; `ALTER INDEX ... REBUILD` each; assert `is_disabled = 0` and `COUNT(*) = 2000`. Case B: table with a nonclustered `PRIMARY KEY (Id)` and a `CREATE CLUSTERED INDEX` on another column: disable the PK, `DROP INDEX` the clustered index, assert the PK is still `is_disabled = 1`; insert; `CREATE CLUSTERED INDEX` again; assert PK still disabled; rebuild; assert enabled. Every failed assertion is `THROW 50000, '<what was expected>', 1;`. End with `PRINT 'heap probe: disabled indexes stay disabled across drop and create of the clustered index';`.
- [ ] **Step 2: Add the CI step** and push the branch; watch the "Round trip against SQL Server <version>" jobs on 2019, 2022 and 2025.
  Run: `gh pr checks <pr> --watch` (open a draft PR `feature/load-as-heap` → master if none exists)
  Expected: the new step passes on all three versions. **If it fails on any version, stop and report: the design's step order depends on it (spec "Tests", last bullet).**
- [ ] **Step 3: Commit** `CI: probe that disabled indexes survive dropping and recreating the clustered index`.

### Task 2: Pure rules: qualification, indexes to disable, recovery script

**Files:**
- Modify: `src/BcpJson/LoadPlanning.cs`
- Create: `src/BcpJson/HeapLoadScript.cs`
- Modify: `src/BcpJson/Program.Import.cs` (`IsImportableFile` only)
- Test: `tests/BcpJson.Tests/LoadPlanningTests.cs`, create `tests/BcpJson.Tests/HeapLoadScriptTests.cs`

**Interfaces:**
- Produces (in `LoadPlanning.cs`, namespace `bcpJson`):
  ```csharp
  /// Catalog facts about one target, read once at planning (null name = none).
  public sealed record HeapLoadFacts(bool ClusteredRowstore, bool ClusteredColumnstore, bool MemoryOptimized,
      string ReferencingForeignKey, string XmlOrSpatialIndex, string IndexedView, bool Temporal,
      bool ChangeTracking, bool Replicated, bool ChangeDataCapture, bool Filestream, bool FullText, bool Empty);
  /// LoadAsHeap true: drop and reload. False with Reason null: nothing to say (heap target). False with Reason: warn.
  public sealed record HeapLoadDecision(bool LoadAsHeap, string Reason);
  internal const int MaxClusteredStatementLength = 3500;
  internal static HeapLoadDecision DecideHeapLoad(HeapLoadFacts facts, bool tableLock, bool stagedClrColumns,
      bool truncateTarget, int clusteredStatementLength);
  internal static List<IndexInfo> IndexesToDisableForHeapLoad(IEnumerable<IndexInfo> indexes);
  ```
  `DecideHeapLoad` checks in the Global Constraints order; `cannot write ...` is not decided here (Task 3 decides it after trying to write). A target with neither clustered rowstore nor columnstore returns `(false, null)` before any other check. `not empty` applies only when `truncateTarget` is false. `IndexesToDisableForHeapLoad` returns every enabled index with `IndexId > 1`, primary keys and unique constraints included.
- Produces (`HeapLoadScript.cs`, `internal static class HeapLoadScript`):
  ```csharp
  internal const string Suffix = ".load-as-heap.sql";
  internal const string MarkerName = "bcpjson_load_as_heap";
  internal static string FileName(string schema, string table);          // schema + "." + table + Suffix
  internal static string Build(string label, DateTime utc, string clusteredStatement, IEnumerable<string> rebuildStatements);
  ```
  `Build` text: a `--` comment block (written by bcpJson import --load-as-heap for `{label}` at `{utc:yyyy-MM-dd HH:mm:ss} UTC`; run this script if the import was interrupted), then the clustered statement, `GO`, then each rebuild statement followed by `GO`.

- [ ] **Step 1: Write failing tests.**
  - `Heap_load_decision` (Theory over a facts builder): clustered with everything clear → `(true, null)`; heap → `(false, null)`; columnstore → `(false, "clustered columnstore")`; `tableLock: false` → `"--no-table-lock"`; staged → `"CLR columns"`; `ReferencingForeignKey = "FK_X"` → `"referenced by foreign key FK_X"`; `XmlOrSpatialIndex = "SIX_G"` → `"XML or spatial index SIX_G"`; `IndexedView = "dbo.V"` → `"indexed view dbo.V"`; each bool fact → its reason; `Empty = false` → `"not empty"`, but `(true, null)` with `truncateTarget: true`; length 3501 → `"clustered index definition too long"`, 3500 → `(true, null)`.
  - `Heap_load_reason_precedence`: no table lock + memory-optimized + FK → `"--no-table-lock"`; memory-optimized + FK → `"memory-optimized"`.
  - `Indexes_to_disable_for_heap_load`: from clustered PK (id 1), NC PK (id 2, pk), disabled NC (id 3), NC unique (id 4), NC columnstore (id 5, type 6) → ids `{2, 4, 5}`.
  - `HeapLoadScriptTests.File_name`: `("split", "Big")` → `"split.Big.load-as-heap.sql"`; `("we]ird", "Tab le")` → `"we]ird.Tab le.load-as-heap.sql"`.
  - `HeapLoadScriptTests.Build_orders_statements`: clustered statement first, then rebuilds in the given order, each followed by a line `GO`; the label appears in the header.
  - `ImportFileTests` (or the existing test class for `IsImportableFile`): `"split.Big.load-as-heap.sql"` is not importable; `"split.Big.part0001.json"` still is.
- [ ] **Step 2: Run** `dotnet test BcpJson.sln` — expect the new tests to fail to compile.
- [ ] **Step 3: Implement** the records and methods above; `IsImportableFile` also rejects names ending in `HeapLoadScript.Suffix` (ordinal ignore case).
- [ ] **Step 4: Run** `dotnet test BcpJson.sln` — all pass.
- [ ] **Step 5: Commit** `Load as heap: qualification rules and recovery script text`.

### Task 3: The option, planning, and the before/after steps

**Files:**
- Modify: `src/BcpJson/ImportOptions.cs`
- Create: `src/BcpJson/Program.HeapLoad.cs`
- Modify: `src/BcpJson/Program.Import.cs` (`PlanImportTable`, `PrepareImportTarget` / `RebuildDisabledIndexes` wiring)
- Test: `tests/BcpJson.Tests/CommandLineTests.cs`

**Interfaces:**
- Consumes: Task 2's `HeapLoadFacts`, `DecideHeapLoad`, `IndexesToDisableForHeapLoad`, `HeapLoadScript`; existing `ReadIndexes`, `DisableIndexes`, `RebuildIndexes`, `RebuildNotStartedMessage`, `TableWork`, `ExecuteSql`.
- Produces: `ImportOptions.LoadAsHeap` (`[Option("load-as-heap")]`, help text from Global Constraints). In `Program.HeapLoad.cs`:
  ```csharp
  private sealed class HeapLoadPlan { public string ClusteredName; public bool IsConstraint; public string ClusteredStatement;
      public string ScriptPath; public List<IndexInfo> ToDisable; }
  static HeapLoadFacts ReadHeapLoadFacts(SqlConnection conn, int objectId);       // one catalog batch
  static string ScriptClusteredIndex(ImportOptions options, string schema, string table, out string name, out bool isConstraint);
  static string ReadHeapLoadMarker(SqlConnection conn, string schema, string table); // null when absent
  static void SetHeapLoadMarker(SqlConnection conn, string schema, string table, string statement);
  static void RemoveHeapLoadMarker(SqlConnection conn, string schema, string table);
  static void PrepareHeapLoad(ImportOptions options, ImportTable table, HeapLoadPlan plan, int parts);  // TableWork before
  static void FinishHeapLoad(ImportOptions options, ImportTable table, HeapLoadPlan plan);              // TableWork after
  ```

- [ ] **Step 1: Failing test** in `CommandLineTests`: `import --load-as-heap ...` parses with `LoadAsHeap == true`; default false.
- [ ] **Step 2: Implement planning** in `PlanImportTable`, right after the target is resolved, for *every* import:
  - `ReadHeapLoadMarker` non-null → error (marker message, `{file}` = full path of `HeapLoadScript.FileName` in `--source-path`), return no items.
  - Else, if that script file exists in `--source-path` → warning `{file} is left over; the target is whole, delete it.`
  Then, when `options.LoadAsHeap` and the table has two or more files: read facts; script the clustered index (SMO from the target database, filegroup/partition scheme and compression kept: do **not** set `NoFileGroup`; join the statements with newlines); `DecideHeapLoad`. If it says load as heap, write the recovery script (`HeapLoadScript.Build`, rebuild statements `ALTER INDEX [x] ON [s].[t] REBUILD;` for `IndexesToDisableForHeapLoad` in `LoadPlanning.RebuildOrder`) to `--source-path`; on an exception the reason becomes `cannot write {path}: {message}`. Any reason → the fallback warning and the table continues exactly as `--disable-indexes` (use `LoadPlanning.IndexesToDisable` as today). `LoadAsHeap` implies `DisableIndexes` for every table, including single-file ones.
  When loading as a heap, the load mode is computed from `new TargetShape(false, false, 0, shape.Partitioned)` (parallel), and the table's `TableWork` gets `PrepareHeapLoad` / `FinishHeapLoad` instead of `PrepareImportTarget` / `RebuildDisabledIndexes`.
- [ ] **Step 3: Implement `PrepareHeapLoad`** (one connection, in order): truncate when `--truncate-target` (same message as today); if `SELECT TOP (1) 1` finds rows → delete the script file, throw with message `{table}: rows were added after planning; not loaded.` (nothing else changed); log the clustered statement at Info; `SetHeapLoadMarker`; `DisableIndexes(plan.ToDisable)`; drop (`ALTER TABLE {q} DROP CONSTRAINT {QuoteName(name)};` or `DROP INDEX {QuoteName(name)} ON {q};`); log the `dropped; loading {n} parts in parallel as a heap.` message. Extended property calls use `sp_addextendedproperty` / `sp_dropextendedproperty` with `@level0type = N'SCHEMA'`, `@level1type = N'TABLE'` and all names and the value as SqlParameters.
- [ ] **Step 4: Implement `FinishHeapLoad`**, deciding only from what exists now: if no `index_id = 1` row and the marker is set (the drop happened) → run `plan.ClusteredStatement` (`CommandTimeout = 0`); on failure log `Could not recreate ...` error with `plan.ScriptPath` and return (marker and file stay). Then rebuild the indexes of `plan.ToDisable` that are disabled now, through `RebuildIndexes` (its own error per index). If the clustered index exists and no index of `plan.ToDisable` is still disabled: `RemoveHeapLoadMarker`, delete the script file, log `recreated; rebuilt {k} index(es).`; otherwise keep both. Failing to connect is reported with `RebuildNotStartedMessage`-style text naming the script, never thrown (`TableWork` drops `after`'s exception when the load failed).
- [ ] **Step 5: Run** `dotnet build BcpJson.sln -warnaserror` and `dotnet test BcpJson.sln` — clean, all pass.
- [ ] **Step 6: Commit** `import --load-as-heap: plan, drop, load as a heap, recreate`.

### Task 4: CI round trip and benchmark

**Files:**
- Create: `tests/integration/heap_indexes.sql`
- Modify: `tests/integration/roundtrip.sh` (`split` mode, after the BCP-parts case (e)), `tests/integration/benchmark.sh`

**Interfaces:**
- Consumes: Task 3's option and messages; existing helpers `create_target_db`, `scalar`, `export_json`, `compare_exact`, `$sj` (JSON parts, 8 per table), `$sb` (BCP parts), `$sjs` (source JSON for the split tables).
- Produces: `heap_indexes.sql`, one row per index of the `split` schema ordered by table and index name: table, index name, type, is_unique, is_primary_key, is_unique_constraint, filter_definition, data space name, `MIN(data_compression_desc)` over partitions, key columns with ASC/DESC in key order, included columns, is_disabled. Before/after comparison is a `diff` of its output.

Cases (each logs one `echo` line when it passes; every failure is `::error::` + `exit 1`):
- [ ] **(a) Setup.** `create_target_db BcpJsonSplitHeap`; then `ALTER INDEX PK_Big ON split.Big REBUILD WITH (DATA_COMPRESSION = PAGE)`; `CREATE INDEX IX_Big_Note ON split.Big (Note) WHERE Note IS NOT NULL`; `CREATE TABLE split.TextRef (Code VARCHAR(30) COLLATE Latin1_General_CS_AS NOT NULL CONSTRAINT FK_TextRef_Text REFERENCES split.Text (Code))`. Save `heap_indexes.sql` output.
- [ ] **(b) Import** `$sj` with `--file-format json --max-tasks 4 --load-as-heap`. Expect log lines: `split.Big: clustered index PK_Big dropped; loading 8 parts in parallel as a heap.`, the same for `PK_Descending`, `CX_NullKey_K`, `PK_Parted`; `split.Text: not loaded as a heap: referenced by foreign key FK_TextRef_Text; disabling indexes instead.`; `split.PartedCci: not loaded as a heap: clustered columnstore; disabling indexes instead.`; `split.Big: clustered index PK_Big recreated; rebuilt 3 index(es).`
- [ ] **(c) Afterwards:** `heap_indexes.sql` output identical to (a) (this covers compression, filter, DESC key and `ps_SplitMonth` for `PK_Parted`); no `*.load-as-heap.sql` in `$sj`; `SELECT COUNT(*) FROM sys.extended_properties WHERE name = N'bcpjson_load_as_heap'` = 0; data identical (`export_json` + `compare_exact` against `$sjs`).
- [ ] **(d) BCP:** fresh `create_target_db BcpJsonSplitHeapBcp`, import `$sb` with `--file-format bcp --load-as-heap`: `split.Big: clustered index PK_Big dropped; loading 4 parts in parallel as a heap.`, data identical to the source for the tables the existing BCP case compares.
- [ ] **(e) Failed load:** copy only `split.Big.part*.json` (no manifest) to a new folder, cut `part0003` to its first 1,000 bytes; import with `--load-as-heap --truncate-target` into `BcpJsonSplitHeap` must exit non-zero; afterwards `PK_Big` is `index_id = 1`, `sys.indexes` has no disabled index on `split.Big`, no script file, no marker.
- [ ] **(f) Duplicate key:** in `BcpJsonSplitHeap`, `ALTER TABLE split.Descending DROP CONSTRAINT PK_Descending; ALTER TABLE split.Descending ADD CONSTRAINT PK_Descending PRIMARY KEY CLUSTERED (Val);` on an empty table; import `split.Descending.part*.json` with `--load-as-heap --truncate-target` must fail with `split.Descending: Could not recreate PK_Descending: ` … `Run the script in ` …`split.Descending.load-as-heap.sql.`; the file and the marker exist. Copy the parts to another folder and import again: must fail with the interrupted-run message, row count unchanged (20,000).
- [ ] **(g) `--target-table`:** import `split.Big.part*.json` with `--target-table split.BigOther --load-as-heap --truncate-target`: `split.BigOther: clustered index CX_BigOther_Stamp dropped; loading 8 parts in parallel as a heap.`, 200,000 rows, `CX_BigOther_Stamp` back as `index_id = 1`.
- [ ] **(h) Read-only folder:** copy the `split.NullKey` parts to a folder, `chmod a-w` it, import with `--load-as-heap --truncate-target`: `split.NullKey: not loaded as a heap: cannot write ` warning, success, `CX_NullKey_K` still `index_id = 1`.
- [ ] **(i) Leftover file:** copy the `split.Text` parts plus an empty `split.Text.load-as-heap.sql` to a folder; import with `--source-file '*'`: warning `split.Text.load-as-heap.sql is left over; the target is whole, delete it.` (the logged `{file}` is the path; match its suffix), success.
- [ ] **(j) Benchmark:** `benchmark.sh` gets `create_db BenchE yes`, `timed "import indexed, --load-as-heap" bcpjson "${IMPORT[@]}" --target-database BenchE --load-as-heap`, `check_rows BenchE`, and the header comment lists it.
- [ ] **Step: push and watch CI** (`gh pr checks <pr> --watch`): all required checks green on 2019, 2022, 2025.
- [ ] **Commit** `CI: --load-as-heap round trips, failures and fallbacks`.

### Task 5: Documentation

**Files:**
- Modify: `README.md` ("Splitting large tables"), `CLAUDE.md` (import row, Code layout, Key-range splitting import load modes), `docs/ROADMAP.md`, `docs/superpowers/specs/2026-11-23-load-as-heap-design.md` (add "As built")

- [ ] **Step 1: README:** the option with an example; when it applies; the fallback list (reason texts); the recovery script and the marker and what to do with each; edition note (outside Enterprise and Developer, SQL Server builds an index on one thread, so the gain is smaller); the 5,000,000-row numbers (53.9 / 30.1 / 20.4 s).
- [ ] **Step 2: CLAUDE.md:** `--load-as-heap` in the import row; `Program.HeapLoad.cs` and `HeapLoadScript.cs` in Code layout; one line in "Import load modes"; `heap_probe.sql` step in the CI paragraph.
- [ ] **Step 3: ROADMAP:** "Done after v2.7.0 (`import --load-as-heap`)" section; option 2 backlog line points to it; "Later" items from the spec stay in the backlog.
- [ ] **Step 4: Spec "As built":** every deviation from the design made during Tasks 1–4, one line each.
- [ ] **Step 5: Commit** `Docs: import --load-as-heap`.
