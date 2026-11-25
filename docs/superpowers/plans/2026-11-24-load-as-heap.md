﻿# import --load-as-heap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `import --load-as-heap`: split tables bound for a clustered rowstore target lose all their indexes for the load, take their parts in parallel as a heap, then get every index back, with a script file and table markers as the safety net.

**Architecture:** Pure rules (which tables qualify, recovery script name and text, marker names) live in `LoadPlanning.cs` and a new `HeapLoadScript.cs` with unit tests. Everything that touches SQL Server lives in a new `Program.HeapLoad.cs`, wired into the existing import planning (`PlanImportTable`) and the per-table `TableWork` before/after hooks. CI proves it in `roundtrip.sh split`.

**Tech Stack:** .NET 8, Microsoft.Data.SqlClient 7, SMO 181, CommandLineParser 2.9, xUnit; bash + sqlcmd + jq for CI; SQL Server 2019/2022/2025 containers.

**Spec:** `docs/superpowers/specs/2026-11-23-load-as-heap-design.md` (revised 2026-11-24: every index is dropped and recreated; the old probe is gone).

## Global Constraints

- CLAUDE.md rules: `QuoteName()` / `SqlLiteral()` for identifiers and literals, parameters where possible; `Interlocked` for shared counters; errors via `PostToConsole(..., InfoLevel.Error)` (exit code 1), fallbacks as `InfoLevel.Warning`; `CommandTimeout = 0` for index builds; connections only from `SqlConnectionFactory.Create()`.
- C# files UTF-8 with BOM and CRLF; `Nullable` disabled; helpers that need tests are `internal`.
- Option: `--load-as-heap` (bool, default false). Help text, verbatim: "For split tables loaded into a table with a clustered index: drop its indexes, load the parts in parallel into the heap, then recreate the indexes. Other tables load as with --disable-indexes."
- Applies to JSON and BCP imports, only to tables with two or more files; every other table of the run (single-file, or fallen back) is loaded as with `--disable-indexes`. All names use the *target* table (after `--target-table`).
- Index statements are scripted with SMO from the target database (filegroup / partition scheme kept: **no** `NoFileGroup`), in creation order: clustered first, then `Program.IndexCreationRank` order, then index id.
- Recovery script `<schema>.<table>.load-as-heap.sql` in `--source-path`. Markers: one extended property per index on the table, named `bcpjson_load_as_heap_001`, `_002`, ... in creation order, value = that index's statement (nvarchar). Each statement at most 3,500 characters.
- Messages, verbatim (placeholders in braces):
  - `{table}: not loaded as a heap: {reason}; disabling indexes instead.`
  - `{table}: {k} index(es) dropped; loading {n} parts in parallel as a heap.`
  - `{table}: Could not recreate {name}: {reason}. Run the script in {path}.`
  - `{table}: clustered index {name} and {k} other index(es) recreated.`
  - `{table}: rows were added after planning; not loaded.`
  - `{table}: an interrupted --load-as-heap run left it without some of its indexes; run the script in {file} (it recreates what is missing and removes the markers; if the file is lost, the statements are in the extended properties bcpjson_load_as_heap_*, drop those after running them), then import again.`
  - `{file} is left over; the target is whole, delete it.`
- Fallback reasons, verbatim, checked in this order: `--no-table-lock`, `CLR columns`, `memory-optimized`, `clustered columnstore`, `referenced by foreign key {name}`, `XML or spatial index {name}`, `disabled index {name}`, `indexed view {name}`, `temporal table`, `change tracking`, `replication`, `change data capture`, `FILESTREAM`, `full-text index`, `not empty`, `index definition too long: {name}`, `cannot write {path}: {message}`. A target that is a heap (no clustered index) gets no message and no change.

## Review Focus

1. A target whose schema, table or index name contains `]`, `'`, a space or non-ASCII characters: every DROP/CREATE and the extended-property calls must quote correctly (parameters for `sp_addextendedproperty` / `sp_dropextendedproperty` arguments), and the script file name must still be the plain `schema.table` text. Test: Task 2 `FileName` with `we]ird` and `Tab le`; Task 3 reviewer check that no statement concatenates a raw name.
2. `--target-table` mapping parts of `split.Big` into `split.BigOther` (clustered index that is not a constraint): the plain `DROP INDEX` / `CREATE CLUSTERED INDEX` path, with script and markers named after `split.BigOther`. Test: Task 4 case (g).
3. Anything failing part way through "before" (after the markers are set, during the drops): "after" creates exactly what is missing, and markers and script stay unless every index is back. Test: Task 4 case (e) plus Task 3's rule that "after" decides from what exists.
4. Source folder read-only: the table falls back with `cannot write ...` and is loaded with `--disable-indexes`, its indexes never dropped. Test: Task 4 case (h).
5. A leftover script file for a table that is whole (no markers) never blocks an import, and the script is never loaded as data even with `--source-file '*'`. Test: Task 2 `IsImportableFile` and Task 4 case (i).

---

### Task 1: (done) CI probe

Ran on 2019/2022/2025 (CI run 45540422748): dropping the clustered index rebuilds and re-enables disabled nonclustered indexes. The design was revised to drop every index; the probe and its CI step were removed in the revision commit. Nothing to do.

### Task 2: Pure rules: qualification, recovery script, markers

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
      string ReferencingForeignKey, string XmlOrSpatialIndex, string DisabledIndex, string IndexedView, bool Temporal,
      bool ChangeTracking, bool Replicated, bool ChangeDataCapture, bool Filestream, bool FullText, bool Empty);
  /// LoadAsHeap true: drop and reload. False with Reason null: nothing to say (heap target). False with Reason: warn.
  public sealed record HeapLoadDecision(bool LoadAsHeap, string Reason);
  internal const int MaxIndexStatementLength = 3500;
  internal static HeapLoadDecision DecideHeapLoad(HeapLoadFacts facts, bool tableLock, bool stagedClrColumns,
      bool truncateTarget, string tooLongIndex);   // tooLongIndex: name of the first index whose statement exceeds the limit, or null
  ```
  Checks in the Global Constraints order; `cannot write ...` is not decided here (Task 3 decides it after trying to write). A target with neither clustered rowstore nor columnstore returns `(false, null)` before any other check. `not empty` applies only when `truncateTarget` is false.
- Produces (`HeapLoadScript.cs`, `internal static class HeapLoadScript`):
  ```csharp
  internal const string Suffix = ".load-as-heap.sql";
  internal const string MarkerPrefix = "bcpjson_load_as_heap_";
  internal static string FileName(string schema, string table);          // schema + "." + table + Suffix
  internal static string MarkerName(int ordinal);                         // 1 -> "bcpjson_load_as_heap_001"
  internal static string Build(string schema, string table, IEnumerable<(string Name, string Statement)> indexes, DateTime utc);
  ```
  `Build` text: a `--` comment block (written by bcpJson import --load-as-heap for `{schema}.{table}` at `{utc:yyyy-MM-dd HH:mm:ss} UTC`; run this script to recreate the indexes if the import was interrupted; rerunnable: each index is created only if it is missing, and the markers are removed at the end), then each index as `IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[schema].[table]') AND name = N'name')` / `BEGIN` / statement / `END` / `GO`, then each marker as `IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 1 AND major_id = OBJECT_ID(...) AND minor_id = 0 AND name = N'marker')` / `    EXEC sys.sp_dropextendedproperty @name = N'marker', @level0type = N'SCHEMA', @level0name = N'schema', @level1type = N'TABLE', @level1name = N'table';` / `GO`. Literals via `Program.SqlLiteral`.

- [ ] **Step 1: Write failing tests.**
  - `Heap_load_decision` (Theory over a facts builder): clustered with everything clear → `(true, null)`; heap → `(false, null)`; columnstore → `(false, "clustered columnstore")`; `tableLock: false` → `"--no-table-lock"`; staged → `"CLR columns"`; `ReferencingForeignKey = "FK_X"` → `"referenced by foreign key FK_X"`; `XmlOrSpatialIndex = "SIX_G"` → `"XML or spatial index SIX_G"`; `DisabledIndex = "IX_Off"` → `"disabled index IX_Off"`; `IndexedView = "dbo.V"` → `"indexed view dbo.V"`; each bool fact → its reason; `Empty = false` → `"not empty"`, but `(true, null)` with `truncateTarget: true`; `tooLongIndex: "IX_Long"` → `"index definition too long: IX_Long"`.
  - `Heap_load_reason_precedence`: no table lock + memory-optimized + FK → `"--no-table-lock"`; memory-optimized + FK → `"memory-optimized"`; disabled index + indexed view → `"disabled index ..."`.
  - `HeapLoadScriptTests.File_name`: `("split", "Big")` → `"split.Big.load-as-heap.sql"`; `("we]ird", "Tab le")` → `"we]ird.Tab le.load-as-heap.sql"`.
  - `HeapLoadScriptTests.Marker_name`: 1 → `"bcpjson_load_as_heap_001"`, 42 → `"bcpjson_load_as_heap_042"`, 999 → `"bcpjson_load_as_heap_999"`.
  - `HeapLoadScriptTests.Build_orders_statements`: statements in the given order, each followed by a line `GO`; the label appears in the header.
  - `IsImportableFile` (existing test class, or `ImportFileTests`): `"split.Big.load-as-heap.sql"` is not importable; `"split.Big.part0001.json"` still is.
- [ ] **Step 2: Run** `dotnet test BcpJson.sln` — expect the new tests to fail to compile.
- [ ] **Step 3: Implement** the records and methods above; `IsImportableFile` also rejects names ending in `HeapLoadScript.Suffix` (ordinal ignore case).
- [ ] **Step 4: Run** `dotnet test BcpJson.sln` — all pass.
- [ ] **Step 5: Commit** `Load as heap: qualification rules, recovery script and markers`.

### Task 3: The option, planning, and the before/after steps

**Files:**
- Modify: `src/BcpJson/ImportOptions.cs`
- Create: `src/BcpJson/Program.HeapLoad.cs`
- Modify: `src/BcpJson/Program.Import.cs` (`PlanImportTable` and its `TableWork` wiring)
- Test: `tests/BcpJson.Tests/CommandLineTests.cs`

**Interfaces:**
- Consumes: Task 2's `HeapLoadFacts`, `DecideHeapLoad`, `HeapLoadScript`; existing `Program.IndexCreationRank` (Program.Copy.cs), `ReadIndexes`, `ExecuteSql`, `TableWork`, `LoadPlanning.IndexesToDisable` (fallback path).
- Produces: `ImportOptions.LoadAsHeap` (`[Option("load-as-heap")]`, help text from Global Constraints). In `Program.HeapLoad.cs`:
  ```csharp
  private sealed record SavedIndex(string Name, bool IsClustered, bool IsConstraint, string Statement);
  private sealed class HeapLoadPlan { public List<SavedIndex> Indexes; public string ScriptPath; }   // Indexes in creation order
  static HeapLoadFacts ReadHeapLoadFacts(SqlConnection conn, int objectId);                            // one catalog batch
  static List<SavedIndex> ScriptTargetIndexes(ImportOptions options, string schema, string table);    // SMO, creation order
  static bool HasHeapLoadMarkers(SqlConnection conn, string schema, string table);
  static void SetHeapLoadMarkers(SqlConnection conn, string schema, string table, List<SavedIndex> indexes);
  static void RemoveHeapLoadMarkers(SqlConnection conn, string schema, string table);   // every bcpjson_load_as_heap_% property
  static void PrepareHeapLoad(ImportOptions options, ImportTable table, HeapLoadPlan plan, int parts);  // TableWork before
  static void FinishHeapLoad(ImportOptions options, ImportTable table, HeapLoadPlan plan);              // TableWork after
  ```

- [ ] **Step 1: Failing test** in `CommandLineTests`: `import --load-as-heap ...` parses with `LoadAsHeap == true`; default false.
- [ ] **Step 2: Implement planning** in `PlanImportTable`, right after the target is resolved, for *every* import:
  - `HasHeapLoadMarkers` → error (interrupted-run message, `{file}` = full path of `HeapLoadScript.FileName` in `--source-path`), return no items.
  - Else, if that script file exists in `--source-path` → warning `{file} is left over; the target is whole, delete it.`
  Then, when `options.LoadAsHeap` and the table has two or more files: read facts; `ScriptTargetIndexes` (statements of one index joined with newlines; SMO script options keep filegroup / partition scheme, compression, filters, includes); `tooLongIndex` = first statement longer than `MaxIndexStatementLength`; `DecideHeapLoad`. If it says load as heap, write `HeapLoadScript.Build(label, DateTime.UtcNow, statements)` to the script path; an exception there becomes reason `cannot write {path}: {message}`. Any reason → the fallback warning, and the table continues exactly as `--disable-indexes` (`LoadPlanning.IndexesToDisable`, existing before/after). `LoadAsHeap` implies `DisableIndexes` for every table that is not loaded as a heap.
  When loading as a heap, the load mode is computed from `new TargetShape(false, false, 0, shape.Partitioned)` (parallel), and the table's `TableWork` gets `PrepareHeapLoad` / `FinishHeapLoad` instead of `PrepareImportTarget` / `RebuildDisabledIndexes`.
- [ ] **Step 3: Implement `PrepareHeapLoad`** (one connection, in order): truncate when `--truncate-target` (same message as today); if `SELECT TOP (1) 1` finds rows → delete the script file and throw with message `{table}: rows were added after planning; not loaded.` (nothing else changed); log each statement at Info; `SetHeapLoadMarkers`; drop the nonclustered indexes in reverse creation order, then the clustered one (`ALTER TABLE {q} DROP CONSTRAINT {QuoteName(name)};` for constraints, else `DROP INDEX {QuoteName(name)} ON {q};`); log `{k} index(es) dropped; loading {n} parts in parallel as a heap.` Extended-property calls use `sp_addextendedproperty` / `sp_dropextendedproperty` with `@level0type = N'SCHEMA'`, `@level1type = N'TABLE'` and all names and values as SqlParameters.
- [ ] **Step 4: Implement `FinishHeapLoad`**, deciding only from what exists now (`sys.indexes` names of the table): if the clustered index is missing, run its statement (`CommandTimeout = 0`); on failure log the `Could not recreate ...` error with `plan.ScriptPath` and return (markers and file stay). Then create each missing nonclustered index in creation order; each failure is the same error for that index, the rest continue. If every saved index exists: `RemoveHeapLoadMarkers`, delete the script file, log `clustered index {name} and {k} other index(es) recreated.` (k = nonclustered indexes in the plan). Otherwise keep both. Connection failures are reported as an error naming the script, never thrown (`TableWork` drops `after`'s exception when the load failed).
- [ ] **Step 5: Run** `dotnet build BcpJson.sln -warnaserror` and `dotnet test BcpJson.sln` — clean, all pass.
- [ ] **Step 6: Commit** `import --load-as-heap: plan, drop, load as a heap, recreate`.

### Task 4: CI round trip and benchmark

**Files:**
- Create: `tests/integration/heap_indexes.sql`
- Modify: `tests/integration/roundtrip.sh` (`split` mode, after the BCP-parts case (e)), `tests/integration/benchmark.sh`

**Interfaces:**
- Consumes: Task 3's option and messages; existing helpers `create_target_db`, `scalar`, `export_json`, `compare_exact`, `$sj` (JSON parts, 8 per table where possible), `$sb` (BCP parts), `$sjs` (source JSON of the split tables).
- Produces: `heap_indexes.sql`, one row per index of the `split` schema ordered by table and index name: table, index name, type, is_unique, is_primary_key, is_unique_constraint, filter_definition, data space name, `MIN(data_compression_desc)` over partitions, key columns with ASC/DESC in key order, included columns, is_disabled. Before/after comparison is a `diff` of its output.

Cases (each logs one `echo` line when it passes; every failure is `::error::` + `exit 1`):
- [ ] **(a) Setup.** `create_target_db BcpJsonSplitHeap`; then `ALTER INDEX PK_Big ON split.Big REBUILD WITH (DATA_COMPRESSION = PAGE)`; `CREATE INDEX IX_Big_Note ON split.Big (Note) WHERE Note IS NOT NULL`; `CREATE TABLE split.TextRef (Code VARCHAR(30) COLLATE Latin1_General_CS_AS NOT NULL CONSTRAINT FK_TextRef_Text REFERENCES split.Text (Code))`. Save `heap_indexes.sql` output.
- [ ] **(b) Import** `$sj` with `--file-format json --max-tasks 4 --load-as-heap`. Expect: `split.Big: 4 index(es) dropped; loading 8 parts in parallel as a heap.`; `split.Descending: 1 index(es) dropped; loading 8 parts in parallel as a heap.`; `split.NullKey: 1 index(es) dropped; loading 8 parts in parallel as a heap.`; `split.Parted: 1 index(es) dropped; loading [0-9]+ parts in parallel as a heap.` (regex: partition grouping decides the count); `split.Text: not loaded as a heap: referenced by foreign key FK_TextRef_Text; disabling indexes instead.`; `split.PartedCci: not loaded as a heap: clustered columnstore; disabling indexes instead.`; `split.Big: clustered index PK_Big and 3 other index(es) recreated.`
- [ ] **(c) Afterwards:** `heap_indexes.sql` output identical to (a) (covers compression, filter, DESC key and `ps_SplitMonth` for `PK_Parted`); no `*.load-as-heap.sql` in `$sj`; `SELECT COUNT(*) FROM sys.extended_properties WHERE name LIKE N'bcpjson[_]load[_]as[_]heap[_]%'` = 0; data identical (`export_json` + `compare_exact` against `$sjs`).
- [ ] **(d) BCP:** fresh `create_target_db BcpJsonSplitHeapBcp`, import `$sb` with `--file-format bcp --load-as-heap`: `split.Big: 3 index(es) dropped; loading 4 parts in parallel as a heap.` (no IX_Big_Note in this database), data identical to the source for the tables the existing BCP case compares.
- [ ] **(e) Failed load:** copy only `split.Big.part*.json` (no manifest) to a new folder, cut `part0003` to its first 1,000 bytes; import with `--load-as-heap --truncate-target` into `BcpJsonSplitHeap` must exit non-zero; afterwards `heap_indexes.sql` output for `split.Big` equals (a)'s, no script file, no marker.
- [ ] **(f) Duplicate key:** in `BcpJsonSplitHeap`, `TRUNCATE TABLE split.Descending; ALTER TABLE split.Descending DROP CONSTRAINT PK_Descending; ALTER TABLE split.Descending ADD CONSTRAINT PK_Descending PRIMARY KEY CLUSTERED (Val);`; import `split.Descending.part*.json` with `--load-as-heap --truncate-target` must fail with `split.Descending: Could not recreate PK_Descending: ` … `Run the script in ` …`split.Descending.load-as-heap.sql.`; the file and the marker exist. Copy the parts to another folder and import again: must fail with the interrupted-run message, row count unchanged (20,000).
- [ ] **(f2) Duplicate in a unique nonclustered index:** in `BcpJsonSplitHeap`, restore `split.Descending` (drop markers and the PK on `Val`, truncate, `ADD CONSTRAINT PK_Descending PRIMARY KEY CLUSTERED (Id DESC)`, `CREATE UNIQUE INDEX UX_Desc_Val ON split.Descending (Val)`); import its parts with `--load-as-heap --truncate-target` (delete the leftover script first): must fail with `split.Descending: Could not recreate UX_Desc_Val: `; `PK_Descending` exists as `index_id = 1`.
- [ ] **(g) `--target-table`:** import `split.Big.part*.json` with `--target-table split.BigOther --load-as-heap --truncate-target`: `split.BigOther: 1 index(es) dropped; loading 8 parts in parallel as a heap.`, 200,000 rows, `CX_BigOther_Stamp` back as `index_id = 1`.
- [ ] **(h) Read-only folder:** copy the `split.NullKey` parts to a folder, `chmod a-w` it, import with `--load-as-heap --truncate-target`: `split.NullKey: not loaded as a heap: cannot write ` warning, success, `CX_NullKey_K` still `index_id = 1`.
- [ ] **(i) Leftover file:** copy the `split.Text` parts plus an empty `split.Text.load-as-heap.sql` to a folder; import with `--source-file '*'`: a warning ending `split.Text.load-as-heap.sql is left over; the target is whole, delete it.`, success.
- [ ] **(j) Benchmark:** `benchmark.sh` gets `create_db BenchE yes`, `timed "import indexed, --load-as-heap" bcpjson "${IMPORT[@]}" --target-database BenchE --load-as-heap`, `check_rows BenchE`, and the header comment lists it.
- [ ] **Push and watch CI** (`gh pr checks 48 --watch`): all required checks green on 2019, 2022, 2025.
- [ ] **Commit** `CI: --load-as-heap round trips, failures and fallbacks`.

### Task 5: Documentation

**Files:**
- Modify: `README.md` ("Splitting large tables"), `CLAUDE.md` (import row, Code layout, Key-range splitting import load modes), `docs/ROADMAP.md`, `docs/superpowers/specs/2026-11-23-load-as-heap-design.md` (add "As built")

- [ ] **Step 1: README:** the option with an example; when it applies; the fallback list (reason texts); the recovery script and the markers and what to do with each; edition note (outside Enterprise and Developer, SQL Server builds an index on one thread, so the gain is smaller); the 5,000,000-row numbers (53.9 / 30.1 / 20.4 s).
- [ ] **Step 2: CLAUDE.md:** `--load-as-heap` in the import row; `Program.HeapLoad.cs` and `HeapLoadScript.cs` in Code layout; one line in "Import load modes"; the probe finding (dropping a clustered index re-enables disabled nonclustered indexes) in one line where `--disable-indexes` is described.
- [ ] **Step 3: ROADMAP:** "Done after v2.7.0 (`import --load-as-heap`)" section; option 2 backlog line points to it; "Later" items from the spec stay in the backlog.
- [ ] **Step 4: Spec "As built":** every deviation from the design made during Tasks 2–4, one line each.
- [ ] **Step 5: Commit** `Docs: import --load-as-heap`.
