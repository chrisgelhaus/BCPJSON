# `copy --load-as-heap` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `copy --load-as-heap --recovery-path <folder>` drops the indexes of an existing split target, copies the parts in parallel into the heap and recreates the indexes, sharing import's heap-load code.

**Architecture:** The heap-load functions in `Program.HeapLoad.cs` stop taking `ImportOptions`/`ImportTable` and take a `HeapLoadTarget` record instead; import builds one per table and behaves exactly as before. Copy builds one in `PrepareCopyTarget` (TableWork before) for an existing split target and calls the same plan/prepare functions there; `FinishCopiedTable` (TableWork after) calls `FinishHeapLoad`.

**Tech Stack:** .NET 8, Microsoft.Data.SqlClient, SMO, CommandLineParser 2.9, xUnit; CI round trip in `tests/integration/roundtrip.sh`.

**Spec:** `docs/superpowers/specs/2026-11-26-copy-load-as-heap-design.md`

## Global Constraints

- Import's behaviour and every import message stay byte-for-byte as they are (CI greps them).
- Copy fallback warning, exact: `{table}: not loaded as a heap: {reason}; loading it with its indexes.`
- `--load-as-heap` without `--recovery-path` is refused in `Valid()` before any connection: `--load-as-heap needs --recovery-path: the folder where copy writes each table's recovery script.`
- `--recovery-path` folder is created if missing (`Directory.CreateDirectory`), stored as a full path.
- Every copy refuses an existing target that carries markers unless `--drop-target` drops it.
- C# files UTF-8 with BOM and CRLF; identifiers through `QuoteName`, literals through `SqlLiteral`; errors through `PostToConsole(..., InfoLevel.Error)`.
- A new option needs a row on its verb's reference page (`DocsTests`).
- The owner has no local SQL Server: database behaviour is proven only by `roundtrip.sh split` in CI.

## Review Focus

1. Preparation fails after the markers are set or some indexes are dropped (e.g. a DROP fails): the finish must still recreate the indexes, so `FinishCopiedTable` calls `FinishHeapLoad` before its `!context.Prepared` early return.
2. `--drop-target` with a marked target: no refusal (the table is dropped and recreated, markers go with it).
3. Rows inserted into the target between copy's truncate and the drop: `PrepareHeapLoad` throws "rows were added after planning; not loaded.", deletes the script, and the table is reported as failed, never loaded with missing indexes.
4. `--recovery-path` on a folder that cannot be written: `PlanHeapLoad` returns the "cannot write" reason and the table falls back with the copy warning.
5. Two copies with the same `--recovery-path` and table names into different databases overwrite each other's script; the script's `DB_NAME()` checks make a wrong one refuse. Accepted, documented in the guide.

---

### Task 1: Share the heap-load code through `HeapLoadTarget`

**Files:**
- Modify: `src/BcpJson/Program.HeapLoad.cs`, `src/BcpJson/Program.Import.cs:255-325`, `src/BcpJson/HeapLoadScript.cs:32-45`
- Test: `tests/BcpJson.Tests/HeapLoadScriptTests.cs`

**Interfaces:**
- Produces (in `partial class Program`, `Program.HeapLoad.cs`):
  - `private sealed record HeapLoadTarget(string Verb, string ConnectionString, string Schema, string Table, int ObjectId, bool TableLock, bool Truncate, string ScriptFolder)` with `QualifiedName` (`QuoteName(Schema) + "." + QuoteName(Table)`), `Label` (`Schema + "." + Table`) and `ScriptPath` (`ScriptFolder == null ? null : Path.Combine(ScriptFolder, HeapLoadScript.FileName(Schema, Table))`). `Truncate` true = the decision waives emptiness and `PrepareHeapLoad` truncates.
  - `static bool CheckHeapLoadLeftovers(SqlConnection conn, HeapLoadTarget target)`
  - `static HeapLoadPlan PlanHeapLoad(SqlConnection conn, HeapLoadTarget target, bool staged, out string notReason)`: returns the plan, or null with `notReason` set when there is something to warn about (null for a heap target). It no longer posts the fallback warning; callers do.
  - `static void PrepareHeapLoad(HeapLoadTarget target, HeapLoadPlan plan, int parts)`, `static void FinishHeapLoad(HeapLoadTarget target, HeapLoadPlan plan)`
  - `static List<SavedIndex> ScriptTargetIndexes(string connectionString, string databaseName, string schema, string table, HashSet<string> hypothetical)`
  - `HeapLoadScript.Build(string verb, string database, string schema, string table, IEnumerable<(string Name, string Statement)> indexes, DateTime utc)`
- Every message keeps import's text with "import" replaced by `target.Verb` (the marker refusal's "then import again.", the marker-removal error's "before the next import", the script's "-- Written by bcpJson import --load-as-heap" and "if the import was interrupted").

- [ ] **Step 1: Write the failing test** in `HeapLoadScriptTests.cs`; existing `Build(...)` calls gain `"import"` as first argument.

```csharp
[Fact]
public void Build_names_the_verb()
{
    string text = HeapLoadScript.Build("copy", "Sales DB", "split", "Big", new[] { ("PK_Big", "CREATE A") }, When);
    Assert.StartsWith("-- Written by bcpJson copy --load-as-heap for split.Big at 2026-11-24 08:05:03 UTC,\n", text);
    Assert.Contains("-- Run this script to recreate the indexes if the copy was interrupted.", text);
}
```

- [ ] **Step 2: Run it** — `dotnet test BcpJson.sln --filter FullyQualifiedName~HeapLoadScriptTests`. Expected: build error (no `Build` overload with 6 arguments).
- [ ] **Step 3: Implement** the `verb` parameter in `HeapLoadScript.Build`, then `HeapLoadTarget` and the signatures above. Import builds its target with `static HeapLoadTarget ImportHeapTarget(ImportOptions options, ImportTable table)` = `new("import", options.GetTargetConnectionString(), table.Schema, table.Table, table.ObjectId, !options.NoTableLock, options.TruncateTarget, options.SourcePath)`; `HeapLoadScriptPath` is replaced by `target.ScriptPath`; `PlanImportTable` posts `"{0}: not loaded as a heap: {1}; disabling indexes instead."` itself when `notReason != null`. `TryCreateIndex` takes the target.
- [ ] **Step 4: Run** `dotnet build BcpJson.sln -warnaserror` and `dotnet test BcpJson.sln`. Expected: build clean, all tests pass (398 + 1).
- [ ] **Step 5: Commit** — `Share the load-as-heap code through HeapLoadTarget (import unchanged)`.

### Task 2: Copy options, validation and docs

**Files:**
- Modify: `src/BcpJson/CopyOptions.cs`, `docs/reference/copy.md`, `docs/guides/large-tables.md` (`## Copy` section, `### If an index cannot be recreated`), `docs/guides/recover-a-failed-run.md` (`## The load-as-heap recovery script`)
- Test: `tests/BcpJson.Tests/CommandLineTests.cs`

**Interfaces:**
- Produces: `CopyOptions.LoadAsHeap` (`--load-as-heap`, bool), `CopyOptions.RecoveryPath` (`--recovery-path`, string), `internal string ValidateLoadAsHeap()` (no connection; returns the Global Constraints error text or null).

- [ ] **Step 1: Write the failing tests** in `CommandLineTests.cs`:

```csharp
[Fact]
public void Copy_load_as_heap_needs_recovery_path()
{
    var without = Parsed<CopyOptions>("copy", "--source-database", "a", "--target-database", "b", "--load-as-heap");
    Assert.True(without.LoadAsHeap);
    Assert.Equal("--load-as-heap needs --recovery-path: the folder where copy writes each table's recovery script.", without.ValidateLoadAsHeap());
    Assert.False(without.Valid());   // refused before any connection
    var with = Parsed<CopyOptions>("copy", "--source-database", "a", "--target-database", "b", "--load-as-heap", "--recovery-path", "r");
    Assert.Equal("r", with.RecoveryPath);
    Assert.Null(with.ValidateLoadAsHeap());
    Assert.Null(Parsed<CopyOptions>("copy", "--source-database", "a", "--target-database", "b").ValidateLoadAsHeap());
}
```

- [ ] **Step 2: Run** `dotnet test BcpJson.sln --filter FullyQualifiedName~CommandLineTests`. Expected: build error (no `LoadAsHeap` on `CopyOptions`).
- [ ] **Step 3: Implement** the two options (HelpText: `--load-as-heap` "For split tables copied into an existing table with a clustered index: drop its indexes, copy the parts in parallel into the heap, then recreate the indexes. Needs --recovery-path."; `--recovery-path` "Folder for the recovery scripts of --load-as-heap (created if missing)."), `ValidateLoadAsHeap()`, and in `Valid()` after `ValidateSplit`: the error, then `Directory.CreateDirectory` + `Path.GetFullPath` when `RecoveryPath` is given (failure: error `Cannot create --recovery-path {path}: {message}`, return false).
- [ ] **Step 4: Docs.** `copy.md`: rows for `` `--load-as-heap` `` and `` `--recovery-path` `` under Target/Running as fits, linking to the guide section. `large-tables.md` `## Copy`: existing targets of split tables can load as a heap with `--load-as-heap --recovery-path`; same qualifying rules as import; the fallback warning text from Global Constraints; the script lands in `--recovery-path`; one bcpjson example with `--split-tables`, `--truncate-target`, `--load-as-heap`, `--recovery-path`; the shared-folder note (Review Focus 5). `recover-a-failed-run.md`: the script is in `--source-path` (import) or `--recovery-path` (copy); the next copy also refuses a marked table, "then copy again.".
- [ ] **Step 5: Run** `dotnet test BcpJson.sln`. Expected: all pass, including `DocsTests` (both options documented, links and anchors resolve).
- [ ] **Step 6: Commit** — `copy: --load-as-heap and --recovery-path options, validation and docs`.

### Task 3: Copy loads existing split targets as heaps

**Files:**
- Modify: `src/BcpJson/Program.Copy.cs` (`CopyTableContext`, `PrepareCopyTarget`, `FinishCopiedTable`)
- Test: `tests/integration/roundtrip.sh` (split mode, new step 7 after the CLR check at the end of step 6), header comment at the top of the file

**Interfaces:**
- Consumes: Task 1's `HeapLoadTarget`, `CheckHeapLoadLeftovers`, `PlanHeapLoad`, `PrepareHeapLoad`, `FinishHeapLoad`; Task 2's `LoadAsHeap`, `RecoveryPath`.
- Produces: `CopyTableContext.HeapTarget` (`HeapLoadTarget`), `CopyTableContext.HeapPlan` (`HeapLoadPlan`).

Order in `PrepareCopyTarget`:
1. Before `PrepareTargetTable`, unless `setting.DropTarget`: open a target connection and call `CheckHeapLoadLeftovers` with a target built from the source schema/table (`ObjectId` unused there), `ScriptFolder = setting.RecoveryPath` (may be null). False → throw a new `HeapLoadRefusedException` (private, in `Program.HeapLoad.cs`); the item loop in `BulkCopyTables` skips posting it (`catch (HeapLoadRefusedException)` before the general catch), since `CheckHeapLoadLeftovers` already posted the error. Without `--recovery-path` (`target.ScriptPath == null`) the refusal reads `{table}: an earlier --load-as-heap run left it without some of its indexes; run its recovery script (it recreates what is missing and removes the markers; if the file is lost, the statements are in the extended properties bcpjson_load_as_heap_*, drop those after running them), then copy again.` and the leftover-file warning is skipped.
2. `PrepareTargetTable` as today (truncation stays here: `--truncate-target` truncates whether or not the table qualifies, so doing it before the decision is equivalent to the spec's order). The heap target therefore has `Truncate = false`.
3. In the `parts > 1` block, after the object id is read: when `setting.LoadAsHeap && !context.Created`, build `HeapLoadTarget("copy", targetConnectionString, schema, table, objectId, !setting.NoTableLock, false, setting.RecoveryPath)` and `PlanHeapLoad(conn, target, false, out reason)`. A plan: set `context.HeapTarget`/`HeapPlan` **before** calling `PrepareHeapLoad(target, plan, parts)`, then `context.Mode = LoadMode.Parallel` (PrepareHeapLoad logs the "loading n parts in parallel as a heap" line; skip the usual parts line). No plan and `reason != null`: post the copy fallback warning, then the existing shape logic.

`FinishCopiedTable`: first line `if (context.HeapPlan != null) FinishHeapLoad(context.HeapTarget, context.HeapPlan);`, before the `!context.Prepared` return.

- [ ] **Step 1: Write the CI step** (it fails until the code exists): step 7 in `roundtrip.sh split`, using the existing `heap_indexes` and `markers` helpers and `BcpJsonSplitCopy` (its tables exist after step 6):
  - (a) `CREATE TABLE split.TextRef (... REFERENCES split.Text (Code))` in `BcpJsonSplitCopy`; `heap_indexes BcpJsonSplitCopy > copy-heap-before.txt`.
  - (b) `bcpjson copy ... --target-database BcpJsonSplitCopy --max-tasks 4 --include-schemas split --exclude-tables split.TextRef --split-tables "split.*:4" --snapshot --truncate-target --load-as-heap --recovery-path "$WORK/copy-recovery"`; grep `split.Big: 4 index(es) dropped; loading 4 parts in parallel as a heap.` (adjust the index count to what `copy-heap-before.txt` shows for split.Big), `split.Text: not loaded as a heap: referenced by foreign key FK_TextRef_Text; loading it with its indexes.` and `split.Big: clustered index PK_Big and ` .
  - (c) `heap_indexes` after equals before (`diff -u`); no `*.load-as-heap.sql` in `$WORK/copy-recovery`; `markers BcpJsonSplitCopy` = 0; `export_json` + `compare_exact "$sjs"` (excluding split.TextRef).
  - (d) Without `--recovery-path`: copy with `--load-as-heap` exits non-zero and logs the Task 2 error.
  - (e) Hand-marked target: `EXEC sys.sp_addextendedproperty N'bcpjson_load_as_heap_001', N'x', N'SCHEMA', N'split', N'TABLE', N'Descending'`; a plain `copy --include-tables split.Descending --truncate-target` exits non-zero and logs `split.Descending: an earlier --load-as-heap run left it without some of its indexes; run its recovery script` and `then copy again.`; the row count is unchanged (not truncated); drop the property.
- [ ] **Step 2: Implement** the copy changes above.
- [ ] **Step 3: Run** `dotnet build BcpJson.sln -warnaserror` and `dotnet test BcpJson.sln`. Expected: clean, all pass. `bash -n tests/integration/roundtrip.sh`. Expected: no output.
- [ ] **Step 4: Commit** — `copy --load-as-heap: existing split targets load as heaps`; push the branch and open the PR; the round trips on SQL Server 2019/2022/2025 must pass (they are the only proof of this task).

### Task 4: CLAUDE.md and roadmap

**Files:** `CLAUDE.md` (copy row of the verb table; Key-range splitting `--load-as-heap` bullet), `docs/ROADMAP.md` (new "Done after v2.8.1" bullet; remove `copy --load-as-heap` from the "Later, from the load-as-heap design" backlog line)

- [ ] **Step 1:** Edit both (copy row: `--load-as-heap` with `--recovery-path` for existing split targets through `HeapLoadTarget`; bullet: shared with copy, copy's fallback loads with its indexes).
- [ ] **Step 2: Run** `dotnet test BcpJson.sln`. Expected: all pass.
- [ ] **Step 3: Commit** — `Docs: copy --load-as-heap in CLAUDE.md and the roadmap`.
