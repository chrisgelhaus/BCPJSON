# `copy --disable-indexes` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `copy --disable-indexes` disables an existing target's nonclustered indexes (except the primary key and foreign-key-referenced ones) for the load and rebuilds them afterwards; `copy --load-as-heap` falls back to it.

**Architecture:** Copy reuses import's pure rules (`LoadPlanning.IndexesToDisable`, `RebuildOrder`) and SQL helpers in `Program.Indexes.cs`. Import's `RebuildDisabledIndexes` moves to `Program.Indexes.cs` with a connection-string signature so both verbs call it. Copy disables in `PrepareCopyTarget` (TableWork before) and rebuilds in `FinishCopiedTable` (TableWork after).

**Tech Stack:** .NET 8, Microsoft.Data.SqlClient, CommandLineParser 2.9, xUnit; CI `tests/integration/roundtrip.sh split`.

**Spec:** `docs/superpowers/specs/2026-11-27-copy-disable-indexes-design.md`

## Global Constraints

- Import's behaviour and messages unchanged (CI greps them).
- Messages reused verbatim from import: `{table}: Disabled indexes: a, b.`, `{table}: Rebuilt n index(es).`, `{table}: memory-optimized; --disable-indexes ignored.`, rebuild failure `... It is still disabled; run ALTER INDEX ... REBUILD.`
- Copy's `--load-as-heap` fallback warning becomes exactly `{table}: not loaded as a heap: {reason}; disabling indexes instead.`
- Applies only to existing targets (not created or recreated by this run); `--snapshot` is not required.
- C# files UTF-8 with BOM; new options need a row on `docs/reference/copy.md` (`DocsTests`).
- No local SQL Server: database behaviour is proven by `roundtrip.sh split` in CI.

## Review Focus

1. Disabling fails halfway (second `ALTER INDEX ... DISABLE` errors): the indexes already disabled must still be rebuilt, so the list is stored in the context before disabling and the finish rebuilds before its `!context.Prepared` return.
2. A part fails: indexes are still rebuilt (finish runs after failures).
3. `--load-as-heap` on an unsplit existing table: disables indexes (as import), not a plain load.
4. `--drop-target`: the table is recreated, so nothing is disabled.
5. A heap target whose nonclustered indexes are all disabled loads split parts in parallel; a clustered target still loads one after another.

---

### Task 1: Option, rule and docs

**Files:**
- Modify: `src/BcpJson/CopyOptions.cs`, `src/BcpJson/LoadPlanning.cs`, `docs/reference/copy.md`, `docs/guides/large-tables.md` (`## Copy`)
- Test: `tests/BcpJson.Tests/CommandLineTests.cs`, `tests/BcpJson.Tests/LoadPlanningTests.cs`

**Interfaces:**
- Produces: `CopyOptions.DisableIndexes` (`--disable-indexes`); `internal static bool LoadPlanning.CopyDisablesIndexes(bool created, bool disableIndexes, bool loadedAsHeap, bool loadAsHeap)` = `!created && !loadedAsHeap && (disableIndexes || loadAsHeap)`.

- [ ] **Step 1: Failing tests.**

```csharp
// CommandLineTests
[Fact]
public void Copy_disable_indexes_defaults_to_false()
{
    Assert.False(Parsed<CopyOptions>("copy", "--source-database", "a", "--target-database", "b").DisableIndexes);
    Assert.True(Parsed<CopyOptions>("copy", "--source-database", "a", "--target-database", "b", "--disable-indexes").DisableIndexes);
}

// LoadPlanningTests
[Theory]
[InlineData(false, true, false, false, true)]    // existing, --disable-indexes
[InlineData(false, false, false, true, true)]    // existing, --load-as-heap fallback (not split or did not qualify)
[InlineData(false, false, true, true, false)]    // loaded as a heap: indexes are dropped, not disabled
[InlineData(true, true, false, true, false)]     // created by this copy
[InlineData(false, false, false, false, false)]  // neither option
public void Copy_disables_indexes_only_on_existing_targets(bool created, bool disableIndexes, bool loadedAsHeap, bool loadAsHeap, bool expected)
{
    Assert.Equal(expected, LoadPlanning.CopyDisablesIndexes(created, disableIndexes, loadedAsHeap, loadAsHeap));
}
```

- [ ] **Step 2: Run** `dotnet test BcpJson.sln --filter "FullyQualifiedName~Copy_disable"`. Expected: build errors (missing members).
- [ ] **Step 3: Implement** the option (HelpText: "Disable nonclustered indexes of existing target tables (except the primary key and indexes used by foreign keys) during the copy and rebuild them afterwards.") and `CopyDisablesIndexes`.
- [ ] **Step 4: Docs.** `copy.md`: a `--disable-indexes` row next to `--load-as-heap`, linking to `../guides/large-tables.md#copy`; in the `--load-as-heap` row, "Other tables load as with `--disable-indexes`.". `large-tables.md` `## Copy`: replace the "Tables that don't qualify ... copy has no `--disable-indexes`." bullet with the new fallback text (Global Constraints), and add a bullet for `--disable-indexes` on existing tables (the rules of [Load first, index afterwards](#load-first-index-afterwards---disable-indexes)).
- [ ] **Step 5: Run** `dotnet test BcpJson.sln`. Expected: all pass (DocsTests included).
- [ ] **Step 6: Commit** — `copy: --disable-indexes option, rule and docs`.

### Task 2: Copy disables and rebuilds

**Files:**
- Modify: `src/BcpJson/Program.Copy.cs` (`CopyTableContext`, `PrepareCopyTarget`, `FinishCopiedTable`), `src/BcpJson/Program.Import.cs` (`RebuildDisabledIndexes` moves), `src/BcpJson/Program.Indexes.cs`
- Test: `tests/integration/roundtrip.sh` (split step 6b, step 7 greps, header comment)

**Interfaces:**
- Consumes: Task 1's `DisableIndexes`, `CopyDisablesIndexes`.
- Produces: `static void RebuildDisabledIndexes(string connectionString, int objectId, string label, string qualifiedName, List<IndexInfo> toDisable)` in `Program.Indexes.cs` (import's body, unchanged behaviour; import calls it with `options.GetTargetConnectionString(), table.ObjectId, table.Label, table.QualifiedName`). `CopyTableContext.TargetObjectId` (int), `CopyTableContext.Disabled` (`List<IndexInfo>`).

`PrepareCopyTarget`, the block after SMO: open the target connection and read the object id when `parts > 1` or `!context.Created && (setting.DisableIndexes || setting.LoadAsHeap)`. Heap planning stays as is (split only). Then, when `LoadPlanning.CopyDisablesIndexes(context.Created, setting.DisableIndexes, false, setting.LoadAsHeap)`: memory-optimized → import's message; else `toDisable = IndexesToDisable(ReadIndexes(conn, objectId))`; when non-empty, set `context.Disabled = toDisable` **before** `DisableIndexes(conn, context.TargetName, toDisable)`, then post `{table}: Disabled indexes: ...`. For `parts > 1`, `ReadTargetShape(conn, objectId, disabled ids)` decides the mode. The heap fallback warning uses the Global Constraints text.

`FinishCopiedTable`: after the heap finish and before `!context.Prepared`: `if (context.Disabled?.Count > 0) RebuildDisabledIndexes(setting.TargetConnectionString(), context.TargetObjectId, tableRef.DisplayName, context.TargetName, context.Disabled);`.

- [ ] **Step 1: CI step 6b** in `roundtrip.sh split`, after "Split copy: ..." echo and before step 7: `heap_indexes BcpJsonSplitCopy > copy-disable-before.txt`; `bcpjson copy ... --target-database BcpJsonSplitCopy --max-tasks 4 --include-schemas split --split-tables "split.Big:4" --snapshot --truncate-target --disable-indexes | tee log`; grep `split.Big: Disabled indexes: ` , `split.Big: Rebuilt ` and `split.Big: 4 parts, loading one after another (clustered index).`; `heap_indexes` after equals before (no index left disabled); `export_json` + `compare_exact "$sjs"`. Step 7: the split.PartedCci and split.Text greps use `; disabling indexes instead.`. Header comment mentions `copy --disable-indexes`. `heap_indexes`/`markers` are defined in step 5b, so step 6b can use them.
- [ ] **Step 2: Implement** the code above.
- [ ] **Step 3: Run** `dotnet build BcpJson.sln -warnaserror`, `dotnet test BcpJson.sln`, `bash -n tests/integration/roundtrip.sh`. Expected: clean, all pass, no output.
- [ ] **Step 4: Commit** — `copy --disable-indexes: disable before the load, rebuild after; --load-as-heap falls back to it`; push and open the PR; CI round trips must pass.

### Task 3: CLAUDE.md and roadmap

- [ ] **Step 1:** CLAUDE.md copy row: `--disable-indexes` (existing targets, shared `RebuildDisabledIndexes`) and the changed fallback text; the `--load-as-heap` bullet's "copy's fallback loads with the indexes (no `--disable-indexes` there)" becomes "copy falls back to `--disable-indexes` as import does". ROADMAP: a "Done after v2.9.0" section with this change; remove "`--disable-indexes` for `copy` into existing tables" and "`copy --disable-indexes` for existing targets" from the backlog lines.
- [ ] **Step 2: Run** `dotnet test BcpJson.sln`. Expected: all pass.
- [ ] **Step 3: Commit** — `Docs: copy --disable-indexes in CLAUDE.md and the roadmap`.
