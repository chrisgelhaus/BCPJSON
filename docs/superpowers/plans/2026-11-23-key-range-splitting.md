# Key-Range Splitting Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Export, import and copy very large tables as key-range parts that run as separate tasks, in parallel wherever SQL Server allows it, chosen per table with `--split-tables`.

**Architecture:**
- **Pure code with unit tests:** part naming, `--split-tables` parsing, range and predicate building, boundary selection, manifest merging, load-mode and index rules, the work queue.
- **Thin database layer (`Program.Split.cs`, `Program.Indexes.cs`):** reads histograms, partitions and index shapes.
- **Shared scheduler:** export, import and copy become lists of work items (a whole table, or one range) run by one largest-first scheduler. Per-table before and after steps go through `TableWork`.
- **Unsplit tables:** a single work item with no range, so their output is unchanged.

**Tech Stack:** .NET 8, Microsoft.Data.SqlClient 7, SMO 181, CommandLineParser 2.9, Newtonsoft.Json, xUnit, bash, sqlcmd and bcp (mssql-tools18), PowerShell 7 (CI lock probe).

**Spec:** `docs/superpowers/specs/2026-11-23-key-range-splitting-design.md`. Section numbers below (§n) refer to it.

## Global Constraints

- C# files are UTF-8 with BOM and CRLF. Edit them with a script that preserves both (see earlier sessions' scratchpad pattern); never `sed -i` a `.cs` file. `Nullable` is disabled.
- Quote identifiers with `QuoteName()`. Boundary values only ever reach SQL through `KeyRange.TextLiteral()` inside `CONVERT(...)`. No raw user input in SQL.
- Shared counters in parallel code use `Interlocked`. Errors go through `PostToConsole(..., InfoLevel.Error)`, caught per part/table. Data-moving commands use `CommandTimeout = 0` / `BulkCopyTimeout = 0`.
- Connections come only from `SqlConnectionFactory.Create()`.
- Tables not matched by `--split-tables` must produce byte-identical files and manifest entries to v2.6.0.
- `--split-tables`: parts 2 to 200, default 8. Part file names: `<schema>.<table>.part0001.<ext>[.gz]` (four digits).
- Linux bcp: no `-x`, no `-o`. Use `ExecuteProcess(..., outputLogFile)`.
- Database behaviour is proven only by `tests/integration/roundtrip.sh` in CI (SQL Server 2019, 2022, 2025). The owner has no local SQL Server.
- Run `dotnet build BcpJson.sln -warnaserror` and `dotnet test BcpJson.sln` before every commit.
- End commit messages with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Work on branch `feature/split-tables` and open one PR. Merge only on the owner's explicit OK.

## Review Focus

1. **Boundary text containing `'`, `"`, `\` or non-ASCII** in a string split column, passed to bcp `queryout` on the command line. Expected: every row is exported exactly once. Pinned by Task 3 (`TextLiteral` tests) and Task 7 (`split.Text` keys with `'`, `"` and `\`).
2. **Concurrent writes during a split export without `--snapshot`.** Expected: a clear warning up front. Pinned by Task 2 (`Valid` warning test).
3. **A unique index that fails to rebuild after `--disable-indexes`** (duplicates loaded). Expected: an error naming the index, exit code 1, and every other disabled index still rebuilt. Pinned by Task 10 (`UX_Dup` integration case).
4. **Importing a split export into a target clustered on different columns.** Expected: correct data, sequential load without an order hint. Pinned by Task 10 (`split.BigOther` target).
5. **`--resume` with a different or missing `--split-tables`.** Expected: the recorded split is reused, with a warning. Pinned by Task 5 (unit test) and Task 7 (integration).

---

### Task 1: Part file names and import grouping

**Files:**
- Create: `src/BcpJson/PartFiles.cs`
- Modify: `src/BcpJson/Program.Import.cs` (`ImportBaseName` delegates to `PartFiles.TableNameOf`)
- Test: `tests/BcpJson.Tests/PartFilesTests.cs`

**Interfaces:**
- Produces:
  - `internal static class PartFiles`
  - `string PartName(string tableName, int part)` returns `"dbo.T.part0001"`.
  - `int PartNumberOf(string fileName)` returns 0 for non-part files.
  - `string TableNameOf(string fileName)`: strips `.gz`, the extension and `.partNNNN`.
  - `List<ImportGroup> GroupImportFiles(IEnumerable<string> fileNames, string targetTable)`.
  - `internal sealed record ImportGroup(string TableName, List<string> Files)`: files ordered by part number, then ordinal name. When `targetTable` is non-empty, all files form one group with that name.

- [ ] **Step 1: Write the failing tests**

```csharp
[Theory]
[InlineData("dbo.T.part0001.json", "dbo.T", 1)]
[InlineData("dbo.T.part0200.json.gz", "dbo.T", 200)]
[InlineData("dbo.T.part0003.bcp", "dbo.T", 3)]
[InlineData("dbo.T.json", "dbo.T", 0)]
[InlineData("dbo.T.partial.json", "dbo.T.partial", 0)]   // not four digits: an ordinary name
[InlineData("dbo.T.part01.json", "dbo.T.part01", 0)]
public void Part_names(string file, string table, int part)
{
    Assert.Equal(table, PartFiles.TableNameOf(file));
    Assert.Equal(table, Program.ImportBaseName(file));
    Assert.Equal(part, PartFiles.PartNumberOf(file));
}

[Fact] public void PartName_pads_to_four_digits() => Assert.Equal("dbo.T.part0007", PartFiles.PartName("dbo.T", 7));

[Fact]
public void Groups_parts_by_table_in_part_order()
{
    var groups = PartFiles.GroupImportFiles(new[] { "dbo.B.json", "dbo.A.part0002.json", "dbo.A.part0010.json", "dbo.A.part0001.json" }, null);
    Assert.Equal(new[] { "dbo.A", "dbo.B" }, groups.Select(g => g.TableName).OrderBy(n => n, StringComparer.Ordinal));
    Assert.Equal(new[] { "dbo.A.part0001.json", "dbo.A.part0002.json", "dbo.A.part0010.json" }, groups.Single(g => g.TableName == "dbo.A").Files);
}

[Fact]
public void Target_table_puts_every_file_in_one_group()
{
    var group = Assert.Single(PartFiles.GroupImportFiles(new[] { "x.part0002.json", "x.part0001.json" }, "dbo.Target"));
    Assert.Equal("dbo.Target", group.TableName);
    Assert.Equal(new[] { "x.part0001.json", "x.part0002.json" }, group.Files);
}
```

Also add to `ManifestTests`: `verify` reports an unlisted `dbo.T.part0001.json` as a warning, like other data files.

- [ ] **Step 2:** Run `dotnet test BcpJson.sln --filter FullyQualifiedName~PartFilesTests`. Expected: FAIL (`PartFiles` does not exist).
- [ ] **Step 3:** Implement `PartFiles` and make `Program.ImportBaseName` return `PartFiles.TableNameOf(fileName)`. A part suffix is `.part` followed by exactly four digits.
- [ ] **Step 4:** Run `dotnet test BcpJson.sln`. Expected: all pass.
- [ ] **Step 5:** Commit: "Part file names and import grouping".

### Task 2: `--split-tables` option

**Files:**
- Create: `src/BcpJson/SplitRequest.cs`
- Modify: `src/BcpJson/ExportOptions.cs`, `src/BcpJson/CopyOptions.cs`
- Test: `tests/BcpJson.Tests/SplitRequestTests.cs`, `tests/BcpJson.Tests/CommandLineTests.cs`

**Interfaces:**
- Produces:
  - `internal sealed record SplitRequest(string Pattern, int Parts)`, with constants `DefaultParts = 8`, `MinParts = 2`, `MaxParts = 200`.
  - `static bool TryParse(IEnumerable<string> values, out List<SplitRequest> requests, out string error)`.
  - `static int? PartsFor(IReadOnlyList<SplitRequest> requests, string tableDisplayName)`: first match wins; `*` is a wildcard; matching is case-insensitive; null means "don't split".
  - `ExportOptions.SplitTables` / `CopyOptions.SplitTables` (`[Option("split-tables", Separator = ',')] IEnumerable<string>`), and `List<SplitRequest> SplitRequests` filled by `Valid()`.

- [ ] **Step 1: Write the failing tests**

```csharp
[Fact]
public void Parses_patterns_and_part_counts()
{
    Assert.True(SplitRequest.TryParse(new[] { "dbo.History:32", "Sales.Order*", "odd:name:4" }, out var r, out _));
    Assert.Equal(new SplitRequest("dbo.History", 32), r[0]);
    Assert.Equal(new SplitRequest("Sales.Order*", 8), r[1]);
    Assert.Equal(new SplitRequest("odd:name", 4), r[2]);       // the last ":" followed by digits is the count
}

[Theory]
[InlineData("dbo.T:1")] [InlineData("dbo.T:201")] [InlineData("dbo.T:0")] [InlineData(":4")] [InlineData("")]
public void Rejects_bad_values(string value)
{
    Assert.False(SplitRequest.TryParse(new[] { value }, out _, out string error));
    Assert.Contains("--split-tables", error);
}

[Fact]
public void First_matching_pattern_wins()
{
    var r = new List<SplitRequest> { new("dbo.History", 32), new("dbo.*", 4) };
    Assert.Equal(32, SplitRequest.PartsFor(r, "DBO.history"));
    Assert.Equal(4, SplitRequest.PartsFor(r, "dbo.Other"));
    Assert.Null(SplitRequest.PartsFor(r, "sales.X"));
}
```

In `CommandLineTests`:
- `export --split-tables dbo.T` parses to one request with 8 parts.
- `export --split-tables dbo.T --source-query "select 1"` fails `Valid()` with an error containing `--split-tables`.
- `Valid()` with `--split-tables` and no `--snapshot` posts the warning `--split-tables without --snapshot: the parts of a table are read at different moments. Use --snapshot for an exact, consistent export.` Use the same text with "copy" for copy. Follow the existing pattern in `CommandLineTests` for checking posted messages, or give `Valid` an internal hook if none exists.

- [ ] **Step 2:** Run the new tests. Expected: FAIL.
- [ ] **Step 3:** Implement `SplitRequest` and the options. Help text: `Split tables into key ranges exported/copied in parallel: schema.table[:parts], * wildcard, parts 2-200 (default 8).`
- [ ] **Step 4:** Run `dotnet test BcpJson.sln`. Expected: all pass.
- [ ] **Step 5:** Commit: "--split-tables option".

### Task 3: Split columns, key ranges and SQL predicates

**Files:**
- Create: `src/BcpJson/KeyRange.cs`
- Test: `tests/BcpJson.Tests/KeyRangeTests.cs`

**Interfaces:**
- Produces `internal sealed class SplitColumn`:
  - properties `Name`, `TypeName` (system type name), `MaxLength` (short, bytes), `Precision`, `Scale` (byte), `Nullable`, `Descending`, `Collation`;
  - `string SqlType`, for example `decimal(18,4)`, `varchar(20)`, `nvarchar(30)` (max_length / 2), `varchar(max)` for -1, `datetime2(7)`, `binary(16)`, or the bare name;
  - `int? ConvertStyle`:

    | Types | Style |
    |---|---|
    | `datetime`, `datetime2`, `smalldatetime`, `date`, `datetimeoffset` | 126 |
    | `float`, `real` | 3 |
    | `money`, `smallmoney` | 2 |
    | `binary`, `varbinary` | 1 |
    | all others, including `time` | null |

  - `static bool IsSplittableType(string typeName)`, true for: `tinyint smallint int bigint decimal numeric money smallmoney bit float real date time datetime datetime2 smalldatetime datetimeoffset char varchar nchar nvarchar uniqueidentifier binary varbinary`.
- Produces `internal sealed class KeyRange`:
  - properties `int Part`, `string Lower`, `bool LowerInclusive`, `string Upper`, `bool UpperInclusive`, `bool IncludeNulls` (a null bound means unbounded);
  - `string Predicate(SplitColumn column)`;
  - `static string TextLiteral(string value)`;
  - `static string ValueExpression(SplitColumn column, string text)`, which returns `CONVERT(<SqlType>, <TextLiteral>[, <style>])`.

- [ ] **Step 1: Write the failing tests**

```csharp
static SplitColumn Col(string type, short len = 8, byte p = 0, byte s = 0, bool nullable = false) =>
    new SplitColumn { Name = "K", TypeName = type, MaxLength = len, Precision = p, Scale = s, Nullable = nullable };

[Fact]
public void Sql_types_and_styles()
{
    Assert.Equal("decimal(18,4)", Col("decimal", 9, 18, 4).SqlType);
    Assert.Equal("nvarchar(30)", Col("nvarchar", 60).SqlType);
    Assert.Equal("varchar(max)", Col("varchar", -1).SqlType);
    Assert.Equal("datetime2(7)", Col("datetime2", 8, 27, 7).SqlType);
    Assert.Equal("binary(16)", Col("binary", 16).SqlType);
    Assert.Equal(126, Col("datetime2", 8, 27, 7).ConvertStyle);
    Assert.Equal(3, Col("float").ConvertStyle);
    Assert.Null(Col("time", 5, 16, 7).ConvertStyle);
    Assert.False(SplitColumn.IsSplittableType("sql_variant"));
    Assert.False(SplitColumn.IsSplittableType("hierarchyid"));
}

[Fact]
public void Middle_part_is_half_open()
{
    var r = new KeyRange { Part = 2, Lower = "100", LowerInclusive = true, Upper = "200", UpperInclusive = false };
    Assert.Equal("([K] >= CONVERT(bigint, N'100') AND [K] < CONVERT(bigint, N'200'))", r.Predicate(Col("bigint")));
}

[Fact]
public void First_part_takes_nulls_and_last_part_is_open()
{
    var first = new KeyRange { Part = 1, Upper = "2026-01-01T00:00:00", UpperInclusive = false, IncludeNulls = true };
    Assert.Equal("([K] < CONVERT(datetime2(7), N'2026-01-01T00:00:00', 126) OR [K] IS NULL)", first.Predicate(Col("datetime2", 8, 27, 7, nullable: true)));
    var last = new KeyRange { Part = 3, Lower = "5", LowerInclusive = false };   // RANGE LEFT: lower bound exclusive
    Assert.Equal("([K] > CONVERT(int, N'5'))", last.Predicate(Col("int", 4)));
}

[Theory]
[InlineData("ab", "N'ab'")]
[InlineData("O'Brien", "N'O''Brien'")]
[InlineData("say \"hi\"", "(N'say ' + NCHAR(34) + N'hi' + NCHAR(34) + N'')")]
[InlineData(@"C:\x", "(N'C:' + NCHAR(92) + N'x')")]
public void Text_literals_are_safe_on_a_command_line(string value, string literal)
{
    Assert.Equal(literal, KeyRange.TextLiteral(value));
}
```

- [ ] **Step 2:** Run `--filter FullyQualifiedName~KeyRangeTests`. Expected: FAIL.
- [ ] **Step 3:** Implement. `TextLiteral` returns `SqlLiteral(value)` when the value has no `"` or `\`. Otherwise it returns a parenthesised concatenation that replaces each `"` with `NCHAR(34)` and each `\` with `NCHAR(92)`. Predicate format: `(<lower> AND <upper>)`, either bound dropped when null, plus ` OR [col] IS NULL` when `IncludeNulls`. When both bounds are null and `IncludeNulls` is false, the predicate is `1 = 1`.
- [ ] **Step 4:** Run `dotnet test BcpJson.sln`. Expected: all pass.
- [ ] **Step 5:** Commit: "Key ranges and range predicates".

### Task 4: Choosing ranges from histograms and partitions

**Files:**
- Create: `src/BcpJson/RangePlanner.cs`
- Test: `tests/BcpJson.Tests/RangePlannerTests.cs`

**Interfaces:**
- Consumes: `KeyRange` (Task 3).
- Produces (static class `RangePlanner`):
  - `internal sealed record HistogramStep(string Key, double RangeRows, double EqualRows)`
  - `List<int> ChooseHistogramSteps(IReadOnlyList<HistogramStep> steps, int parts)`: indexes of the steps whose keys become boundaries.
  - `List<KeyRange> HistogramRanges(IReadOnlyList<string> boundaries, bool nullable, bool descending)`
  - `List<(int First, int Last)> GroupPartitions(IReadOnlyList<long> rowsPerPartition, int parts)`: 1-based partition numbers.
  - `List<KeyRange> PartitionRanges(IReadOnlyList<string> boundaryValues, bool rangeRight, IReadOnlyList<(int First, int Last)> groups, bool nullable)`

- [ ] **Step 1: Write the failing tests**

```csharp
static List<RangePlanner.HistogramStep> Steps(params (double range, double eq)[] s) =>
    s.Select((x, i) => new RangePlanner.HistogramStep((i * 10).ToString(), x.range, x.eq)).ToList();

[Fact]
public void Even_histogram_splits_evenly()
{
    // 10 steps of 100 rows each on the step key: rows below step j = 100 * j, total 1000.
    var steps = Steps(Enumerable.Repeat((0.0, 100.0), 10).ToArray());
    Assert.Equal(new[] { 2, 5, 7 }, RangePlanner.ChooseHistogramSteps(steps, 4)); // targets 250/500/750; ties (200 vs 300, 700 vs 800) take the lower step
}

[Fact]
public void Fewer_steps_than_parts_gives_fewer_parts()
{
    Assert.Equal(new[] { 1, 2 }, RangePlanner.ChooseHistogramSteps(Steps((0, 5), (0, 5), (0, 5)), 8));
}

[Fact]
public void One_dominant_key_cannot_be_split()
{
    Assert.Empty(RangePlanner.ChooseHistogramSteps(Steps((0, 1000000)), 4));
}

[Fact]
public void Histogram_ranges_cover_everything_and_descending_numbers_from_the_top()
{
    var asc = RangePlanner.HistogramRanges(new[] { "10", "20" }, nullable: true, descending: false);
    Assert.Equal(3, asc.Count);
    Assert.True(asc[0].IncludeNulls); Assert.Null(asc[0].Lower); Assert.Equal("10", asc[0].Upper); Assert.False(asc[0].UpperInclusive);
    Assert.Equal("10", asc[1].Lower); Assert.True(asc[1].LowerInclusive); Assert.Equal("20", asc[1].Upper);
    Assert.Null(asc[2].Upper);
    var desc = RangePlanner.HistogramRanges(new[] { "10", "20" }, nullable: false, descending: true);
    Assert.Equal("20", desc[0].Lower); Assert.Null(desc[0].Upper); Assert.Equal(1, desc[0].Part);   // highest values first
    Assert.Null(desc[2].Lower); Assert.Equal(3, desc[2].Part);
}

[Theory]
[InlineData(new long[] { 100, 100, 100, 100 }, 2, "1-2,3-4")]
[InlineData(new long[] { 0, 100, 100, 0, 100, 100, 0 }, 2, "1-3,4-7")]
[InlineData(new long[] { 10, 1000, 10, 10 }, 3, "1-2,3-3,4-4")]
[InlineData(new long[] { 100, 0, 100 }, 5, "1-1,2-3")]
[InlineData(new long[] { 0, 0, 0 }, 4, "1-3")]
public void Partitions_are_grouped_contiguously(long[] rows, int parts, string expected)
{
    Assert.Equal(expected, string.Join(",", RangePlanner.GroupPartitions(rows, parts).Select(g => g.First + "-" + g.Last)));
}

[Fact]
public void Partition_ranges_follow_range_left_and_right()
{
    // 4 partitions, boundaries a|b|c; groups (1-2),(3-4).
    var right = RangePlanner.PartitionRanges(new[] { "a", "b", "c" }, true, new[] { (1, 2), (3, 4) }, nullable: true);
    Assert.Equal("b", right[0].Upper); Assert.False(right[0].UpperInclusive); Assert.True(right[0].IncludeNulls);
    Assert.Equal("b", right[1].Lower); Assert.True(right[1].LowerInclusive); Assert.Null(right[1].Upper);
    var left = RangePlanner.PartitionRanges(new[] { "a", "b", "c" }, false, new[] { (1, 2), (3, 4) }, nullable: false);
    Assert.True(left[0].UpperInclusive); Assert.False(left[1].LowerInclusive);
}
```

- [ ] **Step 2:** Run `--filter FullyQualifiedName~RangePlannerTests`. Expected: FAIL.
- [ ] **Step 3:** Implement the two algorithms below. The remaining methods follow from the tests.
  - **`ChooseHistogramSteps`:**
    ```
    below[j] = sum over i<j of (range[i] + eq[i]) + range[j]    // rows strictly below key j
    total    = sum of all range + eq
    for k in 1..parts-1: target = k*total/parts; pick j>last chosen with |below[j]-target| minimal (lowest j on ties),
                         skip j when below[j] <= below[last chosen] or below[j] == 0 or below[j] >= total
    return chosen (distinct, ascending)
    ```
  - **`GroupPartitions`:** greedy and contiguous. Close the current group when its rows ≥ remaining rows / remaining parts and rows remain after it. Then attach trailing groups with 0 rows to the previous group. Zero total rows gives a single group.
- [ ] **Step 4:** Run `dotnet test BcpJson.sln`. Expected: all pass.
- [ ] **Step 5:** Commit: "Range planning from histograms and partitions".

### Task 5: Manifest support for split tables

**Files:**
- Modify: `src/BcpJson/ExportManifest.cs`, `src/BcpJson/Program.Verify.cs`
- Test: `tests/BcpJson.Tests/ManifestTests.cs`

**Interfaces:**
- Consumes: `KeyRange` (Task 3).
- Produces:
  - `internal sealed class ManifestSplit`: `Kind` (`"histogram"` or `"partition"`), `Column`, `Type` (SqlType), `Descending`, `Nullable`, `PartitionFunction` (omitted when null), `RangeRight` (`bool?`, omitted when null), `PartCount`, `List<ManifestPart> Parts`.
  - `internal sealed class ManifestPart`: `Part`, `Rows`, `List<string> Files`, `Lower`, `LowerInclusive`, `Upper`, `UpperInclusive`, `IncludeNulls`; `KeyRange ToRange()`; `static ManifestPart From(KeyRange range, long rows, IEnumerable<string> files)`.
  - `ManifestTable.Split` (`[JsonProperty(NullValueHandling = Ignore)]`).
  - `void ExportManifest.RecordPart(ManifestTable shell, ManifestPart part, IList<ManifestFile> partFiles, string folder)`.
  - `List<int> ExportManifest.PartsToRedo(string name, string folder)`.
  - `IsIntact` is false for a split table missing parts.

- [ ] **Step 1: Write the failing tests**

```csharp
[Fact]
public void Parts_recorded_concurrently_merge_into_one_entry()
{
    var m = new ExportManifest { Format = "json" };
    ManifestTable Shell() => new ManifestTable { Name = "dbo.T", Split = new ManifestSplit { Kind = "histogram", Column = "Id", Type = "bigint", PartCount = 3, Parts = new() } };
    Parallel.For(1, 4, p =>
    {
        File.WriteAllText(Path.Combine(folder, $"dbo.T.part000{p}.json"), new string('x', p));
        m.RecordPart(Shell(), ManifestPart.From(new KeyRange { Part = p }, 10 * p, new[] { $"dbo.T.part000{p}.json" }),
                     new[] { ManifestFile.Describe(folder, $"dbo.T.part000{p}.json") }, folder);
    });
    var t = ExportManifest.Load(folder).Find("dbo.T");
    Assert.Equal(60, t.Rows);
    Assert.Equal(new[] { 1, 2, 3 }, t.Split.Parts.Select(p => p.Part));
    Assert.Equal(3, t.Files.Count);
    Assert.True(m.IsIntact("dbo.T", folder, true));
}

[Fact]
public void Missing_or_damaged_parts_are_redone()
{
    // Record parts 1 and 2 of a 3-part table, then truncate part 2's file.
    ...
    Assert.Equal(new[] { 2, 3 }, m.PartsToRedo("dbo.T", folder));
    Assert.False(m.IsIntact("dbo.T", folder, false));
}

[Fact]
public void Unsplit_entries_have_no_split_key()  // JSON of a Record()ed table has no "split" property
[Fact]
public void Verify_reports_a_split_table_with_missing_parts() // problem text: "dbo.T: 2 of 3 parts exported."
[Fact]
public void Part_ranges_round_trip()  // From(range).ToRange() keeps all six bound fields
```

- [ ] **Step 2:** Run the manifest tests. Expected: FAIL.
- [ ] **Step 3:** Implement. `RecordPart` runs under `sync`:
  - take the existing entry, or else the shell;
  - replace the part with the same number;
  - replace files with the same name in `Files`, and add the shell's `Files` (the BCP format file);
  - set `Rows` to the sum of the parts, or -1 if any part has -1;
  - keep the shell's `OrderedBy`, set `Exported` to now, then sort and save.
- [ ] **Step 4:** Run `dotnet test BcpJson.sln`. Expected: all pass.
- [ ] **Step 5:** Commit: "Manifest: split tables and parts".

### Task 6: Work queue

**Files:**
- Create: `src/BcpJson/WorkQueue.cs`
- Test: `tests/BcpJson.Tests/WorkQueueTests.cs`

**Interfaces:**
- Produces:
  - `internal sealed class TableWork`:
    - constructor `TableWork(string name, int items, Action before, Action after)`;
    - `void Run(Action item)`.
  - `internal static class WorkQueue`: `static void RunLargestFirst<T>(IEnumerable<T> items, Func<T, long> size, int maxTasks, Action<T> run)`.
- `TableWork.Run` semantics:
  - `before` runs once, on the first call; concurrent callers wait for it.
  - If `before` threw, `Run` throws `InvalidOperationException("<name>: preparation failed: <message>")` without running the item.
  - `after` runs exactly once, in the call that finishes the last item, even when items or `before` failed.
  - An exception from the item takes precedence over one from `after`.
- `RunLargestFirst` uses `Partitioner.Create(items.OrderByDescending(size), EnumerablePartitionerOptions.NoBuffering)` and `Program.Parallelism(maxTasks)`.

- [ ] **Step 1: Write the failing tests**
  - `Before_runs_once_and_after_once_after_the_last_item`: 8 items on 4 threads; `before` and `after` counters are both 1; `after` sees all 8 items done.
  - `After_runs_even_when_an_item_throws`: item 3 throws; the exception propagates; `after` ran once.
  - `Failed_before_skips_items_but_still_runs_after`: no item body runs; each `Run` throws `InvalidOperationException` containing "preparation failed"; `after` ran once.
  - `Largest_items_start_first`: with `maxTasks` 1, the recorded order is sizes 9, 5, 3, 1 for inputs 3, 9, 1, 5.
- [ ] **Step 2:** Run `--filter FullyQualifiedName~WorkQueueTests`. Expected: FAIL.
- [ ] **Step 3:** Implement with `Lazy<Exception>` (`ExecutionAndPublication`) for `before` and `Interlocked.Decrement` for the countdown.
- [ ] **Step 4:** Run `dotnet test BcpJson.sln`. Expected: all pass.
- [ ] **Step 5:** Commit: "Work queue: largest first, per-table before/after".

### Task 7: Split export (JSON and BCP) with resume

**Files:**
- Create: `src/BcpJson/Program.Split.cs`
- Modify:
  - `src/BcpJson/Program.Export.cs` (`ExportToJSON`, `ExportTableToJson`, `BuildTableSelect`, `ExportToBCP`, `ExportTableToBcp`, `TablesToExport`)
  - `src/BcpJson/Program.cs` (`ExportData`)
- Modify tests:
  - `tests/integration/schema.sql`, `tests/integration/data.sql`, `tests/integration/roundtrip.sh` (new `split` mode, export half)
  - `.github/workflows/ci.yml` (step "Split tables (key ranges and partitions)" after Compression, same `if:`)

**Interfaces:**
- Consumes: Tasks 1-6.
- Produces:
  - `internal sealed class TableSplit`: `SplitColumn Column`, `string Kind`, `string PartitionFunction`, `bool? RangeRight`, `List<KeyRange> Ranges`, `List<long> EstimatedRows`; `ManifestSplit ToManifest()`; `static TableSplit FromManifest(ManifestSplit split)`.
  - `static TableSplit PlanSplit(SqlConnection conn, TableRef table, int parts, out string reason)`: null plus a reason when the table cannot be split.
  - `static Dictionary<int, long> TableRowCounts(SqlConnection conn)`: object_id to rows, from `sys.partitions` with index_id 0/1.
  - `private sealed record ExportItem(TableRef Table, KeyRange Range, TableSplit Split, long Size)`: `Range == null` means the whole table.
  - `BuildTableSelect(SqlConnection conn, TableRef table, bool noLock, string where)` adds `WHERE <predicate>` before `ORDER BY`.

- [ ] **Step 1: Add the integration data.**
  - **`schema.sql`:** schema `split`, a partition function `pf_SplitMonth` (`RANGE RIGHT FOR VALUES` on `datetime2(7)`, 2026-01-01 to 2026-06-01 monthly) and scheme `ps_SplitMonth` (`ALL TO ([PRIMARY])`), plus:
    - `split.Big`: `Id bigint` PK clustered, `Stamp datetime2(7)`, `Code varchar(20)`, `Amount decimal(18,4)`, `Note nvarchar(50) NULL`; indexes `IX_Big_Stamp` (Stamp) and unique `UX_Big_Code` (Code).
    - `split.BigOther`: same columns, clustered on `Stamp` (not unique), no PK. Import target for Review Focus 4.
    - `split.Text`: `Code varchar(30) COLLATE Latin1_General_CS_AS` PK clustered, `Seq int`.
    - `split.NullKey`: `K int NULL`, `Id int`; non-unique clustered index on `K`.
    - `split.Descending`: `Id bigint`, PK clustered `(Id DESC)`, `Val int`.
    - `split.Parted`: `Id int`, `Stamp datetime2(7)`, `Val int`; PK clustered `(Id, Stamp)` on `ps_SplitMonth(Stamp)`.
    - `split.PartedHeap`: same columns, heap on `ps_SplitMonth(Stamp)`.
    - `split.PartedCci`: same columns, clustered columnstore on `ps_SplitMonth(Stamp)`.
    - `split.Heap`: unpartitioned heap.
  - **`data.sql`:**
    - `split.Big`: 200,000 rows from a numbers CTE (`Code = CONCAT('C', Id)`, Note NULL every 7th).
    - `split.Text`: 3,000 keys, including `O'Brien`, `say "hi"`, `C:\x`, `Zoë`, `a`/`A`/`b`/`B`.
    - `split.NullKey`: 20,000 rows, every 10th K NULL.
    - `split.Descending`: 20,000 rows, `Val = Id % 100` (duplicates, used by Task 10's `UX_Dup` case).
    - `split.Parted`, `split.PartedHeap`, `split.PartedCci`: 30,000 rows each spread over Jan-Jun 2026, leaving partitions before 2026-01-01 and from 2026-06-01 empty.
    - `split.Heap`: 1,000 rows.
    - Then `UPDATE STATISTICS ... WITH FULLSCAN` on each table, because histograms built at index creation are empty.
- [ ] **Step 2: Add the export half of `roundtrip.sh split`.** It exports JSON with `--include-schemas split --split-tables "split.*:8" --snapshot` into `$WORK/split-json`, then asserts:
  - `split.Big`, `split.NullKey` and `split.Descending` have 8 part files each;
  - `split.Text`, `split.Parted`, `split.PartedHeap` and `split.PartedCci` have at least 2;
  - `split.Heap` has one unsplit file, and the log contains `split.Heap: not split:`;
  - `bcpjson verify` passes;
  - a helper `join_parts dir table` (concatenate parts in order, dropping each part's closing `]` line and the next part's leading `[`) equals `$WORK/source-json/split.<table>.json` for Big, Text, NullKey and Descending;
  - for the three Parted tables, sorted lines are equal (partition column not leading);
  - every manifest entry with parts has `split.partCount == (split.parts | length)`.

  Then BCP: export `--file-format bcp --include-tables split.Big,split.Text,split.Descending,split.Parted --split-tables "split.*:4"` and verify (the import half follows in Task 10). Then resume:
  - delete `split.Big.part0003.json` and remove part 3 from the manifest with `jq`;
  - rerun with `--resume` and **without** `--split-tables`;
  - the log contains `keeps its recorded split (8 parts)` and `1 part(s) to go`;
  - `cmp` the redone part against the original.

  Run it with `bash -n tests/integration/roundtrip.sh` for syntax.
- [ ] **Step 3: Implement `PlanSplit`.** Order of checks:
  1. **Partitioned** (the data space of index 0/1 is a partition scheme): the column comes from the `index_columns` row with `partition_ordinal = 1`. Boundaries: `SELECT CONVERT(nvarchar(4000), CAST(prv.value AS <SqlType>)[, style]) FROM sys.partition_range_values prv ... ORDER BY boundary_id`. A NULL boundary returns the reason "a partition boundary is NULL". Rows per partition come from `sys.partitions` (index_id 0/1). Ranges come from `GroupPartitions` and `PartitionRanges`; Kind is `partition`.
  2. **Clustered rowstore** (type 1): the first key column (`key_ordinal = 1`, with `is_descending_key`). The histogram comes from `sys.dm_db_stats_histogram(@objectId, 1)`, converted to text the same way. Ranges come from `ChooseHistogramSteps` and `HistogramRanges`; Kind is `histogram`.
  3. Otherwise, return a reason.

  Reasons (exact text after `not split: `):
  - `no clustered rowstore index and not partitioned`
  - `split column type <t> is not supported`
  - `varchar split column with a code page other than the database's` (compare `COLLATIONPROPERTY(col collation, 'CodePage')` with the database collation's)
  - `no statistics histogram on the clustered index (run UPDATE STATISTICS)`
  - `the key has too few distinct values`
  - `a partition boundary is NULL`

  A split that yields a single range also returns null, with the reason "the key has too few distinct values".
- [ ] **Step 4: Rework export onto `ExportItem`s and `WorkQueue.RunLargestFirst`.**
  - One `TableRowCounts` query sizes the items. A split table gets one `TableWork` (BCP `before` writes the format file; JSON `before` is empty; `after` is empty).
  - **JSON:** file `options.DataFileName(PartFiles.PartName(name, part), ".json")`; the select adds `range.Predicate(split.Column)`; `WithRetries` per part; `manifest.RecordPart(shell, ManifestPart.From(...), describedFiles, exportPath)`. The shell carries `OrderedBy` and `split.ToManifest()` (empty `Parts`).
  - **BCP:** `queryout` with `SELECT * FROM <table> [WITH (NOLOCK)] WHERE <predicate>`; per-part log names `<partName>_output.log` and `<partName>_error.log`; the format file `<table>-c.fmt` is shared and goes in the shell's `Files`.
  - **Unsplit tables:** keep the exact current code path (`Range == null`): same file names and `manifest.Record` call.
  - **Messages:**
    - `{table}: split into {n} parts by {column} ({kind}).`
    - `{table}: not split: {reason}.` as a warning.
  - **Resume:** when the manifest has `Split` for a table, use `TableSplit.FromManifest` and `PartsToRedo`; if the requested parts differ or are absent, warn `{table}: --resume keeps its recorded split ({n} parts).` and print `{table}: {k} part(s) to go.` A table whose entry is intact is skipped as before; keep today's `--resume: X of Y table(s) already exported, N to go.` line unchanged for unsplit tables.
  - `--split-tables` is ignored for `--source-query` (Task 2 already rejects that combination).
- [ ] **Step 5:** Run `dotnet build BcpJson.sln -warnaserror` and `dotnet test BcpJson.sln`. Expected: all pass. Push the branch, open the draft PR, and watch CI. Expected: the existing steps and the new `split` step pass on all three SQL Server versions. If CI fails, read the job log, fix and push.
- [ ] **Step 6:** Commit: "Split export by key range and partition, with per-part resume".

### Task 8: Load planning rules

**Files:**
- Create: `src/BcpJson/LoadPlanning.cs`
- Test: `tests/BcpJson.Tests/LoadPlanningTests.cs`

**Interfaces:**
- Produces (static class `LoadPlanning`):
  - `enum LoadMode { Parallel, Sequential }`
  - `sealed record TargetShape(bool ClusteredRowstore, bool ClusteredColumnstore, int EnabledNonclusteredIndexes, bool Partitioned)`
  - `LoadMode ChooseLoadMode(TargetShape shape, bool tableLock, bool stagedClrColumns)`
  - `sealed record IndexInfo(int IndexId, string Name, byte Type, bool IsPrimaryKey, bool IsDisabled, bool ReferencedByForeignKey, bool IsSecondaryXml)`
  - `List<IndexInfo> IndexesToDisable(IEnumerable<IndexInfo> indexes)`
  - `List<IndexInfo> RebuildOrder(IEnumerable<IndexInfo> disabled)`: non-secondary-XML first, then by `IndexId`.
  - `const bool ClusteredColumnstoreLoadsInParallel = false` (Task 9 confirms or flips it).

- [ ] **Step 1: Write the failing tests** for the rules of §9 and §11:

| shape (CR, CCI, enabledNC) | tableLock | staged | expected |
|---|---|---|---|
| heap, 0 | true | false | Parallel |
| heap, 2 | true | false | Sequential |
| clustered rowstore, 0 | true | false | Sequential |
| CCI, 0 | true | false | `ClusteredColumnstoreLoadsInParallel ? Parallel : Sequential` |
| clustered rowstore, 3 | false | false | Parallel |
| heap, 0 | true | true | Sequential (CLR staging inserts with TABLOCK) |

  Add `IndexesToDisable` cases:
  - kept: type 0, 1 and 5, the PK (nonclustered PK included), FK-referenced indexes, already-disabled indexes;
  - disabled: types 2, 3, 4, 6 and 7 otherwise, including unique non-referenced ones.

  Add a `RebuildOrder` case: a secondary XML index with a lower id still comes after the primary XML index.
- [ ] **Step 2:** Run `--filter FullyQualifiedName~LoadPlanningTests`. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run `dotnet test BcpJson.sln`. Expected: all pass.
- [ ] **Step 5:** Commit: "Load planning: modes and indexes to disable".

### Task 9: Lock probe in CI (settles the columnstore rule)

**Files:**
- Create: `tests/integration/lock_probe.ps1`
- Modify:
  - `.github/workflows/ci.yml` (step "Bulk load lock modes" after Setup: `shell: pwsh`)
  - `src/BcpJson/LoadPlanning.cs` (constant)
  - `tests/BcpJson.Tests/LoadPlanningTests.cs`

**What the probe does.** For `split.Heap`, `split.Big` and `split.PartedCci`, in a scratch database built from `schema.sql`, the script:
1. opens connection A with `System.Data.SqlClient` (bundled with PowerShell 7) and begins a transaction;
2. runs `SqlBulkCopy` with `TableLock` in that transaction and writes one row (a `DataTable` matching the table's columns);
3. from connection B, reads `request_mode` from `sys.dm_tran_locks` for `resource_type = 'OBJECT'` and that object id, where the session is A's `@@SPID`;
4. rolls back.

It prints `heap=<mode> clustered=<mode> cci=<mode>` and exits 1 unless heap is `BU` and clustered is `X`.

- [ ] **Step 1:** Write the script and the CI step, push, and read the CCI result from the log on all three versions.
- [ ] **Step 2:** If CCI is `BU` on all three versions, set `ClusteredColumnstoreLoadsInParallel = true` and change its unit test row to `Parallel`. Otherwise leave it false. Then make the probe also assert the CCI mode it found, with a comment naming the observed mode.
- [ ] **Step 3:** Run `dotnet test BcpJson.sln`, push, and check CI is green. Then commit: "CI: bulk-load lock probe; columnstore load mode".

### Task 10: Split-aware import with `--disable-indexes`

**Files:**
- Create: `src/BcpJson/Program.Indexes.cs`
- Modify:
  - `src/BcpJson/ImportOptions.cs` (`[Option("disable-indexes")] bool DisableIndexes`, help: `Disable nonclustered indexes (except the primary key and indexes used by foreign keys) during the load and rebuild them afterwards.`)
  - `src/BcpJson/Program.Import.cs` (`ImportFromJSON`, `ImportFromBCP`, `ImportJsonFile`)
  - `tests/integration/roundtrip.sh` (import half of `split`)
- Test: `tests/BcpJson.Tests/CommandLineTests.cs` (`--disable-indexes` parses; default false)

**Interfaces:**
- Consumes: `PartFiles.GroupImportFiles`, `TableWork`, `WorkQueue`, `LoadPlanning`, `SortKeyOf`.
- Produces:
  - `static TargetShape ReadTargetShape(SqlConnection conn, int objectId, ISet<int> indexIdsToBeDisabled)`
  - `static List<IndexInfo> ReadIndexes(SqlConnection conn, int objectId)`
  - `static void DisableIndexes(SqlConnection conn, string qualifiedName, IEnumerable<IndexInfo> indexes)`
  - `static int RebuildIndexes(SqlConnection conn, string qualifiedName, IEnumerable<IndexInfo> indexes)`: returns failures, each already posted as an error.
  - `static string LockEscalation(SqlConnection conn, int objectId)`

- [ ] **Step 1: Add the import half of `roundtrip.sh split`.**
  - **(a) Plain import.** Into a fresh `BcpJsonSplit` (schema.sql) import `$WORK/split-json` (`--source-file '*.json' --max-tasks 4`), then:
    - assert log lines `split.Big: 8 parts, loading one after another`, `split.PartedHeap: ... loading in parallel` and `split.Heap:` (unsplit, no part line);
    - `split.Big` logs the order hint for each part;
    - JSON-export the split schema and compare it with the source export of the split schema (`compare_exact` on `--include-schemas split` exports).
  - **(b) `--disable-indexes`.** Into fresh `BcpJsonSplitIdx`:
    - assert the log `Disabled indexes: IX_Big_Stamp, UX_Big_Code` for split.Big;
    - afterwards `SELECT COUNT(*) FROM sys.indexes WHERE is_disabled = 1` is 0;
    - the data compares equal.
  - **(c) Unique index rebuild failure (Review Focus 3).** In `BcpJsonSplitIdx`:
    - create `UX_Dup` unique on `split.Descending(Val)`, where the data has duplicate `Val` values;
    - truncate and re-import `split.Descending` with `--disable-indexes --truncate-target`;
    - expect exit 1, the log `Could not rebuild index UX_Dup`, and the PK still enabled.
  - **(d) Different clustered key (Review Focus 4).** Import the `split.Big` parts with `--target-table split.BigOther`: 200,000 rows, no order-hint line.
  - **(e) BCP.** Import the Task 7 BCP export into fresh `BcpJsonSplitBcp` and compare those four tables.
  - **(f) Truncate once.** Import `split.Big` parts twice with `--truncate-target`: 200,000 rows, not 25,000. This guards the per-table truncate.
- [ ] **Step 2: Implement.**
  - **Planning pass, serial, one connection:** for each `ImportGroup`, resolve the target (`GetTableObject`), read its indexes, compute `toDisable` (when `--disable-indexes`), the shape after disabling, and staging (any CLR column), then `ChooseLoadMode`. Log `{table}: {n} parts, loading in parallel.` or `{table}: {n} parts, loading one after another ({reason}).`, where reason is `clustered index`, `enabled nonclustered indexes`, `CLR columns` or `columnstore`. Groups with one unsplit file log nothing new.
  - **Partitioned target with `--no-table-lock`:** when `LockEscalation` is not `AUTO`, post Info `{table}: partitioned; ALTER TABLE ... SET (LOCK_ESCALATION = AUTO) lets parts in different partitions load without blocking each other.`
  - **`TableWork` per group:**
    - `before` truncates once (when asked), then `DisableIndexes` and logs `{table}: Disabled indexes: a, b.`;
    - `after` runs `RebuildIndexes` in `RebuildOrder` and logs `{table}: Rebuilt n index(es).` On failure it posts the error `{table}: Could not rebuild index {name}: {message}. It is still disabled; run ALTER INDEX {name} ON {table} REBUILD.`
  - **Items:** Parallel mode gives one item per file. Sequential mode gives one item that loads the files in order. Sizes come from file lengths.
  - **Remove truncation** from `ImportJsonFile` and the BCP per-file body.
  - **BCP log names** use the file's own base name without `.gz` (`dbo.T.part0001_import_output.log`).
  - **Order hints** (v2.6.0 rules) apply per part unchanged.
- [ ] **Step 3:** Run `dotnet test BcpJson.sln`, push, and check CI is green on all three versions.
- [ ] **Step 4:** Commit: "Split-aware import: load modes, truncate once, --disable-indexes".

### Task 11: Split copy

**Files:**
- Modify: `src/BcpJson/Program.Copy.cs` (`BulkCopyTables`, `CopyTable`, `CopyTableViaBulkCopy`), `tests/integration/roundtrip.sh` (`split` mode, copy part)

**Interfaces:**
- Consumes: `PlanSplit`, `TableWork`, `WorkQueue`, `LoadPlanning`, `SplitRequest.PartsFor`.
- Produces: `private sealed class CopyTableContext { List<string> Columns; string TargetName; bool Created; bool NeedsBcp; LoadMode Mode; }`, filled by a table's `before`.

- [ ] **Step 1: Add the copy case to `split`.**
  - `copy --include-schemas split --split-tables "split.*:4" --snapshot` into an empty `BcpJsonSplitCopy`.
  - The log has `split.Big: 4 parts, loading in parallel.`: the target was created as a heap.
  - Indexes exist afterwards (compare `IX_Big_Stamp` presence).
  - A JSON export of the copy equals the source split export.
  - Then copy again into the existing database with `--truncate-target`: the log has `loading one after another (clustered index)`, and the data still matches.
- [ ] **Step 2: Implement.**
  - Split `CopyTable` into:
    - `before`: SMO connect, `PrepareTargetTable`, columns, `NeedsBcp`, shape, mode; disconnect.
    - one range copy per part: `CopyTableViaBulkCopy(setting, context, tableRef, KeyRange range, SplitColumn column)`, whose select adds the predicate when the range is not null;
    - `after`: when created, reconnect SMO and run `CreateTargetIndexes` and `CreateTargetCheckConstraints`, then add to `createdTables`.
  - Unsplit tables are one item through the same path with a null range.
  - `NeedsBcp` tables are never split: warning `{table}: not split: copied through bcp (geography/geometry/hierarchyid columns).`
  - Ranges are planned on the source connection (the snapshot with `--snapshot`).
  - The `--drop-target` pre-pass, foreign keys and triggers stay where they are.
- [ ] **Step 3:** Run `dotnet test BcpJson.sln`, push, and check CI is green (the existing `copy` step must still pass unchanged).
- [ ] **Step 4:** Commit: "Split copy: parallel range streams".

### Task 12: Benchmark workflow

**Files:**
- Create: `.github/workflows/benchmark.yml`, `tests/integration/benchmark.sh`

- [ ] **Step 1: Write `benchmark.yml`.** `workflow_dispatch` with input `rows` (default `5000000`). One job on `ubuntu-latest` with a SQL Server 2022 service (same env and health check as ci.yml), mssql-tools18, a linux-x64 publish, then `bash tests/integration/benchmark.sh "$ROWS"`.
- [ ] **Step 2: Write `benchmark.sh`.** It creates `BenchSource` with `bench.History`:
  - columns: `Id bigint` identity PK clustered, `Stamp datetime2(7)`, `Account int`, `Amount decimal(18,4)`, `Memo nvarchar(80)`;
  - `IX_History_Stamp`, `IX_History_Account`;
  - `ROWS` rows (set-based insert from cross-joined `sys.all_objects`), then `UPDATE STATISTICS ... WITH FULLSCAN`.

  It times each run with `date +%s.%N`:
  - export JSON unsplit vs `--split-tables bench.History:8`, and the same for BCP;
  - import the split JSON into:
    - (a) an indexed copy;
    - (b) the same with `--disable-indexes`;
    - (c) a bare heap;
    - (d) a bare heap, then `CREATE CLUSTERED INDEX` plus both nonclustered indexes timed separately (option 2 by hand).

  It writes a Markdown table of seconds and rows/s to `$GITHUB_STEP_SUMMARY`.
- [ ] **Step 3:** Push and run it once with `gh workflow run benchmark.yml --ref feature/split-tables -f rows=1000000`. Expected: success and a summary table. Record the numbers in the PR description.
- [ ] **Step 4:** Commit: "Benchmark workflow for split loads".

### Task 13: Documentation

**Files:**
- Modify: `README.md`, `CLAUDE.md`, `docs/ROADMAP.md`, `docs/superpowers/specs/2026-11-23-key-range-splitting-design.md` (status line to "implemented in PR #n")

- [ ] **Step 1: `README.md`.**
  - New section "Splitting large tables": when to use it, `--split-tables` syntax, ranges (histogram vs partitions), the load-mode table from §9, `--disable-indexes`, the `--snapshot` advice, resume, and the benchmark numbers from Task 12.
  - Option rows for export, import and copy.
  - Output files (`.partNNNN`) and the manifest `split` example.
  - Known limitations: copy creates partitioned tables unpartitioned; a dominant leading key value can't be split; `--batch-size 0` is per part.
- [ ] **Step 2: `CLAUDE.md`.**
  - Verbs table notes for export, import and copy.
  - Code layout entries: `PartFiles.cs`, `SplitRequest.cs`, `KeyRange.cs`, `RangePlanner.cs`, `WorkQueue.cs`, `LoadPlanning.cs`, `Program.Split.cs`, `Program.Indexes.cs`.
  - The roundtrip mode list gets `split`; mention the lock probe and the benchmark workflow.
- [ ] **Step 3: `docs/ROADMAP.md`.** Replace "TODO: extremely large tables" with a "Done after v2.6.0" entry, and move the design's "Later" list into a roadmap backlog section, including staging tables and partition switching.
- [ ] **Step 4:** Run `dotnet test BcpJson.sln`, push, and check CI is green. Mark the PR ready for review. Then commit: "Docs: splitting large tables".
