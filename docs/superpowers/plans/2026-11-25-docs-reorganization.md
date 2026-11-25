# Documentation Reorganization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the 708-line, feature-ordered README with a short README, six task guides, a per-verb reference and CONTRIBUTING.md. Unit tests keep the options documented and the links valid.

**Architecture:** Plain Markdown in `docs/guides/` and `docs/reference/`. `tests/BcpJson.Tests/DocsTests.cs` checks the docs against the `[Option]` attributes and the files on disk. `release.yml` ships the user docs in every package.

**Tech Stack:** Markdown (GitHub-flavoured), xUnit, CommandLineParser 2.9 attributes (`CommandLine.OptionAttribute`), GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-11-25-docs-reorganization-design.md`

## Global Constraints

- **Files**, exactly as named:
  - `README.md`, `CONTRIBUTING.md`;
  - `docs/guides/back-up-a-database.md`, `restore-an-export.md`, `copy-between-servers.md`, `large-tables.md`, `recover-a-failed-run.md`, `load-into-a-live-database.md`;
  - `docs/reference/export.md`, `import.md`, `copy.md`, `verify.md`, `common.md`, `manifest.md`, `json-format.md`, `limitations.md`.
- **Source text:** the old README is `git show master:README.md`. Every fact in it lands on the page that the spec's section 2 table names. Text may be shortened only where it is duplicated.
- **No behaviour changes.** Do not change the C# code or the help texts. The only C# change is the new test file.
- **Shipped files:** `LICENSE`, `NOTICE`, `README.md`, `docs/guides/`, `docs/reference/`. Links in those docs point only at shipped files. Anything else uses `https://github.com/chrisgelhaus/BCPJSON/blob/master/<path>`.
- **Anchors:** the README keeps a `## Install` heading, because `SECURITY.md` links to `README.md#install`.
- **Guide shape:**
  - one opening sentence on when to use the guide;
  - numbered steps with commands;
  - `## Variations`;
  - `## If something goes wrong`;
  - `## See also`.
- **Reference shape:** option tables with the columns `Option | Default | Description`. The option cell shows `` `--long-name` `` (plus `` `-x` `` when the option has a short name). `export -k` and `copy --source-query` are marked "Not implemented" and also listed in `limitations.md`.
- **Example names:** the same placeholder names in every page: server `sql01` / `sql02`, database `Sales`, folder `D:\exports\Sales`.
- **Encoding:**
  - Markdown files: UTF-8 with BOM and CRLF, like the existing docs.
  - C# files: UTF-8 with BOM and CRLF. The repo stores LF and `.gitattributes` sets `*.cs text eol=crlf`.
- **Specs and plans:** the older files under `docs/superpowers` are not edited.

## Review Focus

1. **A fact from the old README that lands nowhere.** Examples: a warning message text, a fallback reason, a default value, the `.partial` behaviour. The reader expects every documented behaviour to still be documented. Check per section of the old README against the spec's section 2 table (Tasks 2–4 reviewers, and the final review).
2. **A guide whose commands don't run as written.** Examples: an option name that doesn't exist, a missing required option, `--source-file` patterns quoted wrongly for PowerShell vs bash. Copy-paste commands must work. Test: Task 1's `Every_documented_option_exists` fails on unknown `--names` in code spans of the guides and reference pages.
3. **A link that works on GitHub but not inside an unpacked package** (to `CONTRIBUTING.md`, `docs/ROADMAP.md`, `.github/...`, `SECURITY.md`). Test: Task 4's `User_docs_link_only_to_shipped_files`.
4. **A heading renamed after another page linked to it by anchor.** Test: Task 1's `Relative_links_and_anchors_resolve`.
5. **A new option added later without docs.** Test: Task 2's `Every_option_is_documented_on_its_verb_page`.

---

### Task 1: Docs test helpers and the link test

**Files:**
- Create: `tests/BcpJson.Tests/DocsTests.cs`

**Interfaces:**
- Produces (in `DocsTests.cs`, `internal static class DocsFiles`):
  - `string RepoRoot()`: the first ancestor of `AppContext.BaseDirectory` that contains `BcpJson.sln`.
  - `IEnumerable<string> DocFiles()`: `README.md`, `CONTRIBUTING.md` if it exists, and all `*.md` files under `docs/guides` and `docs/reference`, as full paths.
  - `IEnumerable<(string Target, string Anchor)> RelativeLinks(string markdown)`:
    - covers inline links `[text](target)` that are not `http(s):` or `mailto:`;
    - `Target` is the path part and may be empty for `#anchor` links;
    - `Anchor` is the fragment, or null;
    - ignores links inside fenced code blocks and inline code.
  - `IReadOnlySet<string> Anchors(string markdown)`:
    - builds the GitHub slugs of all ATX headings (`#` to `######`), skipping fenced code blocks;
    - slug rule: lower-case; delete every character that is not a letter, digit, space, `-` or `_`; replace each space with `-`;
    - a repeated slug gets `-1`, `-2`, … in order.
  - `IEnumerable<string> CodeSpanOptions(string markdown)`: every `--name` token (regex `--[a-z][a-z0-9-]*`) found inside inline code spans or fenced code blocks.

- [ ] **Step 1: Write the failing tests**

```csharp
[Theory]
[InlineData("Install", "install")]
[InlineData("Load as a heap: `--load-as-heap`", "load-as-a-heap---load-as-heap")]
[InlineData("Keys and indexes", "keys-and-indexes")]
[InlineData("JSON format", "json-format")]
public void Slug(string heading, string expected) =>
    Assert.Contains(expected, DocsFiles.Anchors("## " + heading + "\n"));

[Fact]
public void Repeated_headings_get_numbered_slugs() =>
    Assert.Equal(new[] { "copy", "copy-1" }, DocsFiles.Anchors("## Copy\n### Copy\n").OrderBy(x => x));

[Fact]
public void Links_outside_code_only()
{
    var links = DocsFiles.RelativeLinks("[a](x.md#b) `[c](y.md)` [d](https://e) [f](#g)\n```\n[h](z.md)\n```\n").ToList();
    Assert.Equal(new[] { ("x.md", "b"), ("", "g") }, links);
}

[Fact]
public void Relative_links_and_anchors_resolve()
{
    // For every DocFiles() file: each RelativeLinks target exists (relative to the file's folder;
    // "" means the file itself), and when Anchor != null it is in Anchors() of the target if the target is .md.
    // Collect all failures and assert the list is empty, so one run shows every broken link.
}
```

- [ ] **Step 2: Run them and confirm they fail.** Run `dotnet test BcpJson.sln --filter DocsTests`. Expected: they fail to compile, because `DocsFiles` is missing.

- [ ] **Step 3: Implement `DocsFiles`** as described in Interfaces, in the same file (namespace `bcpJson.Tests`).

- [ ] **Step 4: Run them and confirm they pass.** Run `dotnet test BcpJson.sln`. Expected: all pass, with the old README still in place, since its links resolve today. If the old README has a broken link, fix the link in `README.md`, not the test.

- [ ] **Step 5: Commit.** Message: `Docs tests: link and anchor checker`.

### Task 2: Reference pages

**Files:**
- Create: `docs/reference/export.md`, `import.md`, `copy.md`, `verify.md`, `common.md`, `manifest.md`, `json-format.md`, `limitations.md`
- Modify: `tests/BcpJson.Tests/DocsTests.cs`

**Interfaces:**
- Consumes: `DocsFiles.RepoRoot()`, `DocsFiles.CodeSpanOptions(string)`.
- Produces: the anchors that guides link to. Each page's headings are its public interface. The anchors Task 3 may use: `common.md#connecting`, `#table-filters`, `#parallelism`, `#retries`, `#logging`, `#exit-codes`; `manifest.md#the-split-object`; `import.md#loading-options`.

- [ ] **Step 1: Write the failing tests**

```csharp
public static TheoryData<Type, string> Verbs => new()
{
    { typeof(ExportOptions), "export.md" }, { typeof(ImportOptions), "import.md" },
    { typeof(CopyOptions), "copy.md" },     { typeof(VerifyOptions), "verify.md" },
};

[Theory, MemberData(nameof(Verbs))]
public void Every_option_is_documented_on_its_verb_page(Type options, string page)
{
    // For every property with CommandLine.OptionAttribute: name = LongName non-empty ? "--" + LongName : "-" + ShortName.
    // The page text (docs/reference/<page>) must contain "`" + name + "`". Collect every missing name and assert the list is empty.
}

[Fact]
public void Every_documented_option_exists()
{
    // The union of all "--" + LongName over the four options classes, plus "--help" and "--version".
    // Every CodeSpanOptions() token in docs/guides/*.md and docs/reference/*.md must be in that set. Report each unknown token with its file.
}
```

- [ ] **Step 2: Run them and confirm they fail.** Run `dotnet test BcpJson.sln --filter DocsTests`. Expected: FAIL, because the pages are missing.

- [ ] **Step 3: Write the eight pages.**
  - Option descriptions start from each property's `HelpText` and add the details from the old README: defaults, interactions, the related messages.
  - Group the options under `##` headings. `import.md` must have `## Loading options` (table lock, batch size, `--disable-indexes`, `--load-as-heap`, `--truncate-target`).
  - `common.md` holds the sections listed under Interfaces.
  - `manifest.md` holds the fields of `bcpjson-manifest.json`, `## The split object`, and how `verify`, `--resume` and import use the manifest.
  - `json-format.md` and `limitations.md` are moved from the old README. `limitations.md` adds `export -k` and `copy --source-query`.

- [ ] **Step 4: Run them and confirm they pass.** Run `dotnet test BcpJson.sln`. Expected: all pass, including `Relative_links_and_anchors_resolve` for the new pages.

- [ ] **Step 5: Commit.** Message: `Docs: per-verb reference, common concepts, manifest, JSON format, limitations`.

### Task 3: Task guides

**Files:**
- Create: the six files under `docs/guides/` (Global Constraints)

**Interfaces:**
- Consumes: the reference anchors from Task 2.
- Produces: the anchors the README links to, one per guide, which is the file itself. `large-tables.md#load-as-a-heap` and `recover-a-failed-run.md#the-load-as-heap-recovery-script` are linked from each other.

- [ ] **Step 1: Write the guides**, following the guide shape in Global Constraints and the contents in spec section 1. Move each piece of text according to the spec's section 2 table:
  - the table-locking explanation goes to `load-into-a-live-database.md`;
  - `--snapshot` goes to `back-up-a-database.md`;
  - the load-as-heap recovery goes to `recover-a-failed-run.md`;
  - "how the ranges are chosen" and "tables that are not split" go to `large-tables.md`.

  Option details are links to `../reference/<verb>.md#…`. Do not copy option descriptions into the guides.

- [ ] **Step 2: Run the tests.** Run `dotnet test BcpJson.sln`. Expected: all pass. The link and anchor test and `Every_documented_option_exists` now cover the guides.

- [ ] **Step 3: Commit.** Message: `Docs: six task guides`.

### Task 4: README and CONTRIBUTING.md

**Files:**
- Modify: `README.md` (rewrite)
- Create: `CONTRIBUTING.md`
- Modify: `tests/BcpJson.Tests/DocsTests.cs`

- [ ] **Step 1: Write the failing test**

```csharp
[Fact]
public void User_docs_link_only_to_shipped_files()
{
    // For README.md and docs/guides/*.md, docs/reference/*.md: every RelativeLinks target, resolved to a full path,
    // must be LICENSE, NOTICE, README.md, or a file under docs/guides or docs/reference. Report every offender.
}
```

- [ ] **Step 2: Run it and confirm it fails.** Run `dotnet test BcpJson.sln --filter User_docs_link_only_to_shipped_files`. Expected: FAIL. The old README links to `.github/workflows/release.yml` and `SECURITY.md`.

- [ ] **Step 3: Write the README.** It contains, in this order:
  - title and one-paragraph intro;
  - `## Why`;
  - `## Features` (one line each, linking to a guide or to `docs/reference/common.md`);
  - `## Requirements`;
  - `## Install`, with the package contents updated to LICENSE, NOTICE, this README and the `docs/guides` and `docs/reference` folders;
  - `## Quick start` (export, then import, about 5 lines);
  - `## I want to…` (one line per guide, phrased as the job);
  - `## Reference` (links to the reference pages);
  - `## Security`, linking to SECURITY.md by GitHub URL;
  - `## License`.

  Then write `CONTRIBUTING.md` from the old README's "Build from source", "Releasing" and "Project layout" sections, plus a short "Tests" section that points at `dotnet test` and `tests/integration/roundtrip.sh`, as described in CLAUDE.md.

- [ ] **Step 4: Run the tests and confirm they pass.** Run `dotnet test BcpJson.sln`. Expected: all pass.

- [ ] **Step 5: Commit.** Message: `Docs: short README with task index; CONTRIBUTING.md`.

### Task 5: Packaging, CLAUDE.md, ROADMAP

**Files:**
- Modify: `.github/workflows/release.yml` (Package step, around line 166)
- Modify: `CLAUDE.md`
- Modify: `docs/ROADMAP.md`

- [ ] **Step 1: Change the Package step.** In `release.yml`, replace `cp LICENSE NOTICE README.md "stage/$name/"` with copies that keep the paths: `mkdir -p "stage/$name/docs" && cp -r docs/guides docs/reference "stage/$name/docs/"`. Before zipping, fail the step when any of these is missing:
  - `stage/$name/README.md`
  - `stage/$name/docs/guides/restore-an-export.md`
  - `stage/$name/docs/reference/export.md`

  Fail with `echo "::error::..."; exit 1`.

- [ ] **Step 2: Update CLAUDE.md.**
  - Replace mentions of README sections with the new pages.
  - Add one line under Conventions: "User docs live in `docs/guides` (one page per job) and `docs/reference` (one page per verb, plus `common.md`, `manifest.md`, `json-format.md`, `limitations.md`). A new option needs a row on its verb's reference page; `DocsTests` enforces it, along with links and anchors."
  - Add `CONTRIBUTING.md` to the build section.

- [ ] **Step 3: Update ROADMAP.**
  - Point "Done" entries that name README sections at the new pages.
  - Add a "Done after v2.8.1" section with one line: the documentation reorganized into task guides and a per-verb reference, with link and option tests.

- [ ] **Step 4: Verify.** Run `dotnet test BcpJson.sln` (expected: all pass). Then push and open a draft PR. CI must be green: two builds and three round trips. Then run `gh workflow run release.yml --ref docs/reorganize`. If the workflow refuses to run outside master, note that in the report: the package check then runs at the next tag. If the dry run does run, list the contents of one artifact to show the docs are there.

- [ ] **Step 5: Commit.** Message: `Docs: ship guides and reference in packages; CLAUDE.md and roadmap`.
