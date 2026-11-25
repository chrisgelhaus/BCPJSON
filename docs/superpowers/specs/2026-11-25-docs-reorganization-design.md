# Documentation reorganization: task guides plus a per-verb reference

Status: draft for the owner's review.

## Why

The README is 708 lines and about 9,500 words, organized by feature. Its readers are DBAs who are running bcpJson and looking up how to do one particular job. To restore a split export today they have to put the steps together from Import, Table locking, Resilience and Splitting large tables. Splitting large tables alone is about 150 lines and covers export, import and copy together.

## Goals

- A DBA finds the job in a short "I want to…" list and follows one page from start to finish, with the commands in order.
- Every option of every verb is described on that verb's reference page: what it does and its default. Concepts that several verbs share (connections, filters, parallelism, exit codes, the manifest) are explained once, on their own reference page, and the option rows link there.
- The README fits on a couple of screens: what bcpJson is, install, a quick start and the list of guides.
- Contributor material lives in its own file.

## Non-goals

- New features or changed behaviour. The CLI and its help text stay as they are.
- A documentation website (MkDocs, GitHub Pages). Plain Markdown, read on GitHub and in the release packages.
- Rewriting the design specs, plans or ROADMAP, beyond fixing links into the README.

## Anti-goals

- **Lost content.** Every fact in today's README lands on some page. Removing material is allowed only where it is duplicated.
- **Repetition that drifts.** A guide explains a step and links to the reference for the option details. It does not copy option descriptions.
- **Broken links:** on GitHub, in the release packages, and from `SECURITY.md`, `CLAUDE.md` and `ROADMAP.md`.

## Approaches

1. **A page per verb, plus a page per shared topic.** This keeps close to the code, but a job like restoring a split export still spans four pages.
2. **Task guides backed by a per-verb reference. Chosen (2026-11-25).** It matches how DBAs search: by job. The cross-verb topics fall naturally into the guides that need them.
3. **Keep one README and trim it.** This is the least work, but the file stays long and organized by feature.

## Design

### 1. Files

```text
README.md                    what it is, why, features (short), requirements, install, quick start, "I want to…" list, links
CONTRIBUTING.md              build from source, tests, CI, releasing, project layout
docs/guides/
  back-up-a-database.md        export JSON/BCP, filters, a query to a file, --snapshot, --compress, the manifest, verify
  restore-an-export.md         import JSON/BCP, file and table mapping, --batch-size, .gz, table locks (default), the manifest
  copy-between-servers.md      copy, --drop-target / truncate, keys and indexes, check constraints, --foreign-keys, --triggers, --snapshot on the source
  large-tables.md              --split-tables for export, import and copy; how the ranges are chosen; tables that are not split; part files and the manifest; how parts load; --disable-indexes; --load-as-heap
  recover-a-failed-run.md      retries, .partial files, verify, export --resume, re-running an import, the load-as-heap recovery script and markers
  load-into-a-live-database.md --no-table-lock, its trade-offs, --snapshot on a busy source
docs/reference/
  export.md  import.md  copy.md  verify.md   every option of that verb (connection options included): name, short name, default, description; the verb's output files
  common.md                    shared concepts: connecting (Windows Authentication by default, SQL logins, --trust-server-certificate), table filters (include/exclude, `*` wildcards, no include filter = all user tables), parallelism (--max-tasks), retries, logging (--log-file), exit codes (1 if any error was reported)
  manifest.md                  bcpjson-manifest.json: fields, the split object, how verify, --resume and import use it
  json-format.md               the JSON export format (from today's "JSON format")
  limitations.md               known limitations (from today's "Known limitations")
```

The README's anchor `#install` stays, because `SECURITY.md` links to it.

### 2. Where today's README goes

| Today's section | New home |
|---|---|
| Why, Features, Requirements, Install | README (Features trimmed to one line each, linking to guides) |
| Build from source, Releasing, Project layout | CONTRIBUTING.md |
| Usage: Export | guides/back-up-a-database.md (examples); reference/export.md (options) |
| Usage: Import | guides/restore-an-export.md; reference/import.md |
| Usage: Copy (keys, FKs, check constraints, triggers) | guides/copy-between-servers.md; reference/copy.md |
| Table locking | the "how it works" text goes to load-into-a-live-database.md; restore-an-export.md and copy-between-servers.md say TABLOCK is the default and link there |
| Consistent point-in-time exports (--snapshot) | back-up-a-database.md (the main text); copy-between-servers.md and load-into-a-live-database.md link to it |
| Resilience: retries, complete files, manifest, verify, resume | recover-a-failed-run.md; the manifest's format goes to reference/manifest.md; retries to reference/common.md; verify's options to reference/verify.md |
| Compressed exports | back-up-a-database.md; reference/export.md for the options |
| Splitting large tables (all subsections) | large-tables.md; the load-as-heap recovery steps go to recover-a-failed-run.md, linked from large-tables.md; the manifest's split object to reference/manifest.md |
| Features bullets with no guide of their own (exit codes, Windows Authentication, streaming import, parallel tables, filters) | one line each in the README's Features; the details in reference/common.md |
| JSON format | reference/json-format.md |
| Known limitations | reference/limitations.md |
| Resources, Security, License | README (short) |

### 3. Page shape

- **Guides.** These open with one sentence saying when to use the guide. Then come numbered steps with commands, then "Variations" (short subsections for the common alternatives) and "If something goes wrong". Each guide ends with "See also", linking to the reference pages. Examples use the same placeholder server and database names across all guides.
- **Reference pages.** Each page has a table with these columns:
  - option (long and short name);
  - default;
  - what it does, in one or two sentences.

  Options are grouped (source, target, filters, loading, …), and rows link to `common.md` for the shared concepts. Options that are accepted but not implemented (`export -k`, `copy --source-query`) are listed with "not implemented" and also appear in `limitations.md`. The descriptions start from the `HelpText` in the options classes and are expanded where the help text is terse.
- **The README's "I want to…" list:** one line per guide, phrased as the job ("Back up a database to JSON or BCP files", "Restore an export into an existing database", …).

### 4. Keeping the reference in sync

A unit test (`tests/BcpJson.Tests/DocsTests.cs`) reads the `[Option]` attributes of `ExportOptions`, `ImportOptions`, `CopyOptions` and `VerifyOptions` by reflection (there is no shared base class). It asserts that every option appears in that verb's reference page, as `` `--name` ``, or as `` `-k` `` for an option with only a short name. A new option without documentation then fails the build. The test finds the docs folder by walking up from the test assembly to the repo root (the folder that holds `BcpJson.sln`).

Also:
- A second test checks every relative Markdown link in `README.md`, `CONTRIBUTING.md`, `docs/guides` and `docs/reference`:
  - the file exists;
  - an anchor matches a heading of the target file, using GitHub's slug rules (lower case, punctuation removed, spaces as hyphens, `-1` suffixes for repeats).
- Links inside the user docs (`README.md`, `docs/guides`, `docs/reference`) may point only to files that ship in the package (section 5). Links to `CONTRIBUTING.md`, `docs/ROADMAP.md`, `SECURITY.md` or the specs use absolute `https://github.com/chrisgelhaus/BCPJSON/blob/master/...` URLs, so nothing dangles offline. The link test enforces this.

### 5. Release packages

Each package holds `LICENSE`, `NOTICE`, `README.md`, `docs/guides/` and `docs/reference/`, at the same relative paths as in the repo, so the README's links work offline. `CONTRIBUTING.md` is not packaged. The Install section's sentence about the package contents is updated. The smoke test checks that `README.md`, `docs/guides/restore-an-export.md` and `docs/reference/export.md` are in each unpacked package.

### 6. Other files that point into the README

- `CLAUDE.md`: today it says things like "Documented in the README". Point it to the new pages, and add one line under Conventions: user docs live in `docs/guides` and `docs/reference`, and every new option goes into its reference page (the test enforces this).
- `docs/ROADMAP.md` "Done" entries that name README sections: update the references to the new pages. Historical entries otherwise stay as they are.
- The specs and plans under `docs/superpowers` are historical records and are not edited, even where they mention README sections.
- `SECURITY.md`: `README.md#install` keeps working.

## Tests

- `dotnet test`: `DocsTests` (options documented, relative links resolve).
- CI: the existing builds. The release dry run (manual `release.yml`) shows that the packages contain the docs.

## Left to the builder

- The exact wording, the order of the guides in the README, and how much of today's explanatory text is shortened while moving (nothing factual is lost).
- How many examples each guide carries, beyond those moved from today's README.

## Calls made in writing this

Not decided by the owner; correct any of them.

- Six guides, with the names and contents in section 1.
- Contributor material moves to `CONTRIBUTING.md`.
- A unit test enforces that every option is documented, and that relative links and their anchors resolve.
- Release packages carry `docs/guides` and `docs/reference`, but not `CONTRIBUTING.md`. User docs link to unpackaged files by absolute GitHub URL.
- No shared option table: each verb page lists all its options. Shared concepts go on `reference/common.md`, and the manifest gets its own page, `reference/manifest.md`.
- Options that are accepted but not implemented (`export -k`, `copy --source-query`) are listed as such, and also appear in limitations.
- The older specs and plans are not edited.
- The README keeps Why, a short Features list, Requirements, Install (same anchor) and a quick start.
