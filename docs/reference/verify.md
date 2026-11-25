# verify

Checks an export folder against its `bcpjson-manifest.json`. It needs no database.

```powershell
bcpjson verify --path D:\exports\Sales
```

How-to: [Recover a failed run](../guides/recover-a-failed-run.md#verify-an-export), [Back up a database](../guides/back-up-a-database.md).

## Options

| Option | Default | Description |
|---|---|---|
| `--path` | current directory | The export folder to check. |
| `--log-file` | | Also append every console message to this file. See [Logging](common.md#logging). |

## What it checks

- Every file the manifest lists exists, with its recorded size and SHA-256.
- The export completed: the manifest's `completed` is set.
- For a split table, it reports `<table>: <k> of <n> parts exported.` when parts are missing.

**Errors (exit code 1):**
- a missing file;
- a changed size or hash, from corruption, an accidental edit or an incomplete copy to another disk;
- an export that never completed.

**Warnings only:**
- extra `.json` or `.bcp` files the manifest does not list, such as tables left from an older export;
- leftover `.partial` files.

Run it after copying an export to archive storage, and before restoring from it. Compressed exports are checked the same way, against the `.gz` files.

<!-- CI docs-only test; this PR is closed without merging. -->
