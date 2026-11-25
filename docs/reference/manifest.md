# The export manifest

Every export writes `bcpjson-manifest.json` into the export folder. It records what was exported, so the folder can be checked with `verify`, finished with `export --resume`, and loaded safely by `import`.

## Fields

The manifest records:
- the tool version and file format;
- the server and the database (the real database, also with `--snapshot`);
- the query (for `--source-query` exports) and whether a snapshot was used;
- the start and completion times;
- for every table: its row count, when it was exported, and the size and SHA-256 of each of its files.

```json
{
  "tool": "bcpjson 2.4.0+...",
  "format": "json",
  "server": "sql01",
  "database": "Sales",
  "query": null,
  "snapshot": true,
  "started": "2026-11-21T02:00:00Z",
  "completed": "2026-11-21T02:41:13Z",
  "tables": [
    {
      "name": "Sales.Customer",
      "rows": 19820,
      "exported": "2026-11-21T02:00:04Z",
      "files": [ { "name": "Sales.Customer.json", "bytes": 4120334, "sha256": "9f2c..." } ],
      "orderedBy": [ { "column": "CustomerID", "type": "int", "descending": false, "collation": null } ]
    }
  ]
}
```

- The manifest is saved after every table, so it always reflects what has finished.
- `completed` is set only when the whole export succeeded.
- With `--compress gzip` it also records `"compression": "gzip"` and the size and SHA-256 of the `.gz` files. A missing `compression` means none.

## orderedBy and the order hint

`orderedBy` appears on JSON exports of tables that have a primary key. It is the key the rows are sorted by.

JSON import reads it. When the target table's clustered index has exactly that key (the same columns, types, directions and collations), import tells SQL Server that the rows already arrive in clustered index order, so the load needs no sort. That makes large loads into an empty clustered table noticeably faster. The console says `Rows are in clustered index order; loading with an order hint.` when it applies.

SQL Server checks the order and would fail the load if it were wrong. So for any other target, including a JSON file without a manifest, the import runs without the hint. BCP exports are unordered, so BCP imports never get a hint.

## The split object

A split table stays one entry. That entry lists all its part files, plus a `split` object recording how the table was cut:

```json
{
  "name": "dbo.OrderHistory",
  "rows": 120000000,
  "files": [ { "name": "dbo.OrderHistory.part0001.json", "bytes": 912345678, "sha256": "4be1..." }, "... one per part ..." ],
  "split": {
    "kind": "histogram",
    "column": "OrderHistoryID",
    "type": "bigint",
    "descending": false,
    "nullable": false,
    "partCount": 16,
    "boundaries": [ "7500000", "15000000", "... 15 values in all ..." ],
    "parts": [
      { "part": 1, "rows": 7500000, "files": [ "dbo.OrderHistory.part0001.json" ],
        "lower": null, "lowerInclusive": false, "upper": "7500000", "upperInclusive": false, "includeNulls": false },
      "... one per part ..."
    ]
  }
}
```

- `kind` is `histogram` or `partition`. A `partition` split also records `partitionFunction` and `rangeRight`.
- `boundaries` are stored as the text SQL Server converts the key to.

## How verify uses it

`verify` checks every file the manifest lists, part files included, and the `completed` flag. See [verify](verify.md).

## How --resume uses it

`export --resume`:
- skips tables the manifest records whose files still have the recorded size;
- for a split table, keeps the recorded split and re-exports only the missing or damaged parts.

The run must use the same `--file-format`, `--compress`, `--source-database` and `--source-query` as the manifest, or it stops with an error. See [Resume an interrupted export](../guides/recover-a-failed-run.md#resume-an-interrupted-export).

## How import uses the manifest

The manifest itself is never loaded as data, so `--source-file "*.json"` still works. When the source folder has a manifest:

- **Known tables:** for a table the manifest knows, import loads only the files the manifest lists for it, and skips the others: `<table>: skipping <file>: not part of the export in <manifest>.`
- **Unfinished split exports:** a split table whose export is unfinished is refused: `<table>: the export is incomplete (<k> of <n> parts); finish it with export --resume.`
- **Unknown tables:** files of tables the manifest does not mention load as usual.

With or without a manifest, a table that has both a whole-table file and part files is refused, and nothing is loaded into it: `<table>: both <file> and part files are present; remove the files of the older export.`
