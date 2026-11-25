# import

Loads JSON or BCP files written by `export` into tables that already exist.

```powershell
bcpjson import --file-format json --source-path D:\exports\Sales --source-file "*.json" --target-server sql02 --target-database Sales --truncate-target
```

How-to: [Restore an export](../guides/restore-an-export.md), [Large tables](../guides/large-tables.md), [Load into a live database](../guides/load-into-a-live-database.md).

## Files

| Option | Default | Description |
|---|---|---|
| `--file-format` | | `json` or `bcp`. Required. |
| `--source-path` | current directory | Folder that holds the files. |
| `--source-file` | | File name or wildcard pattern, for example `"*.json"` or `"*.json.gz"`. Required. The manifest, `.partial` files and `.load-as-heap.sql` recovery scripts are never loaded as data. |

## Target

| Option | Default | Description |
|---|---|---|
| `--target-server` | `localhost` | SQL Server instance to load into. |
| `--target-database` | | Database to load into. |
| `--target-login`, `--target-password` | empty | SQL login. Leave both out for Windows Authentication. See [Connecting](common.md#connecting). |
| `--trust-server-certificate` | off | Accept the server's TLS certificate without validation. |
| `--target-table` | the file name | Load into this `schema.table` instead of the table named by the file (the file name without extension, `.gz` and `.partNNNN`). |

## Loading options

| Option | Default | Description |
|---|---|---|
| `--truncate-target` | off | Truncate each table before its first file or part loads. |
| `--no-table-lock` | off (tables are locked) | Load without a table lock (`TABLOCK`). The lock is the fastest way to load and allows minimal logging; turn it off when other sessions must use the table during the load. See [Load into a live database](../guides/load-into-a-live-database.md). |
| `--batch-size` | `50000` | Rows per committed batch. A failure rolls back only the current batch; batches already committed stay. `0` loads each file (each part file of a split table) in one transaction. |
| `--disable-indexes` | off | Disable nonclustered indexes, except the primary key, the clustered index and indexes a foreign key references, before a table's first file, and rebuild them after its last, also after a failed load. See [Load first, index afterwards](../guides/large-tables.md#load-first-index-afterwards---disable-indexes). |
| `--load-as-heap` | off | For split tables loaded into an empty table with a clustered index: drop every index, load the parts in parallel into the heap, then recreate the indexes. Other tables load as with `--disable-indexes`. See [Load as a heap](../guides/large-tables.md#load-as-a-heap---load-as-heap). |

## Running

| Option | Default | Description |
|---|---|---|
| `--max-tasks` | unlimited (`-1`; `0` also means unlimited) | Most files, or parts, imported at once. See [Parallelism](common.md#parallelism). |
| `--retries` | `3` | Retries when opening a connection fails transiently. Loads themselves are not retried. `0` turns retries off. See [Retries](common.md#retries). |
| `--log-file` | | Also append every console message to this file. See [Logging](common.md#logging). |

## What import does with the data

- **Preserved values:** identity values and NULLs are kept. Computed and `rowversion` columns are skipped, and the server recalculates them.
- **Constraints:** the bulk load does not check constraints (the bcp and SqlBulkCopy default, which is what makes it fast). Foreign keys and check constraints on the target become untrusted. Run `ALTER TABLE ... WITH CHECK CHECK CONSTRAINT ALL` afterwards if you need them trusted.
- **Compressed files:** `.json.gz` and `.bcp.gz` are recognised by their extension, and the table name comes from the name without `.gz`.
  - JSON files, compressed or not, are streamed: each row goes straight into the bulk load, so memory use does not grow with the table.
  - BCP files are decompressed to the temp folder first, because `bcp` reads only plain files. That needs free space for the largest uncompressed file.
- **BCP format file:** a BCP import uses the export's `<schema>.<table>-c.fmt` (or an older `-c.xml`).
- **Order hint:** when the folder has a manifest whose `orderedBy` matches the target's clustered key exactly, JSON import loads with an order hint (`Rows are in clustered index order; loading with an order hint.`). See [The export manifest](manifest.md#orderedby-and-the-order-hint).
- **Split exports:** part files are grouped by table, and a manifest in the folder decides which files load. See [How import uses the manifest](manifest.md#how-import-uses-the-manifest) and [Large tables](../guides/large-tables.md).
