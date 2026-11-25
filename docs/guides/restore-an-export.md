# Restore an export

Use this guide to load an export folder back into a database, for example to prove a backup or to fill a test system.

## Steps

1. **Create the tables.** `import` loads into tables that already exist. Create the schema first, for example from a script of the source database. Keys and indexes can be in place; see [Large tables](large-tables.md) for loading big tables into indexed targets quickly.

2. **Check the export** if it has been copied or archived:

   ```powershell
   bcpjson verify --path D:\exports\Sales
   ```

3. **Load every file.** The table name comes from each file name:

   ```powershell
   bcpjson import --file-format json --source-path D:\exports\Sales --source-file "*.json" `
                  --target-server sql02 --target-database Sales --truncate-target
   ```

   `--truncate-target` empties each table before its first file loads, so the restore can be repeated. By default every table is loaded under a table lock, the fastest way to load. See [Load into a live database](load-into-a-live-database.md) for when to turn the lock off.

4. **Check the exit code.** It is `0` when every file loaded, and `1` if anything failed. The console names each failure.

## Variations

### BCP files

```powershell
bcpjson import --file-format bcp --source-path D:\exports\Sales --source-file "*.bcp" `
               --target-server sql02 --target-database Sales --truncate-target
```

Each `.bcp` file needs its `<schema>.<table>-c.fmt` format file from the export.

### One file into another table

```powershell
bcpjson import --file-format bcp --source-path D:\exports\Sales --source-file Sales.Customer.bcp `
               --target-server sql02 --target-database Scratch --target-table dbo.CustomerCopy
```

### A compressed export

```powershell
bcpjson import --file-format json --source-path D:\exports\Sales --source-file "*.json.gz" `
               --target-server sql02 --target-database Sales --batch-size 0
```

Use a pattern that matches the `.gz` files. JSON is streamed straight from the compressed file. BCP files are decompressed to the temp folder first, which needs free space for the largest uncompressed file.

### Batch size

Each batch (`--batch-size`, default 50,000 rows) is committed on its own.
- **Larger batches** load a little faster and commit less often.
- **`--batch-size 0`** gives all or nothing per file. The cost is one long transaction, which needs log space for the whole file unless the load is minimally logged.

### Split exports

Exports with part files (`<table>.part0001.json`, …) load with the same command. See [Large tables](large-tables.md#import-the-parts) for how the parts load, and for `--disable-indexes` and `--load-as-heap`.

## If something goes wrong

- **A load failed partway:** the batches already committed stay in the table. Fix the cause and rerun with `--truncate-target`. Loads are not retried automatically, because replaying one could duplicate rows.
- **Import refuses or skips files:** with a manifest in the folder, import loads only what the export recorded. See [How import uses the manifest](../reference/manifest.md#how-import-uses-the-manifest).
- **Constraints are untrusted afterwards:** the bulk load does not check them. Run `ALTER TABLE ... WITH CHECK CHECK CONSTRAINT ALL` if you need them trusted.
- **An earlier `--load-as-heap` run left a table without its indexes:** see [The load-as-heap recovery script](recover-a-failed-run.md#the-load-as-heap-recovery-script).

## See also

- [import options](../reference/import.md)
- [Load into a live database](load-into-a-live-database.md)
- [Large tables](large-tables.md)
