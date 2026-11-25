# Load into a live database

Use this guide when the tables you load into are in use while the load runs, or when you read from a busy production database.

By default `import` and `copy` take a table lock (`TABLOCK`) on every target table, which is the fastest way to load. `--no-table-lock` turns it off.

## Steps

1. **Decide whether the target can be locked.** If nothing reads or writes the table during the load, keep the default. If an application must keep working with it, use `--no-table-lock`.
2. **Load without the table lock:**

   ```powershell
   # Append a feed into a table that applications are reading and writing right now
   bcpjson import --file-format bcp --source-path D:\exports\Sales --source-file Sales.Orders.bcp `
                  --target-server sql02 --target-database Sales --no-table-lock
   ```

3. **Read the source consistently.** If the source is busy, use `--snapshot` on `export` or `copy`, so every table reflects the same moment without blocking the source. See [Point-in-time exports](back-up-a-database.md#point-in-time-exports-with---snapshot).

## How table locking works

Every load asks SQL Server for a table lock on the target table:
- `SqlBulkCopyOptions.TableLock` for JSON import and copy;
- `bcp ... -h "TABLOCK"` for BCP import.

Taking one table-level lock instead of thousands of row and page locks is the fastest way to bulk load:
- there is far less lock management, and no lock escalation part-way through the load;
- under the **SIMPLE** or **BULK_LOGGED** recovery model the load can be **minimally logged** (for example into a heap or an empty table), which cuts transaction-log writes dramatically. Under FULL recovery every row is still logged, but the locking savings remain.

The lock is held while each batch is written: 50,000 rows by default for import (`--batch-size`), 10,000 for copy. So for practical purposes other sessions cannot use the table until the load finishes. On a heap the lock is a bulk-update (BU) lock. On a table with a clustered index it is an exclusive table lock.

## When to keep the default (table lock on)

- Restoring an export into a new or scratch database, or into tables you have just truncated.
- Nightly or batch loads into staging tables that nothing else reads during the load.
- Any load where speed and transaction-log volume matter more than concurrent access.

## When to use `--no-table-lock`

- **The target table is live.** With the lock, readers block until each batch commits, unless they use snapshot isolation, RCSI or `NOLOCK`, and writers block too. Use row locking when an application must keep working with the table during the load.
- **Several files load into the same table with a clustered index.** With table locks those loads run one at a time; without them they can run in parallel:

  ```powershell
  bcpjson import --file-format json --source-path D:\exports\Sales --source-file "orders_*.json" `
                 --target-server sql02 --target-database Sales --target-table dbo.Orders --no-table-lock
  ```

  Heaps are the exception: BU locks are compatible with each other, so parallel table-locked loads into a heap work well, and you can keep the default. For the parts of a split table, bcpJson makes this choice itself; see [Import the parts](large-tables.md#import-the-parts).
- **Lock-sensitive environments,** for example a busy server where an exclusive table lock would trip blocking alerts.

Without the table lock:
- the load is fully logged;
- it uses row and page locks, which SQL Server may still escalate to a table lock on very large loads;
- it is noticeably slower on big tables.

`--load-as-heap` needs the table lock, so with `--no-table-lock` it falls back to `--disable-indexes`.

## Variations

### Refresh a database that stays online during a copy

```powershell
bcpjson copy --source-server sql01 --source-database Sales `
             --target-server sql02 --target-database Sales --truncate-target --no-table-lock
```

`--snapshot` affects only the source. The target is written normally, and is table-locked unless you pass `--no-table-lock`.

### Reading a busy source

- `--snapshot` reads a consistent copy without blocking writers.
- `--nolock` (export and copy) also avoids blocking, but can read uncommitted, inconsistent data. Don't rely on it for backups. It is unnecessary with `--snapshot`.

## If something goes wrong

- **Blocking alerts or timeouts** in applications during a load: rerun with `--no-table-lock`.
- **Parallel loads into one clustered table run one at a time:** that is the table lock; see [Import the parts](large-tables.md#import-the-parts).

## See also

- [import loading options](../reference/import.md#loading-options)
- [copy options](../reference/copy.md)
