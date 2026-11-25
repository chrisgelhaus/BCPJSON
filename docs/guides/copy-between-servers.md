# Copy between servers

Use this guide to copy tables straight from one database to another, for example to refresh a test database from production, with no files in between.

## Steps

1. **Create the target database.** `copy` creates missing tables but not the database.

2. **Copy.** This command makes a consistent copy of a whole database, with its relationships:

   ```powershell
   bcpjson copy --source-server sql01 --source-database Sales `
                --target-server sql02 --target-database Sales --drop-target --snapshot --foreign-keys
   ```

   - **Tables:** missing target tables are created from the source definition: columns, nullability, identity, defaults and computed columns. After the data is loaded, copy creates the primary key, unique constraints, indexes and check constraints. `--foreign-keys` adds the foreign keys once every table is loaded.
   - **`--drop-target`** recreates tables that already exist.
   - **`--snapshot`** reads every table from the same moment. See [Point-in-time exports](back-up-a-database.md#point-in-time-exports-with---snapshot).

3. **Check the exit code.** It is `0` on success, and `1` if any table, index, constraint or key failed. The console names each failure, and the remaining objects are still created.

## Variations

### Only some tables, into existing tables

```powershell
bcpjson copy --source-server sql01 --source-database Sales --target-server sql02 --target-database Sales `
             --include-schemas dbo --truncate-target --max-tasks 4
```

Tables that already exist keep their own indexes, constraints and triggers. copy does not add, change or remove them.

### Keys and indexes

**Default:** on. **Option:** `--no-indexes` turns it off.

When copy creates a target table, it recreates every primary key, unique constraint and index of the source table. The table counts as created when it did not exist, or when `--drop-target` dropped it. The recreated objects include:
- clustered and nonclustered indexes;
- unique and filtered indexes;
- included columns and descending keys;
- data compression;
- columnstore, XML and spatial indexes.

They are built **after the data is loaded**. Bulk loading into a table without indexes (with the default table lock) is the fastest way to load, and building each index once at the end is much cheaper than maintaining it row by row. The clustered index is built first, then the others in dependency order.

- **Filegroups:** indexes are created on the target database's default filegroup. Filegroup placement is not copied, because the target may not have the same filegroups.
- **Partitioned tables:** a partitioned source table is created on the target's partition scheme of the same name, when the target has one. That scheme's partition function must take the type of the table's partitioning column, or creating the table fails. Otherwise the table is created unpartitioned.
- **Names:** index and constraint names are kept, except for constraints declared without a name (such as `PRIMARY KEY` on a column). SQL Server named those itself (`PK__Orders__3214EC07…`), and the copy gets its own system-generated name, just as running the original `CREATE TABLE` would.
- **Failures:** an index that cannot be created is reported as an error (exit code 1), and the remaining indexes are still created.

Use `--no-indexes` when:
- the target is a staging or scratch database and you will create your own indexes, or none;
- you want the copy to finish as quickly as possible and don't need to query the result efficiently;
- the target has little free space: building large indexes needs temporary space in the data files and `tempdb`.

### Check constraints

**Default:** on. **Option:** `--no-check-constraints` turns it off.

On every table it creates, copy recreates the source's check constraints right after the table's indexes.

- **Trust:** a check constraint involves only its own table, and the copied rows already satisfied it in the source. So trusted constraints are created `WITH CHECK`, which validates them and keeps them trusted in the target. Disabled or untrusted constraints keep that state.
- **Names** are kept, except that constraints declared without a name get a new system name.
- **Failures:** a constraint that cannot be created is reported as an error, and the others are still created. The typical cause is a constraint that calls a user-defined function that does not exist in the target.

Use `--no-check-constraints` when the target is a staging area that must accept rows the source would reject, or to save the validation scan on very large tables.

### Foreign keys

**Default:** off. **Option:** `--foreign-keys` turns it on.

With `--foreign-keys`, copy recreates the foreign keys of the tables it created. It does this after **every** table has been copied and indexed. Supported keys include:
- multi-column keys;
- keys that reference a unique constraint rather than the primary key;
- self-references and cross-schema references;
- their `ON DELETE` and `ON UPDATE` actions.

Keys that are trusted in the source are created `WITH CHECK`, so SQL Server validates the copied rows and the key is trusted in the target too. Keys that are disabled or untrusted in the source are created the same way in the target. Keys are created one at a time. One that fails is reported as an error, and the others are still created.

Why it is off by default: a foreign key involves two tables, so it can fail in situations that don't affect a single table.
- **Partial copies.** With table filters (`--include-tables`, `--exclude-schemas` and the others), the table a key references may not be part of the copy. The key can be created only if that table already exists in the target with the referenced key.
- **Copies of a changing database.** Without `--snapshot`, a child row can be copied without the parent row it points to, and the `WITH CHECK` validation fails. Use `--snapshot` with `--foreign-keys`; bcpJson warns if you don't.
- **Validation time.** Checking a large table against its parent takes time. Skip foreign keys for scratch copies that don't need them.

`--foreign-keys` cannot be combined with `--no-indexes`, because foreign keys need the referenced primary keys and unique constraints.

**Re-running a copy.** With `--drop-target`, copy first removes the foreign keys defined on the tables it is about to drop and recreate (they would be dropped with their tables anyway). So a database previously copied with `--foreign-keys` can be refreshed. A foreign key on a table *outside* the copy that references a copied table is left alone. If it prevents a table from being dropped, that table is reported as an error.

### Triggers

**Default:** off. **Option:** `--triggers` turns it on.

With `--triggers`, copy recreates the DML triggers (`AFTER` and `INSTEAD OF`) of the tables it created. It does this **after all tables are loaded**, so the triggers never fire on the copied rows.
- **Body:** each trigger's body is copied as is. The `CREATE TRIGGER ... ON ...` header is regenerated with bracketed names, which also keeps it correct for triggers renamed with `sp_rename`.
- **State:** disabled triggers stay disabled.
- **Order:** the `FIRST` / `LAST` firing order set with `sp_settriggerorder` is kept.

```powershell
# Every schema object bcpJson handles: keys, indexes, check constraints, foreign keys and triggers
bcpjson copy --source-server sql01 --source-database Sales --target-server sql02 --target-database Sales `
             --drop-target --snapshot --foreign-keys --triggers
```

Why it is off by default:
- **Triggers run on every later write.** In a copy, an audit trigger keeps writing audit rows. A trigger that sends mail, calls a procedure or writes to another database keeps doing so. That is often not wanted in a test or reporting copy.
- **Triggers reference other objects.** The procedures, functions, tables or databases a trigger uses must exist in the target, or creating (or later firing) the trigger fails.
- **Some triggers can't be recreated.** Encrypted triggers (`WITH ENCRYPTION`) cannot be scripted and are reported as errors. CLR triggers need their assembly in the target.

Each trigger that cannot be created is reported as an error, and the others are still created. Database-level (DDL) triggers are not copied.

### A target that stays online

Copy without the table lock so applications keep using the target tables: see [Load into a live database](load-into-a-live-database.md).

### Very large tables

`--split-tables` copies big tables as parallel key-range streams: see [Large tables](large-tables.md#copy).

## If something goes wrong

- **A table failed to copy:** copies are not retried, because some batches may already be committed. Rerun with `--drop-target` or `--truncate-target`.
- **A foreign key fails its `WITH CHECK` validation:** the source changed during the copy. Rerun with `--snapshot`.
- **Tables with geography, geometry or hierarchyid columns** are moved through `bcp` and need it on `PATH`.

## See also

- [copy options](../reference/copy.md)
- [Known limitations](../reference/limitations.md#copy)
