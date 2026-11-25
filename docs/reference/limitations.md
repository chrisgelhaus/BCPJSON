# Known limitations

## Connections and BCP files

- **Passwords on the command line.** bcp accepts SQL passwords only through `-P`, so they are visible in the process list while bcp runs. Prefer Windows Authentication.
- **BCP character format uses `|` as the field terminator.** Data that contains `|` or line breaks will not round-trip through BCP files. Use JSON for such tables.
- **BCP exports (`bcp out`) are not ordered,** so BCP files are not reliably diff-able. JSON files are.
- **BCP import of tables with awkward names and certain indexes fails.** The failing combination is a schema or table name containing `.` or `"`, together with a filtered, XML or spatial index. bcp cannot set `QUOTED_IDENTIFIER ON` for such names. Use JSON import for that table.

## Data types and constraints

- `sql_variant` values are re-imported as `nvarchar`.
- `import` bulk loads without checking constraints (the bcp and SqlBulkCopy default, which is what makes it fast). Foreign keys and check constraints on the target are not validated during the load, and SQL Server marks them as not trusted. Run `ALTER TABLE ... WITH CHECK CHECK CONSTRAINT ALL` afterwards if you need them trusted.

## Options that are not implemented

- `export -k` is reserved. JSON exports always write NULLs.
- `copy --source-query` is not supported. It is accepted, ignored and warned about.

## copy

- `copy` recreates keys, indexes and check constraints, and foreign keys and triggers on request. It does not copy other objects, such as views, procedures, functions or DDL triggers.
- Indexes are placed on the target's default filegroup.
- A partitioned source table is created on the target's partition scheme of the same name if there is one, and unpartitioned otherwise. That scheme's function must take the partitioning column's type, or creating the table fails.

## Split tables

- A key where one value holds most of the rows cannot be cut inside that value. Text keys may give fewer parts than asked.
- BCP exports are unordered, so BCP imports give SQL Server no `ORDER` hint.
- `--batch-size 0` is one transaction per part, not per table.
- A real table whose name ends in `.part` and four digits (for example `dbo.x.part0001`) is read as a part on import.
- Tables with a sparse column set are never split. Loading such a table through JSON import or copy is not tested.

## --load-as-heap

It applies only to split tables loaded into empty targets. It does not apply to:
- tables referenced by foreign keys;
- tables with XML, spatial, disabled or full-text indexes;
- tables referenced by indexed views;
- temporal tables;
- tables with change tracking, replication or change data capture;
- tables with FILESTREAM data;
- memory-optimized tables;
- tables with a clustered columnstore index.

See [When a table falls back](../guides/large-tables.md#when-a-table-falls-back).
