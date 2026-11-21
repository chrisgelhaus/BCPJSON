/*
    One line per index/key of every user table, describing everything copy must reproduce:
    name, type, uniqueness, PK/unique constraint, filter, key columns (with DESC), included columns, compression.
    Run with: bcpjson export --source-query "$(cat indexes.sql)" --file-format json, then diff two databases.
    Catalog columns use different collations, so every piece is converted with COLLATE DATABASE_DEFAULT.
*/
SET NOCOUNT ON;
SELECT s.name COLLATE DATABASE_DEFAULT + N'.' + t.name COLLATE DATABASE_DEFAULT
       -- Constraints declared without a name get a random system name (PK__Table__3214EC07...); a copy gets its own.
       + N' | ' + CASE WHEN EXISTS (SELECT 1 FROM sys.key_constraints kc
                                    WHERE kc.parent_object_id = i.object_id AND kc.unique_index_id = i.index_id AND kc.is_system_named = 1)
                       THEN N'<system-named>'
                       ELSE i.name COLLATE DATABASE_DEFAULT END
       + N' | ' + i.type_desc COLLATE DATABASE_DEFAULT
       + N' | unique=' + CAST(i.is_unique AS NVARCHAR(1))
       + N' pk=' + CAST(i.is_primary_key AS NVARCHAR(1))
       + N' uq=' + CAST(i.is_unique_constraint AS NVARCHAR(1))
       + N' | filter=' + ISNULL(i.filter_definition COLLATE DATABASE_DEFAULT, N'')
       + N' | keys=' + ISNULL(STUFF((
             SELECT N', ' + c.name COLLATE DATABASE_DEFAULT + CASE WHEN ic.is_descending_key = 1 THEN N' DESC' ELSE N'' END
             FROM sys.index_columns ic
                  JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
             WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 0
             ORDER BY ic.key_ordinal, c.name
             FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, N''), N'')
       + N' | include=' + ISNULL(STUFF((
             SELECT N', ' + c.name COLLATE DATABASE_DEFAULT
             FROM sys.index_columns ic
                  JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
             WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 1
             ORDER BY c.name
             FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, N''), N'')
       + N' | compression=' + ISNULL((
             SELECT TOP (1) p.data_compression_desc COLLATE DATABASE_DEFAULT
             FROM sys.partitions p
             WHERE p.object_id = i.object_id AND p.index_id = i.index_id), N'-') AS [index]
FROM sys.indexes i
     JOIN sys.tables t ON t.object_id = i.object_id
     JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE i.type > 0
      AND t.is_ms_shipped = 0
ORDER BY [index];
