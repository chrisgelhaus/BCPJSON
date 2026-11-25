/*
    One line per index of the "split" schema, ordered by table and index name, for import --load-as-heap: every
    property it must bring back after dropping and recreating the indexes. Fields are separated by " | ":
    table, index, type, unique, primary key, unique constraint, filter, data space (filegroup or partition scheme),
    compression (MIN over partitions), key columns in key order with ASC/DESC, included columns, disabled.
    Run with: sqlcmd -h -1 -W -i heap_indexes.sql; a before/after comparison is a diff of the output.
    Catalog columns use different collations, so every piece is converted with COLLATE DATABASE_DEFAULT.
*/
SET NOCOUNT ON;
SELECT CAST(s.name COLLATE DATABASE_DEFAULT + N'.' + t.name COLLATE DATABASE_DEFAULT
       + N' | ' + i.name COLLATE DATABASE_DEFAULT
       + N' | ' + i.type_desc COLLATE DATABASE_DEFAULT
       + N' | unique=' + CAST(i.is_unique AS NVARCHAR(1))
       + N' pk=' + CAST(i.is_primary_key AS NVARCHAR(1))
       + N' uq=' + CAST(i.is_unique_constraint AS NVARCHAR(1))
       + N' | filter=' + ISNULL(i.filter_definition COLLATE DATABASE_DEFAULT, N'')
       + N' | space=' + ISNULL(ds.name COLLATE DATABASE_DEFAULT, N'-')
       + N' | compression=' + ISNULL((
             SELECT MIN(p.data_compression_desc COLLATE DATABASE_DEFAULT)
             FROM sys.partitions p
             WHERE p.object_id = i.object_id AND p.index_id = i.index_id), N'-')
       + N' | keys=' + ISNULL(STUFF((
             SELECT N', ' + c.name COLLATE DATABASE_DEFAULT + CASE WHEN ic.is_descending_key = 1 THEN N' DESC' ELSE N' ASC' END
             FROM sys.index_columns ic
                  JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
             WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.key_ordinal > 0
             ORDER BY ic.key_ordinal
             FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, N''), N'')
       + N' | include=' + ISNULL(STUFF((
             SELECT N', ' + c.name COLLATE DATABASE_DEFAULT
             FROM sys.index_columns ic
                  JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
             WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 1
             ORDER BY c.name
             FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, N''), N'')
       + N' | disabled=' + CAST(i.is_disabled AS NVARCHAR(1)) AS NVARCHAR(2000)) AS [index]
FROM sys.indexes i
     JOIN sys.tables t ON t.object_id = i.object_id
     JOIN sys.schemas s ON s.schema_id = t.schema_id
     LEFT JOIN sys.data_spaces ds ON ds.data_space_id = i.data_space_id
WHERE s.name = N'split' AND i.index_id > 0
ORDER BY s.name, t.name, i.name;
