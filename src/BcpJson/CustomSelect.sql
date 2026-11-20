/* Reference copy of Program.TableMetadataSql (Program.Export.cs), used by JSON export.
   Not loaded at runtime; keep the two in sync. @objectId = sys.tables.object_id */
SET NOCOUNT ON;
DECLARE @Columns NVARCHAR(MAX), @SortOrder NVARCHAR(MAX);

SELECT   @Columns = COALESCE(@Columns + N', ', N'') +
                    CASE WHEN sc.system_type_id = 240
                         THEN QUOTENAME(sc.name) + N'.ToString() AS ' + QUOTENAME(sc.name)
                         ELSE QUOTENAME(sc.name)
                    END
FROM     sys.columns sc
WHERE    sc.object_id = @objectId
ORDER BY sc.column_id;

IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = @objectId AND is_primary_key = 1)
BEGIN
    SELECT   @SortOrder = COALESCE(@SortOrder + N', ', N'') + QUOTENAME(c.name) +
                          CASE ic.is_descending_key WHEN 1 THEN N' DESC' ELSE N' ASC' END
    FROM     sys.indexes i
             INNER JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
             INNER JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
    WHERE    i.object_id = @objectId
             AND i.is_primary_key = 1
             AND ic.key_ordinal > 0
    ORDER BY ic.key_ordinal;
END
ELSE
BEGIN
    /* No primary key: sort by every column that can appear in ORDER BY (not text, ntext, image, xml or CLR types). */
    SELECT   @SortOrder = COALESCE(@SortOrder + N', ', N'') + QUOTENAME(c.name)
    FROM     sys.columns c
    WHERE    c.object_id = @objectId
             AND c.system_type_id NOT IN (34, 35, 99, 240, 241)
    ORDER BY c.column_id;
END;

SELECT @Columns, @SortOrder;
