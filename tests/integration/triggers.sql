/*
    One line per DML trigger: table, name, AFTER / INSTEAD OF, events with FIRST/LAST order, disabled state and
    the full definition.
*/
SET NOCOUNT ON;
SELECT s.name COLLATE DATABASE_DEFAULT + N'.' + t.name COLLATE DATABASE_DEFAULT
       + N' | ' + tr.name COLLATE DATABASE_DEFAULT
       + N' | ' + CASE WHEN tr.is_instead_of_trigger = 1 THEN N'INSTEAD OF' ELSE N'AFTER' END
       + N' | events=' + ISNULL(STUFF((
             SELECT N', ' + te.type_desc COLLATE DATABASE_DEFAULT
                    + CASE WHEN te.is_first = 1 THEN N' FIRST' WHEN te.is_last = 1 THEN N' LAST' ELSE N'' END
             FROM sys.trigger_events te
             WHERE te.object_id = tr.object_id
             ORDER BY te.type_desc
             FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, N''), N'')
       + N' | disabled=' + CAST(tr.is_disabled AS NVARCHAR(1))
       -- SMO regenerates the CREATE TRIGGER header with bracketed names (the body is copied verbatim), so brackets
       -- are ignored when comparing definitions.
       + N' | definition=' + REPLACE(REPLACE(OBJECT_DEFINITION(tr.object_id), N'[', N''), N']', N'') COLLATE DATABASE_DEFAULT AS [trigger]
FROM sys.triggers tr
     JOIN sys.tables t ON t.object_id = tr.parent_id
     JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE tr.parent_class = 1
ORDER BY [trigger];
