/*
    One line per check constraint: table, name (system-generated names compare as <system-named>), definition,
    disabled and trusted state.
*/
SET NOCOUNT ON;
SELECT s.name COLLATE DATABASE_DEFAULT + N'.' + t.name COLLATE DATABASE_DEFAULT
       + N' | ' + CASE WHEN cc.is_system_named = 1 THEN N'<system-named>' ELSE cc.name COLLATE DATABASE_DEFAULT END
       + N' | ' + cc.definition COLLATE DATABASE_DEFAULT
       + N' | disabled=' + CAST(cc.is_disabled AS NVARCHAR(1))
       + N' trusted=' + CAST(1 - cc.is_not_trusted AS NVARCHAR(1)) AS [check_constraint]
FROM sys.check_constraints cc
     JOIN sys.tables t ON t.object_id = cc.parent_object_id
     JOIN sys.schemas s ON s.schema_id = t.schema_id
ORDER BY [check_constraint];
