/*
    One line per foreign key of a database, describing everything copy --foreign-keys must reproduce:
    owning table, name (system-generated names compare as <system-named>), referenced table, column pairs,
    ON DELETE / ON UPDATE actions, and whether the key is disabled or trusted.
*/
SET NOCOUNT ON;
SELECT ps.name COLLATE DATABASE_DEFAULT + N'.' + pt.name COLLATE DATABASE_DEFAULT
       + N' | ' + CASE WHEN fk.is_system_named = 1 THEN N'<system-named>' ELSE fk.name COLLATE DATABASE_DEFAULT END
       + N' | -> ' + rs.name COLLATE DATABASE_DEFAULT + N'.' + rtab.name COLLATE DATABASE_DEFAULT
       + N' | ' + ISNULL(STUFF((
             SELECT N', ' + pc.name COLLATE DATABASE_DEFAULT + N'=' + rc.name COLLATE DATABASE_DEFAULT
             FROM sys.foreign_key_columns fkc
                  JOIN sys.columns pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
                  JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
             WHERE fkc.constraint_object_id = fk.object_id
             ORDER BY fkc.constraint_column_id
             FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, N''), N'')
       + N' | delete=' + fk.delete_referential_action_desc COLLATE DATABASE_DEFAULT
       + N' update=' + fk.update_referential_action_desc COLLATE DATABASE_DEFAULT
       + N' | disabled=' + CAST(fk.is_disabled AS NVARCHAR(1))
       + N' trusted=' + CAST(1 - fk.is_not_trusted AS NVARCHAR(1)) AS [foreign_key]
FROM sys.foreign_keys fk
     JOIN sys.tables pt ON pt.object_id = fk.parent_object_id
     JOIN sys.schemas ps ON ps.schema_id = pt.schema_id
     JOIN sys.tables rtab ON rtab.object_id = fk.referenced_object_id
     JOIN sys.schemas rs ON rs.schema_id = rtab.schema_id
ORDER BY [foreign_key];
