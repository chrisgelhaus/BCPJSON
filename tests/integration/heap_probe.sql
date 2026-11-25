-- Heap probe: do disabled nonclustered indexes stay disabled when the clustered index is dropped and
-- created again? import --load-as-heap relies on it (design: docs/superpowers/specs/2026-11-23-load-as-heap-design.md).
-- Run with: sqlcmd -S <server> -U sa -P <password> -C -b -I -i tests/integration/heap_probe.sql
-- Any failed assertion THROWs, so sqlcmd -b exits non-zero (the database is then left behind). Creates and drops database bcpjson_heapprobe.
SET NOCOUNT ON;
GO
IF DB_ID(N'bcpjson_heapprobe') IS NOT NULL
BEGIN
    ALTER DATABASE bcpjson_heapprobe SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE bcpjson_heapprobe;
END
CREATE DATABASE bcpjson_heapprobe;
GO
USE bcpjson_heapprobe;
GO
    -- Case A: the clustered index is a PRIMARY KEY constraint, the other indexes are nonclustered and unique nonclustered.
    CREATE TABLE dbo.CaseA (Id int NOT NULL, Name varchar(20) NOT NULL, Code int NOT NULL,
        CONSTRAINT PK_CaseA PRIMARY KEY CLUSTERED (Id));
    CREATE NONCLUSTERED INDEX IX_CaseA_Name ON dbo.CaseA (Name);
    CREATE UNIQUE NONCLUSTERED INDEX UX_CaseA_Code ON dbo.CaseA (Code);

    INSERT dbo.CaseA (Id, Name, Code)
    SELECT TOP (1000) n, CONCAT('n', n), n
    FROM (SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM sys.all_objects a CROSS JOIN sys.all_objects b) x
    ORDER BY n;

    ALTER INDEX IX_CaseA_Name ON dbo.CaseA DISABLE;
    ALTER INDEX UX_CaseA_Code ON dbo.CaseA DISABLE;
    ALTER TABLE dbo.CaseA DROP CONSTRAINT PK_CaseA;

    IF (SELECT COUNT(*) FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.CaseA') AND name IN (N'IX_CaseA_Name', N'UX_CaseA_Code') AND is_disabled = 1) <> 2
        THROW 50000, 'A: both nonclustered indexes should still exist and be disabled after dropping the primary key', 1;
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.CaseA') AND index_id = 0)
        THROW 50000, 'A: the table should be a heap after dropping the primary key', 1;

    INSERT dbo.CaseA (Id, Name, Code)
    SELECT TOP (1000) 1000 + n, CONCAT('n', 1000 + n), 1000 + n
    FROM (SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM sys.all_objects a CROSS JOIN sys.all_objects b) x
    ORDER BY n;

    ALTER TABLE dbo.CaseA ADD CONSTRAINT PK_CaseA PRIMARY KEY CLUSTERED (Id);

    IF (SELECT COUNT(*) FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.CaseA') AND name IN (N'IX_CaseA_Name', N'UX_CaseA_Code') AND is_disabled = 1) <> 2
        THROW 50000, 'A: both nonclustered indexes should still be disabled after creating the primary key again', 1;

    ALTER INDEX IX_CaseA_Name ON dbo.CaseA REBUILD;
    ALTER INDEX UX_CaseA_Code ON dbo.CaseA REBUILD;

    IF (SELECT COUNT(*) FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.CaseA') AND name IN (N'IX_CaseA_Name', N'UX_CaseA_Code') AND is_disabled = 0) <> 2
        THROW 50000, 'A: both nonclustered indexes should be enabled after the rebuild', 1;
    IF (SELECT COUNT(*) FROM dbo.CaseA) <> 2000
        THROW 50000, 'A: the table should hold 2000 rows', 1;

    -- Case B: the primary key is nonclustered and a separate clustered index sits on another column.
    CREATE TABLE dbo.CaseB (Id int NOT NULL, Name varchar(20) NOT NULL,
        CONSTRAINT PK_CaseB PRIMARY KEY NONCLUSTERED (Id));
    CREATE CLUSTERED INDEX CX_CaseB_Name ON dbo.CaseB (Name);

    INSERT dbo.CaseB (Id, Name)
    SELECT TOP (1000) n, CONCAT('n', n)
    FROM (SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM sys.all_objects a CROSS JOIN sys.all_objects b) x
    ORDER BY n;

    ALTER INDEX PK_CaseB ON dbo.CaseB DISABLE;
    DROP INDEX CX_CaseB_Name ON dbo.CaseB;

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.CaseB') AND name = N'PK_CaseB' AND is_disabled = 1)
        THROW 50000, 'B: the primary key should still exist and be disabled after dropping the clustered index', 1;

    INSERT dbo.CaseB (Id, Name)
    SELECT TOP (1000) 1000 + n, CONCAT('n', 1000 + n)
    FROM (SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM sys.all_objects a CROSS JOIN sys.all_objects b) x
    ORDER BY n;

    CREATE CLUSTERED INDEX CX_CaseB_Name ON dbo.CaseB (Name);

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.CaseB') AND name = N'PK_CaseB' AND is_disabled = 1)
        THROW 50000, 'B: the primary key should still be disabled after creating the clustered index again', 1;

    ALTER INDEX PK_CaseB ON dbo.CaseB REBUILD;

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.CaseB') AND name = N'PK_CaseB' AND is_disabled = 0)
        THROW 50000, 'B: the primary key should be enabled after the rebuild', 1;
    IF (SELECT COUNT(*) FROM dbo.CaseB) <> 2000
        THROW 50000, 'B: the table should hold 2000 rows', 1;

    PRINT 'heap probe: disabled indexes stay disabled across drop and create of the clustered index';
GO
USE master;
GO
IF DB_ID(N'bcpjson_heapprobe') IS NOT NULL
BEGIN
    ALTER DATABASE bcpjson_heapprobe SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE bcpjson_heapprobe;
END
GO
