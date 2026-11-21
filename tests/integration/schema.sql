/*
    Integration test schema. Run with sqlcmd: -v DbName=<database>
    Creates the database and two schemas:
      rt    - awkward data (every column type, odd names, |, line breaks, unicode). JSON-only: BCP char format
              cannot round-trip | or line breaks (known limitation).
      plain - data that BCP char format can round-trip: a 120k-row table (crosses the 50k JSON import batch),
              identity with gaps, and CLR types.
*/
CREATE DATABASE [$(DbName)];
GO
USE [$(DbName)];
GO
CREATE SCHEMA rt;
GO
CREATE SCHEMA plain;
GO

CREATE TABLE rt.AllTypes
(
    Id                 INT              NOT NULL PRIMARY KEY,
    c_bigint           BIGINT           NULL,
    c_int              INT              NULL,
    c_smallint         SMALLINT         NULL,
    c_tinyint          TINYINT          NULL,
    c_bit              BIT              NULL,
    c_decimal          DECIMAL(38, 10)  NULL,
    c_numeric          NUMERIC(18, 4)   NULL,
    c_money            MONEY            NULL,
    c_smallmoney       SMALLMONEY       NULL,
    c_float            FLOAT            NULL,
    c_real             REAL             NULL,
    c_date             DATE             NULL,
    c_time             TIME(7)          NULL,
    c_datetime         DATETIME         NULL,
    c_datetime2        DATETIME2(7)     NULL,
    c_smalldatetime    SMALLDATETIME    NULL,
    c_datetimeoffset   DATETIMEOFFSET(7) NULL,
    c_char             CHAR(10)         NULL,
    c_varchar          VARCHAR(100)     NULL,
    c_varcharmax       VARCHAR(MAX)     NULL,
    c_nchar            NCHAR(10)        NULL,
    c_nvarchar         NVARCHAR(200)    NULL,
    c_nvarcharmax      NVARCHAR(MAX)    NULL,
    c_text             TEXT             NULL,
    c_ntext            NTEXT            NULL,
    c_binary           BINARY(8)        NULL,
    c_varbinary        VARBINARY(50)    NULL,
    c_varbinarymax     VARBINARY(MAX)   NULL,
    c_image            IMAGE            NULL,
    c_uniqueidentifier UNIQUEIDENTIFIER NULL,
    c_xml              XML              NULL,
    c_computed         AS (CAST(c_int AS BIGINT) * 2)
);

CREATE TABLE rt.[odd ]] name's]
(
    [col with space] INT          NOT NULL PRIMARY KEY,
    [quote"col]      NVARCHAR(20) NULL,
    [semi;col]       VARCHAR(10)  NULL
);

CREATE TABLE rt.NoKey
(
    A INT         NULL,
    B VARCHAR(10) NULL
);

CREATE TABLE rt.Empty
(
    Id INT NOT NULL PRIMARY KEY
);

CREATE TABLE plain.Numbers
(
    Id     INT              NOT NULL PRIMARY KEY,
    Big    BIGINT           NOT NULL,
    Txt    VARCHAR(40)      NOT NULL,
    Amount DECIMAL(19, 4)   NULL,
    Ratio  FLOAT            NULL,
    Stamp  DATETIME2(3)     NOT NULL,
    Uid    UNIQUEIDENTIFIER NOT NULL
);

-- Full 38-digit precision: values beyond System.Decimal's 28-29 significant digits.
CREATE TABLE plain.Decimals
(
    Id        INT             NOT NULL PRIMARY KEY,
    Whole     DECIMAL(38, 0)  NULL,
    Mixed     DECIMAL(38, 10) NULL,
    Half      NUMERIC(38, 19) NULL,
    Fraction  DECIMAL(38, 38) NULL,
    Small     DECIMAL(29, 2)  NULL
);

CREATE TABLE plain.IdentityGaps
(
    Id   INT IDENTITY(1000, 5) NOT NULL PRIMARY KEY,
    Name VARCHAR(20)           NOT NULL
);

CREATE TABLE plain.Spatial
(
    Id   INT         NOT NULL PRIMARY KEY,
    Geo  GEOGRAPHY   NULL,
    Geom GEOMETRY    NULL,
    Node HIERARCHYID NULL
);
GO

/* One index of every kind, so copy's index recreation is checked against a real server (indexes.sql compares them). */
CREATE UNIQUE NONCLUSTERED INDEX UX_AllTypes_Guid ON rt.AllTypes (c_uniqueidentifier) WHERE c_uniqueidentifier IS NOT NULL;
CREATE NONCLUSTERED INDEX IX_AllTypes_Int_Date ON rt.AllTypes (c_int DESC, c_date) INCLUDE (c_varchar);
CREATE PRIMARY XML INDEX PXML_AllTypes_Xml ON rt.AllTypes (c_xml);
CREATE XML INDEX SXML_AllTypes_Xml_Path ON rt.AllTypes (c_xml) USING XML INDEX PXML_AllTypes_Xml FOR PATH;
CREATE NONCLUSTERED INDEX [IX odd ]] name's] ON rt.[odd ]] name's] ([semi;col]);
CREATE CLUSTERED INDEX CX_NoKey_A ON rt.NoKey (A);
ALTER TABLE plain.Numbers ADD CONSTRAINT UQ_Numbers_Uid UNIQUE NONCLUSTERED (Uid);
CREATE NONCLUSTERED INDEX IX_Numbers_Stamp ON plain.Numbers (Stamp) WITH (DATA_COMPRESSION = PAGE);
CREATE NONCLUSTERED COLUMNSTORE INDEX NCCI_Numbers ON plain.Numbers (Big, Amount, Ratio);
CREATE SPATIAL INDEX SIX_Spatial_Geo ON plain.Spatial (Geo);
GO

/* One foreign key of every kind, for copy --foreign-keys (foreign_keys.sql compares them). */
CREATE TABLE rt.Parent
(
    Id   INT     NOT NULL PRIMARY KEY,
    Code CHAR(3) NOT NULL CONSTRAINT UQ_Parent_Code UNIQUE,
    A    INT     NOT NULL,
    B    INT     NOT NULL,
    CONSTRAINT UQ_Parent_AB UNIQUE (A, B)
);

CREATE TABLE rt.Child
(
    Id         INT     NOT NULL CONSTRAINT PK_Child PRIMARY KEY,
    ParentId   INT     NULL CONSTRAINT FK_Child_Parent REFERENCES rt.Parent (Id) ON DELETE CASCADE,   -- cascade delete
    ParentCode CHAR(3) NULL CONSTRAINT FK_Child_ParentCode REFERENCES rt.Parent (Code),            -- to a unique constraint
    A          INT     NULL,
    B          INT     NULL,
    ManagerId  INT     NULL CONSTRAINT FK_Child_Manager REFERENCES rt.Child (Id),                  -- self-reference
    CONSTRAINT FK_Child_ParentAB FOREIGN KEY (A, B) REFERENCES rt.Parent (A, B) ON UPDATE CASCADE  -- two columns
);

CREATE TABLE plain.Ref
(
    Id           INT NOT NULL PRIMARY KEY,
    ChildId      INT NULL REFERENCES rt.Child (Id) ON DELETE SET NULL,   -- cross-schema, system-named
    LooseChildId INT NULL,
    OffChildId   INT NULL
);
ALTER TABLE plain.Ref WITH NOCHECK ADD CONSTRAINT FK_Ref_Loose FOREIGN KEY (LooseChildId) REFERENCES rt.Child (Id);  -- not trusted
ALTER TABLE plain.Ref ADD CONSTRAINT FK_Ref_Off FOREIGN KEY (OffChildId) REFERENCES rt.Child (Id);
ALTER TABLE plain.Ref NOCHECK CONSTRAINT FK_Ref_Off;                                                                -- disabled
GO

/* One check constraint of every kind (check_constraints.sql compares them). data.sql has a row only the disabled one allows. */
ALTER TABLE rt.Parent ADD CONSTRAINT CK_Parent_Code CHECK (Code LIKE '[A-Z][A-Z][A-Z]');
ALTER TABLE rt.Parent ADD CHECK (A >= 0 AND B >= 0);                                                                   -- system-named
ALTER TABLE rt.Child ADD CONSTRAINT CK_Child_AB CHECK ((A IS NULL AND B IS NULL) OR (A IS NOT NULL AND B IS NOT NULL));
ALTER TABLE plain.Ref WITH NOCHECK ADD CONSTRAINT CK_Ref_Loose CHECK (LooseChildId > 0);                                -- not trusted
ALTER TABLE plain.Ref ADD CONSTRAINT CK_Ref_Off CHECK (OffChildId < 50);
ALTER TABLE plain.Ref NOCHECK CONSTRAINT CK_Ref_Off;                                                                   -- disabled
GO

/* Triggers (triggers.sql compares them). rt.ChildAudit is filled by TR_Child_Audit when data.sql inserts rt.Child, and is
   copied as data: if any trigger fired during a copy or import, the audit table would gain rows and the data diff fails. */
CREATE TABLE rt.ChildAudit
(
    Id      INT     IDENTITY(1, 1) NOT NULL PRIMARY KEY,
    ChildId INT     NOT NULL,
    Action  CHAR(1) NOT NULL
);
GO
CREATE TRIGGER rt.TR_Child_Audit ON rt.Child AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    INSERT rt.ChildAudit (ChildId, Action)
    SELECT Id, CASE WHEN EXISTS (SELECT 1 FROM deleted) THEN 'U' ELSE 'I' END FROM inserted;
END;
GO
CREATE TRIGGER rt.TR_Child_Second ON rt.Child AFTER INSERT AS SET NOCOUNT ON;
GO
EXEC sp_settriggerorder @triggername = N'rt.TR_Child_Audit', @order = N'First', @stmttype = N'INSERT';
EXEC sp_settriggerorder @triggername = N'rt.TR_Child_Second', @order = N'Last', @stmttype = N'INSERT';
GO
CREATE TRIGGER rt.TR_Child_Disabled ON rt.Child AFTER DELETE AS SET NOCOUNT ON;
GO
DISABLE TRIGGER rt.TR_Child_Disabled ON rt.Child;
GO
CREATE TRIGGER rt.TR_Empty_InsteadOfDelete ON rt.Empty INSTEAD OF DELETE AS SET NOCOUNT ON;
GO
