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
    c_computed         AS (c_int * 2)
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
