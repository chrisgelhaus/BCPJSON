/*
    Database for the snapshot consistency test (roundtrip.sh consistency).

    inv.A and inv.B hold 300,000 rows each. writer.sql moves random amounts from a row of A to the same row of B
    in single transactions, so SUM(A.Value) + SUM(B.Value) is constant at every committed point in time.
    An export taken at one point in time (--snapshot) must add up to that total; an export that reads A and B at
    different moments while transfers are running will not. inv.Control counts committed transfers (updated in
    the same transactions) and tells the writer when to stop.
*/
IF DB_ID(N'BcpJsonLive') IS NOT NULL
BEGIN
    ALTER DATABASE BcpJsonLive SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE BcpJsonLive;
END;
GO
CREATE DATABASE BcpJsonLive;
GO
USE BcpJsonLive;
GO
CREATE SCHEMA inv;
GO
CREATE TABLE inv.A (Id INT NOT NULL PRIMARY KEY, Value BIGINT NOT NULL);
CREATE TABLE inv.B (Id INT NOT NULL PRIMARY KEY, Value BIGINT NOT NULL);
CREATE TABLE inv.Control (Id INT NOT NULL PRIMARY KEY, Transfers BIGINT NOT NULL, [Stop] BIT NOT NULL);

WITH n AS
(
    SELECT TOP (300000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i
    FROM sys.all_objects a CROSS JOIN sys.all_objects b
)
INSERT inv.A (Id, Value) SELECT i, 1000 FROM n;

INSERT inv.B (Id, Value) SELECT Id, 1000 FROM inv.A;
INSERT inv.Control (Id, Transfers, [Stop]) VALUES (1, 0, 0);
GO
