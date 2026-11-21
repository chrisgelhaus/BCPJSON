/*
    Seed data for the integration source database. Run with sqlcmd: -v DbName=<database>
    Kept ASCII-only; non-ASCII characters are built with NCHAR() so the file encoding cannot matter.
*/
USE [$(DbName)];
GO
SET NOCOUNT ON;

DECLARE @crlf NCHAR(2) = NCHAR(13) + NCHAR(10);
DECLARE @emoji NVARCHAR(4) = NCHAR(0xD83D) + NCHAR(0xDE00);   -- U+1F600 as a surrogate pair
DECLARE @cjk NVARCHAR(4) = NCHAR(0x4E2D) + NCHAR(0x6587);
DECLARE @gruesse NVARCHAR(10) = N'Gr' + NCHAR(252) + NCHAR(223) + N'e';

-- Row 1: every column NULL
INSERT rt.AllTypes (Id) VALUES (1);

-- Row 2: ordinary values
INSERT rt.AllTypes (Id, c_bigint, c_int, c_smallint, c_tinyint, c_bit, c_decimal, c_numeric, c_money, c_smallmoney, c_float, c_real,
                    c_date, c_time, c_datetime, c_datetime2, c_smalldatetime, c_datetimeoffset,
                    c_char, c_varchar, c_varcharmax, c_nchar, c_nvarchar, c_nvarcharmax, c_text, c_ntext,
                    c_binary, c_varbinary, c_varbinarymax, c_image, c_uniqueidentifier, c_xml)
VALUES (2, 42, 7, 3, 1, 1, 12345.6789012345, 1.5, 19.99, 2.5, 3.141592653589793, 2.5,
        '2024-02-29', '12:34:56.1234567', '2024-02-29T23:59:59.997', '2024-02-29T23:59:59.1234567', '2024-02-29T23:59:00', '2024-02-29T23:59:59.1234567+05:30',
        'abc', 'hello', REPLICATE(CAST('x' AS VARCHAR(MAX)), 9000), NCHAR(228), @gruesse, REPLICATE(CAST(NCHAR(233) AS NVARCHAR(MAX)), 5000), 'legacy text', N'legacy ntext',
        0x0102030405060708, 0xDEADBEEF, CONVERT(VARBINARY(MAX), REPLICATE(CAST('AB' AS VARCHAR(MAX)), 10000)), 0x00FF,
        '6F9619FF-8B86-D011-B42D-00C04FC964FF', N'<root a="1"><child>text &amp; more</child></root>');

-- Row 3: maximum values (full 38-digit decimals are in plain.Decimals)
INSERT rt.AllTypes (Id, c_bigint, c_int, c_smallint, c_tinyint, c_bit, c_decimal, c_numeric, c_money, c_smallmoney, c_float, c_real,
                    c_date, c_time, c_datetime, c_datetime2, c_smalldatetime, c_datetimeoffset,
                    c_char, c_varchar, c_nchar, c_nvarchar, c_binary, c_varbinary, c_uniqueidentifier)
VALUES (3, 9223372036854775807, 2147483647, 32767, 255, 0, 123456789012345678.0123456789, 99999999999999.9999, 922337203685477.5807, 214748.3647, 1.7976931348623157E+308, 3.4E+38,
        '9999-12-31', '23:59:59.9999999', '9999-12-31T23:59:59.997', '9999-12-31T23:59:59.9999999', '2079-06-06T23:59:00', '9999-12-31T23:59:59.9999999+14:00',
        'ZZZZZZZZZZ', REPLICATE('z', 100), REPLICATE(NCHAR(0x00FF), 10), REPLICATE(NCHAR(0x4E2D), 200), 0xFFFFFFFFFFFFFFFF, CONVERT(VARBINARY(50), REPLICATE(CHAR(255), 50)),
        'FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF');

-- Row 4: minimum values
INSERT rt.AllTypes (Id, c_bigint, c_int, c_smallint, c_tinyint, c_bit, c_decimal, c_numeric, c_money, c_smallmoney, c_float, c_real,
                    c_date, c_time, c_datetime, c_datetime2, c_smalldatetime, c_datetimeoffset,
                    c_char, c_varchar, c_nchar, c_nvarchar, c_binary, c_varbinary, c_uniqueidentifier)
VALUES (4, -9223372036854775808, -2147483648, -32768, 0, 0, -0.0000000001, -99999999999999.9999, -922337203685477.5808, -214748.3648, -2.2250738585072014E-308, -1.18E-38,
        '0001-01-01', '00:00:00', '1753-01-01T00:00:00', '0001-01-01T00:00:00', '1900-01-01T00:00:00', '0001-01-01T00:00:00-12:00',
        '', '', N'', N'', 0x0000000000000000, 0x, '00000000-0000-0000-0000-000000000000');

-- Row 5: awkward text
INSERT rt.AllTypes (Id, c_char, c_varchar, c_varcharmax, c_nchar, c_nvarchar, c_nvarcharmax, c_text, c_ntext, c_xml)
VALUES (5, 'a|b', 'pipe|here', '  leading and trailing  ', N'"q"', N'line1' + @crlf + N'line2' + NCHAR(9) + N'tab "quoted" \backslash',
        N'emoji ' + @emoji + N' and ' + @cjk + @crlf + N'{"json":"inside"},[1,2]', 'text with ''apostrophe''', N'ntext ' + @emoji,
        N'<a><![CDATA[<not xml> | pipe]]></a>');

INSERT rt.[odd ]] name's] ([col with space], [quote"col], [semi;col])
VALUES (1, N'say "hi"', 'a;b'),
       (2, NULL, NULL),
       (3, N'O''Brien', '');

-- No primary key: export sorts by every column. Includes duplicate rows and NULLs.
INSERT rt.NoKey (A, B)
VALUES (1, 'x'), (1, 'x'), (NULL, 'y'), (2, NULL), (NULL, NULL);

-- 120,000 rows: crosses the 50,000-row JSON import batch size.
WITH n AS
(
    SELECT TOP (120000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i
    FROM sys.all_objects a CROSS JOIN sys.all_objects b
)
INSERT plain.Numbers (Id, Big, Txt, Amount, Ratio, Stamp, Uid)
SELECT i,
       CAST(i AS BIGINT) * 1000003,
       'row ' + CAST(i AS VARCHAR(10)),
       CASE WHEN i % 7 = 0 THEN NULL ELSE i * 1.2345 END,
       CASE WHEN i % 11 = 0 THEN NULL ELSE i * 0.25 END,
       DATEADD(SECOND, i, CAST('2020-01-01T00:00:00.123' AS DATETIME2(3))),
       NEWID()
FROM n;

-- 38-digit decimals: maximum, minimum, smallest fraction, values at and just past System.Decimal's limit
-- (79228162514264337593543950335), ordinary values and NULLs.
-- One INSERT per row: a multi-row VALUES gives each column one common type, which overflows when 38-digit
-- fractions are mixed with integers.
INSERT plain.Decimals (Id, Whole, Mixed, Half, Fraction, Small) VALUES (1, NULL, NULL, NULL, NULL, NULL);
INSERT plain.Decimals (Id, Whole, Mixed, Half, Fraction, Small)
VALUES (2, 99999999999999999999999999999999999999, 9999999999999999999999999999.9999999999,
        9999999999999999999.9999999999999999999, 0.99999999999999999999999999999999999999, 999999999999999999999999999.99);
INSERT plain.Decimals (Id, Whole, Mixed, Half, Fraction, Small)
VALUES (3, -99999999999999999999999999999999999999, -9999999999999999999999999999.9999999999,
        -9999999999999999999.9999999999999999999, -0.99999999999999999999999999999999999999, -999999999999999999999999999.99);
INSERT plain.Decimals (Id, Whole, Mixed, Half, Fraction, Small)
VALUES (4, 1, 0.0000000001, 0.0000000000000000001, 0.00000000000000000000000000000000000001, 0.01);
INSERT plain.Decimals (Id, Whole, Mixed, Half, Fraction, Small)
VALUES (5, 79228162514264337593543950335, 1234567890123456789.0123456789,
        1234567890123456789.0123456789012345678, 0.12345678901234567890123456789012345678, 1.50);
INSERT plain.Decimals (Id, Whole, Mixed, Half, Fraction, Small)
VALUES (6, 79228162514264337593543950336, 12345.6789000000, 0, 0, 0);
INSERT plain.Decimals (Id, Whole, Mixed, Half, Fraction, Small)
VALUES (7, 0, -0.5000000000, 42.5, -0.5, -12.34);

-- Identity values with gaps must be preserved.
INSERT plain.IdentityGaps (Name) VALUES ('a'), ('b'), ('c'), ('d'), ('e');
DELETE plain.IdentityGaps WHERE Id IN (1005, 1015);

-- CLR types. geography/geometry use the SRIDs that ::Parse() assumes (4326 / 0); other SRIDs are a known JSON limitation.
INSERT plain.Spatial (Id, Geo, Geom, Node)
VALUES (1, NULL, NULL, NULL),
       (2, geography::STGeomFromText('POINT(-122.35 47.65)', 4326), geometry::STGeomFromText('POINT(1 2)', 0), hierarchyid::Parse('/')),
       (3, geography::STGeomFromText('LINESTRING(-122.36 47.656, -122.343 47.656)', 4326),
           geometry::STGeomFromText('POLYGON((0 0, 3 0, 3 3, 0 3, 0 0))', 0), hierarchyid::Parse('/1/2.5/3/'));

-- Foreign keys: valid references, plus an orphan (99) that only the disabled FK_Ref_Off allows.
INSERT rt.Parent (Id, Code, A, B) VALUES (1, 'AAA', 1, 1), (2, 'BBB', 1, 2), (3, 'CCC', 2, 1);
INSERT rt.Child (Id, ParentId, ParentCode, A, B, ManagerId)
VALUES (1, 1, 'AAA', 1, 1, NULL), (2, 2, 'BBB', 1, 2, 1), (3, NULL, NULL, NULL, NULL, 2), (4, 3, 'CCC', 2, 1, 1);
INSERT plain.Ref (Id, ChildId, LooseChildId, OffChildId) VALUES (1, 1, 2, 99), (2, NULL, NULL, NULL), (3, 4, 4, 3);
GO
