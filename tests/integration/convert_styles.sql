/*
    Split boundaries travel as text: CONVERT(nvarchar(4000), value[, style]) when planned, CONVERT(type, text[, style])
    in the range predicates (SplitColumn.ConvertStyle). Every type must come back as the same value and the same text.
    Fails (THROW) naming each type that does not.
*/
SET NOCOUNT ON;
DECLARE @failures NVARCHAR(MAX) = N'';
DECLARE @t NVARCHAR(4000);

DECLARE @float FLOAT = 0.1;
SET @t = CONVERT(NVARCHAR(4000), @float, 3);
IF CONVERT(FLOAT, @t, 3) <> @float OR CONVERT(NVARCHAR(4000), CONVERT(FLOAT, @t, 3), 3) <> @t SET @failures += N' float(' + @t + N')';

DECLARE @floatMax FLOAT = -2.2250738585072014E-308;
SET @t = CONVERT(NVARCHAR(4000), @floatMax, 3);
IF CONVERT(FLOAT, @t, 3) <> @floatMax OR CONVERT(NVARCHAR(4000), CONVERT(FLOAT, @t, 3), 3) <> @t SET @failures += N' float(' + @t + N')';

DECLARE @real REAL = 0.1;
SET @t = CONVERT(NVARCHAR(4000), @real, 3);
IF CONVERT(REAL, @t, 3) <> @real OR CONVERT(NVARCHAR(4000), CONVERT(REAL, @t, 3), 3) <> @t SET @failures += N' real(' + @t + N')';

DECLARE @money MONEY = -922337203685477.5807;
SET @t = CONVERT(NVARCHAR(4000), @money, 2);
IF CONVERT(MONEY, @t, 2) <> @money OR CONVERT(NVARCHAR(4000), CONVERT(MONEY, @t, 2), 2) <> @t SET @failures += N' money(' + @t + N')';

DECLARE @smallmoney SMALLMONEY = 214748.3647;
SET @t = CONVERT(NVARCHAR(4000), @smallmoney, 2);
IF CONVERT(SMALLMONEY, @t, 2) <> @smallmoney OR CONVERT(NVARCHAR(4000), CONVERT(SMALLMONEY, @t, 2), 2) <> @t SET @failures += N' smallmoney(' + @t + N')';

DECLARE @varbinary VARBINARY(50) = 0xDEADBEEF00;
SET @t = CONVERT(NVARCHAR(4000), @varbinary, 1);
IF CONVERT(VARBINARY(50), @t, 1) <> @varbinary OR CONVERT(NVARCHAR(4000), CONVERT(VARBINARY(50), @t, 1), 1) <> @t SET @failures += N' varbinary(' + @t + N')';

DECLARE @binary BINARY(4) = 0x000102FF;
SET @t = CONVERT(NVARCHAR(4000), @binary, 1);
IF CONVERT(BINARY(4), @t, 1) <> @binary OR CONVERT(NVARCHAR(4000), CONVERT(BINARY(4), @t, 1), 1) <> @t SET @failures += N' binary(' + @t + N')';

DECLARE @datetime2 DATETIME2(7) = '2024-02-29T23:59:59.1234567';
SET @t = CONVERT(NVARCHAR(4000), @datetime2, 126);
IF CONVERT(DATETIME2(7), @t, 126) <> @datetime2 OR CONVERT(NVARCHAR(4000), CONVERT(DATETIME2(7), @t, 126), 126) <> @t SET @failures += N' datetime2(' + @t + N')';

DECLARE @datetime2Whole DATETIME2(7) = '2026-01-01T00:00:00';
SET @t = CONVERT(NVARCHAR(4000), @datetime2Whole, 126);
IF CONVERT(DATETIME2(7), @t, 126) <> @datetime2Whole OR CONVERT(NVARCHAR(4000), CONVERT(DATETIME2(7), @t, 126), 126) <> @t SET @failures += N' datetime2(' + @t + N')';

DECLARE @datetimeoffset DATETIMEOFFSET(7) = '2024-02-29T23:59:59.1234567+05:30';
SET @t = CONVERT(NVARCHAR(4000), @datetimeoffset, 126);
IF CONVERT(DATETIMEOFFSET(7), @t, 126) <> @datetimeoffset OR CONVERT(NVARCHAR(4000), CONVERT(DATETIMEOFFSET(7), @t, 126), 126) <> @t SET @failures += N' datetimeoffset(' + @t + N')';

DECLARE @datetime DATETIME = '2024-02-29T23:59:59.997';
SET @t = CONVERT(NVARCHAR(4000), @datetime, 126);
IF CONVERT(DATETIME, @t, 126) <> @datetime OR CONVERT(NVARCHAR(4000), CONVERT(DATETIME, @t, 126), 126) <> @t SET @failures += N' datetime(' + @t + N')';

DECLARE @smalldatetime SMALLDATETIME = '2079-06-06T23:59:00';
SET @t = CONVERT(NVARCHAR(4000), @smalldatetime, 126);
IF CONVERT(SMALLDATETIME, @t, 126) <> @smalldatetime OR CONVERT(NVARCHAR(4000), CONVERT(SMALLDATETIME, @t, 126), 126) <> @t SET @failures += N' smalldatetime(' + @t + N')';

DECLARE @date DATE = '0001-01-01';
SET @t = CONVERT(NVARCHAR(4000), @date, 126);
IF CONVERT(DATE, @t, 126) <> @date OR CONVERT(NVARCHAR(4000), CONVERT(DATE, @t, 126), 126) <> @t SET @failures += N' date(' + @t + N')';

DECLARE @time TIME(7) = '23:59:59.9999999';
SET @t = CONVERT(NVARCHAR(4000), @time);
IF CONVERT(TIME(7), @t) <> @time OR CONVERT(NVARCHAR(4000), CONVERT(TIME(7), @t)) <> @t SET @failures += N' time(' + @t + N')';

DECLARE @decimal DECIMAL(38, 10) = -1234567890123456789012345678.0123456789;
SET @t = CONVERT(NVARCHAR(4000), @decimal);
IF CONVERT(DECIMAL(38, 10), @t) <> @decimal OR CONVERT(NVARCHAR(4000), CONVERT(DECIMAL(38, 10), @t)) <> @t SET @failures += N' decimal(' + @t + N')';

DECLARE @guid UNIQUEIDENTIFIER = '6F9619FF-8B86-D011-B42D-00C04FC964FF';
SET @t = CONVERT(NVARCHAR(4000), @guid);
IF CONVERT(UNIQUEIDENTIFIER, @t) <> @guid OR CONVERT(NVARCHAR(4000), CONVERT(UNIQUEIDENTIFIER, @t)) <> @t SET @failures += N' uniqueidentifier(' + @t + N')';

/* Interpolated boundaries (KeyInterpolation) are written in these forms; each must convert to the intended value. */
IF CONVERT(DATETIME, N'2026-01-01T00:00:01.010', 126) <> DATETIMEFROMPARTS(2026, 1, 1, 0, 0, 1, 10) SET @failures += N' datetime(interpolated)';
IF CONVERT(SMALLDATETIME, N'2026-01-01T05:01:00', 126) <> SMALLDATETIMEFROMPARTS(2026, 1, 1, 5, 1) SET @failures += N' smalldatetime(interpolated)';
IF CONVERT(DATE, N'2026-07-02', 126) <> DATEFROMPARTS(2026, 7, 2) SET @failures += N' date(interpolated)';
IF CONVERT(DATETIME2(7), N'2026-01-01T12:00:00.1234567', 126) <> DATETIME2FROMPARTS(2026, 1, 1, 12, 0, 0, 1234567, 7) SET @failures += N' datetime2(7)(interpolated)';
IF CONVERT(DATETIME2(3), N'2026-01-01T12:00:00.123', 126) <> DATETIME2FROMPARTS(2026, 1, 1, 12, 0, 0, 123, 3) SET @failures += N' datetime2(3)(interpolated)';
IF CONVERT(DATETIME2(0), N'2026-01-01T12:00:01', 126) <> DATETIME2FROMPARTS(2026, 1, 1, 12, 0, 1, 0, 0) SET @failures += N' datetime2(0)(interpolated)';
IF CONVERT(DATETIMEOFFSET(7), N'2026-01-01T12:00:00.1234567+05:30', 126) <> DATETIMEOFFSETFROMPARTS(2026, 1, 1, 12, 0, 0, 1234567, 5, 30, 7)
   OR DATEPART(TZOFFSET, CONVERT(DATETIMEOFFSET(7), N'2026-01-01T12:00:00.1234567+05:30', 126)) <> 330 SET @failures += N' datetimeoffset(interpolated)';
IF CONVERT(DATETIMEOFFSET(0), N'2026-01-01T12:00:00-08:00', 126) <> DATETIMEOFFSETFROMPARTS(2026, 1, 1, 12, 0, 0, 0, -8, 0, 0) SET @failures += N' datetimeoffset(0)(interpolated)';
IF CONVERT(MONEY, N'-1234.5678', 2) <> CAST(-1234.5678 AS MONEY) SET @failures += N' money(interpolated)';
IF CONVERT(DECIMAL(18, 4), N'0.5000') <> 0.5 SET @failures += N' decimal(interpolated)';
IF CONVERT(BIGINT, N'9223372036854775400') <> 9223372036854775400 SET @failures += N' bigint(interpolated)';

IF @failures <> N''
BEGIN
    DECLARE @message NVARCHAR(2048) = LEFT(N'CONVERT styles do not round-trip:' + @failures, 2048);
    THROW 50000, @message, 1;
END;
PRINT N'CONVERT styles round-trip every split column type.';
