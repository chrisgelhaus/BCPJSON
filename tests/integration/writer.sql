/*
    Background writer for the snapshot consistency test. Runs against BcpJsonLive until inv.Control.Stop = 1
    (or for at most 10 minutes). Each transaction moves a random amount from inv.A to the same row of inv.B and
    counts the transfer, so the combined total never changes at a committed point in time.
    Deadlock victims (the non-snapshot control export takes shared locks) are retried.
*/
SET NOCOUNT ON;
SET XACT_ABORT ON;
DECLARE @end DATETIME2 = DATEADD(MINUTE, 10, SYSUTCDATETIME());
DECLARE @id INT, @amount BIGINT, @stop BIT = 0;

WHILE @stop = 0 AND SYSUTCDATETIME() < @end
BEGIN
    SET @id = 1 + ABS(CHECKSUM(NEWID())) % 300000;
    SET @amount = 1 + ABS(CHECKSUM(NEWID())) % 100;
    BEGIN TRY
        BEGIN TRANSACTION;
        UPDATE inv.A SET Value = Value - @amount WHERE Id = @id;
        UPDATE inv.B SET Value = Value + @amount WHERE Id = @id;
        UPDATE inv.Control SET Transfers = Transfers + 1, @stop = [Stop] WHERE Id = 1;
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        IF ERROR_NUMBER() <> 1205 THROW;   -- anything but a deadlock is a real failure
    END CATCH;
END;

SELECT CONCAT('writer stopped after ', Transfers, ' transfers') FROM inv.Control WHERE Id = 1;
