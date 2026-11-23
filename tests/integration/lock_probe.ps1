# Lock probe: which table lock does a TABLOCK SqlBulkCopy take on a heap, a clustered rowstore table and a
# clustered columnstore table? Decides whether parts of a split table can load in parallel (LoadPlanning).
# Needs SQL_SERVER, SQL_PASSWORD (SQL_USER defaults to sa), sqlcmd on PATH, and no published bcpjson.
# Prints "heap=<mode> clustered=<mode> cci=<mode>"; exits 1 unless heap is BU (bulk update: loads run in
# parallel), clustered is exclusive and cci is the mode recorded below.
# Observed on SQL Server 2019, 2022 and 2025 (Linux containers): heap=BU clustered=Sch-M cci=S+X. The clustered
# table (split.Big, with nonclustered indexes) takes a schema-modification lock on its first load into an empty
# table, which excludes everything, so it is as serial as X. The columnstore takes S+X, never BU, so
# LoadPlanning.ClusteredColumnstoreLoadsInParallel stays false.
$ErrorActionPreference = 'Stop'

$server = $env:SQL_SERVER
$user = if ($env:SQL_USER) { $env:SQL_USER } else { 'sa' }
$pwd = $env:SQL_PASSWORD
$db = 'bcpjson_lockprobe'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path

# System.Data.SqlClient is not part of PowerShell 7 on Linux; fall back to Microsoft.Data.SqlClient from the SqlServer module.
$ns = 'System.Data.SqlClient'
try { $null = [System.Data.SqlClient.SqlConnection] } catch {
    Write-Host 'System.Data.SqlClient not available; using Microsoft.Data.SqlClient from the SqlServer module.'
    Install-Module SqlServer -Force -Scope CurrentUser -Repository PSGallery | Out-Null
    Import-Module SqlServer
    $ns = 'Microsoft.Data.SqlClient'
}
function New-Sql([string]$type, [object[]]$ArgList) { New-Object -TypeName "$ns.$type" -ArgumentList $ArgList }

& sqlcmd -S $server -U $user -P $pwd -C -b -I -i (Join-Path $here 'schema.sql') -v "DbName=$db" | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'schema.sql failed' }

$cs = "Server=$server;User ID=$user;Password=$pwd;Database=$db;TrustServerCertificate=True;Encrypt=True"

function Get-LockMode([string]$table) {
    $a = New-Sql SqlConnection @($cs); $b = New-Sql SqlConnection @($cs)
    $a.Open(); $b.Open()
    try {
        $idCmd = $b.CreateCommand(); $idCmd.CommandText = "SELECT OBJECT_ID(N'$table')"
        $objectId = [int]$idCmd.ExecuteScalar()   # before the load: nothing here may wait for locks
        $tx = $a.BeginTransaction()
        $cmd = $a.CreateCommand(); $cmd.Transaction = $tx; $cmd.CommandText = 'SELECT @@SPID'
        $spid = [int]$cmd.ExecuteScalar()

        # One row shaped like the table.
        $dt = New-Object System.Data.DataTable
        $ad = New-Sql SqlDataAdapter @("SELECT TOP 0 * FROM $table", $a)
        $ad.SelectCommand.Transaction = $tx
        [void]$ad.Fill($dt)
        $row = $dt.NewRow()
        foreach ($c in $dt.Columns) {
            $row[$c.ColumnName] = switch ($c.DataType.Name) {
                'DateTime' { [datetime]'2024-01-15' }
                'String'   { 'x' }
                'Decimal'  { [decimal]1 }
                default    { [Convert]::ChangeType(1, $c.DataType) }
            }
        }
        $dt.Rows.Add($row)

        $bc = New-Sql SqlBulkCopy @($a, [System.Data.SqlClient.SqlBulkCopyOptions]::TableLock, $tx)
        $bc.DestinationTableName = $table
        $bc.WriteToServer($dt)

        $q = $b.CreateCommand()
        $q.CommandText = "SELECT DISTINCT request_mode FROM sys.dm_tran_locks WHERE request_session_id = $spid AND resource_type = 'OBJECT' AND resource_database_id = DB_ID() AND resource_associated_entity_id = $objectId"
        $q.CommandTimeout = 30
        $modes = @(); $r = $q.ExecuteReader(); while ($r.Read()) { $modes += $r.GetString(0) }; $r.Close()
        $tx.Rollback()
        return ($modes | Sort-Object) -join '+'
    } finally { $a.Close(); $b.Close() }
}

$heap = Get-LockMode 'split.Heap'
$clustered = Get-LockMode 'split.Big'
$cci = Get-LockMode 'split.PartedCci'
"heap=$heap clustered=$clustered cci=$cci"

& sqlcmd -S $server -U $user -P $pwd -C -Q "ALTER DATABASE [$db] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [$db];" | Out-Null

$ok = ($heap -eq 'BU') -and ($clustered -in 'X', 'Sch-M') -and ($cci -eq 'S+X')
if (-not $ok) { Write-Host 'Unexpected lock modes: expected heap=BU clustered=X or Sch-M cci=S+X.'; exit 1 }
