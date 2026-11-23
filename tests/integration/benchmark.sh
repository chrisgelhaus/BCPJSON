#!/usr/bin/env bash
# Benchmark of split exports and the load options for a large table. Manual (benchmark.yml), never part of CI.
#
#   benchmark.sh [rows]     rows defaults to 1000000
#
# Builds BenchSource with bench.History (bigint identity clustered PK, two nonclustered indexes), then times:
#   export JSON and BCP, unsplit and --split-tables bench.History:8
#   import of the split JSON parts into (a) an indexed table, (b) the same with --disable-indexes, (c) a bare heap,
#   (d) a bare heap followed by CREATE CLUSTERED INDEX and both nonclustered indexes (the build times are reported
#   separately from the load: this is "option 2" done by hand)
# and writes a Markdown table (seconds, rows/s) to $GITHUB_STEP_SUMMARY (stdout when unset).
#
# Environment: SQL_SERVER (default localhost), SQL_USER (default sa), SQL_PASSWORD (required),
#              BCPJSON (required, path to the bcpjson executable or bcpjson.dll),
#              WORK (default ${RUNNER_TEMP:-/tmp}/bcpjson-benchmark)
set -euo pipefail

ROWS="${1:-1000000}"
SQL_SERVER="${SQL_SERVER:-localhost}"
SQL_USER="${SQL_USER:-sa}"
: "${SQL_PASSWORD:?SQL_PASSWORD is required}"
: "${BCPJSON:?BCPJSON (path to bcpjson.dll or the bcpjson executable) is required}"
WORK="${WORK:-${RUNNER_TEMP:-/tmp}/bcpjson-benchmark}"
SUMMARY="${GITHUB_STEP_SUMMARY:-/dev/stdout}"
PARTS=8

export SQLCMDPASSWORD="$SQL_PASSWORD"

sql() {
    sqlcmd -S "$SQL_SERVER" -U "$SQL_USER" -C -b -I -f 65001 "$@"
}

SOURCE=(--source-server "$SQL_SERVER" --source-login "$SQL_USER" --source-password "$SQL_PASSWORD")
TARGET=(--target-server "$SQL_SERVER" --target-login "$SQL_USER" --target-password "$SQL_PASSWORD")

bcpjson() {
    if [[ "$BCPJSON" == *.dll ]]; then
        dotnet "$BCPJSON" "$@" --trust-server-certificate
    else
        "$BCPJSON" "$@" --trust-server-certificate
    fi
}

NAMES=()
SECS=()

now() { date +%s.%N; }

# record <label> <start> <end>
record() {
    NAMES+=("$1")
    SECS+=("$(awk -v a="$2" -v b="$3" 'BEGIN { printf "%.1f", b - a }')")
}

# timed <label> <command...>: runs the command (output to a log file, shown if it fails) and records its duration.
timed() {
    local label="$1"; shift
    local log="$WORK/$(echo "$label" | tr -c 'A-Za-z0-9' '_').log" t0 t1
    echo "== $label"
    t0="$(now)"
    if ! "$@" > "$log" 2>&1; then
        cat "$log"
        echo "::error::$label failed"
        exit 1
    fi
    t1="$(now)"
    record "$label" "$t0" "$t1"
    echo "   ${SECS[-1]} s"
}

# timed_sql <label> <database> <statement>: the same for one T-SQL statement.
timed_sql() {
    local label="$1" db="$2" stmt="$3" t0 t1
    echo "== $label"
    t0="$(now)"
    sql -d "$db" -Q "$stmt"
    t1="$(now)"
    record "$label" "$t0" "$t1"
    echo "   ${SECS[-1]} s"
}

table_sql() {
    # bench.History without indexes beyond what the caller adds.
    cat <<'SQL'
CREATE TABLE bench.History (
    Id      bigint IDENTITY(1,1) NOT NULL,
    Stamp   datetime2(7)  NOT NULL,
    Account int           NOT NULL,
    Amount  decimal(18,4) NOT NULL,
    Memo    nvarchar(80)  NULL
SQL
}

create_db() {
    local db="$1" indexed="$2" pk=""
    [ "$indexed" = yes ] && pk=", CONSTRAINT PK_History PRIMARY KEY CLUSTERED (Id)"
    sql -Q "IF DB_ID(N'$db') IS NOT NULL BEGIN ALTER DATABASE [$db] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [$db]; END; CREATE DATABASE [$db]; ALTER DATABASE [$db] SET RECOVERY SIMPLE;"
    sql -d "$db" -Q "EXEC (N'CREATE SCHEMA bench'); $(table_sql) $pk);"
    if [ "$indexed" = yes ]; then
        sql -d "$db" -Q "CREATE INDEX IX_History_Stamp ON bench.History (Stamp); CREATE INDEX IX_History_Account ON bench.History (Account);"
    fi
}

row_count() { sql -d "$1" -h -1 -W -Q "SET NOCOUNT ON; SELECT COUNT_BIG(*) FROM bench.History" | tr -d '[:space:]'; }

check_rows() {
    local n
    n="$(row_count "$1")"
    [ "$n" = "$ROWS" ] || { echo "::error::$1 holds $n rows, expected $ROWS"; exit 1; }
}

rm -rf "$WORK"
mkdir -p "$WORK"

echo "Benchmark with $ROWS rows, $PARTS parts"

# ---- source -------------------------------------------------------------------------------------------------------
create_db BenchSource yes
sql -d BenchSource -Q "
SET NOCOUNT ON;
INSERT INTO bench.History (Stamp, Account, Amount, Memo)
SELECT TOP ($ROWS)
       DATEADD(SECOND, n, CONVERT(datetime2(7), '2020-01-01')),
       CONVERT(int, (n * 7919) % 100003),
       CONVERT(decimal(18,4), (n % 1000000) / 100.0),
       CONCAT(N'memo ', n)
FROM (SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
      FROM sys.all_objects a CROSS JOIN sys.all_objects b CROSS JOIN sys.all_objects c) AS t
ORDER BY n;
UPDATE STATISTICS bench.History WITH FULLSCAN;"
check_rows BenchSource

# ---- exports ------------------------------------------------------------------------------------------------------
EXPORT=(export "${SOURCE[@]}" --source-database BenchSource --include-tables bench.History)
timed "export JSON unsplit"  bcpjson "${EXPORT[@]}" --file-format json --export-path "$WORK/json-whole"
timed "export JSON split $PARTS" bcpjson "${EXPORT[@]}" --file-format json --export-path "$WORK/json-split" --split-tables "bench.History:$PARTS"
timed "export BCP unsplit"   bcpjson "${EXPORT[@]}" --file-format bcp  --export-path "$WORK/bcp-whole"
timed "export BCP split $PARTS"  bcpjson "${EXPORT[@]}" --file-format bcp  --export-path "$WORK/bcp-split" --split-tables "bench.History:$PARTS"

# ---- imports of the split JSON parts ------------------------------------------------------------------------------
IMPORT=(import "${TARGET[@]}" --file-format json --source-path "$WORK/json-split" --source-file 'bench.History.part*.json')

create_db BenchA yes
timed "import indexed" bcpjson "${IMPORT[@]}" --target-database BenchA
check_rows BenchA

create_db BenchB yes
timed "import indexed, --disable-indexes" bcpjson "${IMPORT[@]}" --target-database BenchB --disable-indexes
check_rows BenchB

create_db BenchC no
timed "import bare heap" bcpjson "${IMPORT[@]}" --target-database BenchC
check_rows BenchC

create_db BenchD no
timed "import bare heap (for option 2)" bcpjson "${IMPORT[@]}" --target-database BenchD
check_rows BenchD
LOAD_D="${SECS[-1]}"
START_INDEXES="${#SECS[@]}"
timed_sql "  CREATE CLUSTERED INDEX (PK)" BenchD "CREATE UNIQUE CLUSTERED INDEX PK_History ON bench.History (Id);"
timed_sql "  CREATE INDEX IX_History_Stamp" BenchD "CREATE INDEX IX_History_Stamp ON bench.History (Stamp);"
timed_sql "  CREATE INDEX IX_History_Account" BenchD "CREATE INDEX IX_History_Account ON bench.History (Account);"
BUILD_D="$(printf '%s\n' "${SECS[@]:$START_INDEXES}" | awk '{ s += $1 } END { printf "%.1f", s }')"
NAMES+=("option 2 total (heap load + index builds)")
SECS+=("$(awk -v a="$LOAD_D" -v b="$BUILD_D" 'BEGIN { printf "%.1f", a + b }')")

# ---- summary ------------------------------------------------------------------------------------------------------
{
    echo "### bcpJson split benchmark"
    echo
    echo "$ROWS rows in bench.History (bigint identity clustered PK, two nonclustered indexes), $PARTS parts, SQL Server $(sql -h -1 -W -Q "SET NOCOUNT ON; SELECT CONVERT(varchar(40), SERVERPROPERTY('ProductVersion'))" | tr -d '[:space:]'), $(nproc) cores."
    echo
    echo "| Step | Seconds | Rows/s |"
    echo "|---|---:|---:|"
    for i in "${!NAMES[@]}"; do
        case "${NAMES[$i]}" in
            "  CREATE"*) rate="" ;;
            *) rate="$(awk -v r="$ROWS" -v s="${SECS[$i]}" 'BEGIN { if (s > 0) printf "%d", r / s; else print "n/a" }')" ;;
        esac
        echo "| ${NAMES[$i]} | ${SECS[$i]} | $rate |"
    done
} | tee -a "$SUMMARY"
