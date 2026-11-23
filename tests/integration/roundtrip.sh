#!/usr/bin/env bash
# Round-trip integration tests for bcpjson against a real SQL Server.
#
#   roundtrip.sh setup   create BcpJsonSource (schema + data) and the empty target databases
#   roundtrip.sh json    export JSON -> import JSON -> export JSON, diff with the source export
#   roundtrip.sh bcp     export BCP  -> import BCP  -> export JSON, diff with the source export (schema "plain")
#   roundtrip.sh copy    copy --snapshot --foreign-keys --triggers -> export JSON and the definitions of indexes,
#                        foreign keys, check constraints and triggers, diff exactly;
#                        rerun with --drop-target;
#                        copy --no-indexes --no-check-constraints -> bare tables, rows identical (order ignored)
#   roundtrip.sh snapshot  export --snapshot (JSON and BCP), diff with the source export; no snapshot may be left behind
#   roundtrip.sh consistency  export --snapshot while a writer moves value between two tables: totals must be exact;
#                          without --snapshot the same export must drift (proves the check works)
#   roundtrip.sh resilience   manifest + verify (and tampering caught), export --resume after a simulated crash,
#                          connection retry while the database comes online
#   roundtrip.sh compression  export --compress gzip (JSON and BCP) -> verify -> import the .gz files (small and
#                          single-transaction batches) -> export JSON, diff with the source export; --resume rules
#   roundtrip.sh split     CONVERT styles of split boundaries; export --split-tables (JSON from a snapshot, BCP): part
#                          counts, verify, joined parts equal the unsplit export; --resume redoes a missing part;
#                          import of the parts (load modes, --disable-indexes and a failing unique rebuild, another
#                          clustered key, BCP parts, truncate once per table)
#
# Table locking: JSON import and copy use the default (TABLOCK); BCP import uses --no-table-lock.
#
# Environment:
#   SQL_SERVER    (default localhost)
#   SQL_USER      (default sa)
#   SQL_PASSWORD  required
#   BCPJSON       required: path to bcpjson.dll (run with dotnet) or a published bcpjson executable
#   WORK          (default ${RUNNER_TEMP:-/tmp}/bcpjson-roundtrip) output folder, kept for inspection
set -euo pipefail

SQL_SERVER="${SQL_SERVER:-localhost}"
SQL_USER="${SQL_USER:-sa}"
: "${SQL_PASSWORD:?SQL_PASSWORD is required}"
: "${BCPJSON:?BCPJSON (path to bcpjson.dll or the bcpjson executable) is required}"
WORK="${WORK:-${RUNNER_TEMP:-/tmp}/bcpjson-roundtrip}"
HERE="$(cd "$(dirname "$0")" && pwd)"
MANIFEST=bcpjson-manifest.json

export SQLCMDPASSWORD="$SQL_PASSWORD"

sql() {
    # -I: QUOTED_IDENTIFIER ON (sqlcmd defaults to OFF), required for filtered/XML/spatial indexes.
    sqlcmd -S "$SQL_SERVER" -U "$SQL_USER" -C -b -I -f 65001 "$@"
}

SOURCE=(--source-server "$SQL_SERVER" --source-login "$SQL_USER" --source-password "$SQL_PASSWORD")
TARGET=(--target-server "$SQL_SERVER" --target-login "$SQL_USER" --target-password "$SQL_PASSWORD")

# Runs bcpjson; the command line is echoed with the password masked. A non-zero exit fails the script.
# verify needs no server, so it gets no --trust-server-certificate.
bcpjson() {
    local shown="$*" extra=(--trust-server-certificate)
    echo "+ bcpjson ${shown//"$SQL_PASSWORD"/***}"
    [ "$1" = verify ] && extra=()
    if [[ "$BCPJSON" == *.dll ]]; then
        dotnet "$BCPJSON" "$@" "${extra[@]}"
    else
        "$BCPJSON" "$@" "${extra[@]}"
    fi
}

# Exports every table of a database (or the given schemas) to JSON in a folder.
export_json() {
    local db="$1" dir="$2"; shift 2
    rm -rf "$dir"
    bcpjson export "${SOURCE[@]}" --source-database "$db" --file-format json --export-path "$dir" "$@"
}

# Exact, line-by-line comparison of two JSON export folders.
compare_exact() {
    local expected="$1" actual="$2"
    echo "Comparing $actual with $expected"
    # The manifest records when and from where each export ran, so it always differs.
    if ! diff -r -u -x "$MANIFEST" "$expected" "$actual"; then
        echo "::error::Round trip changed the data: $actual differs from $expected"
        return 1
    fi
    echo "Identical: $(data_file_count "$expected") files."
}

# Files in an export folder, not counting the manifest.
data_file_count() {
    ls "$1" | grep -vc "^$MANIFEST\$"
}

# Comparison that ignores row order (used where the target has no primary key to sort by).
compare_unordered() {
    local expected="$1" actual="$2" status=0 f
    echo "Comparing $actual with $expected (row order ignored)"
    for f in "$expected"/*.json; do
        local name; name="$(basename "$f")"
        [ "$name" = "$MANIFEST" ] && continue
        if [ ! -f "$actual/$name" ]; then
            echo "::error::Missing $name in $actual"
            status=1
            continue
        fi
        if ! diff -u <(normalize_rows "$f") <(normalize_rows "$actual/$name"); then
            echo "::error::$name differs (ignoring row order)"
            status=1
        fi
    done
    [ "$status" -eq 0 ] && echo "Identical (unordered): $(data_file_count "$expected") files."
    return "$status"
}

# One row object per line without the array punctuation, sorted.
normalize_rows() {
    sed -e 's/^\[//' -e 's/^,//' -e 's/\]$//' "$1" | LC_ALL=C sort
}

# Fails if a database snapshot was left behind (--snapshot must always drop it).
assert_no_snapshots() {
    local left
    left="$(sql -h -1 -W -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM sys.databases WHERE source_database_id IS NOT NULL;" | tr -d '[:space:]')"
    if [ "$left" != "0" ]; then
        echo "::error::$left database snapshot(s) left behind"
        sql -Q "SELECT name, create_date FROM sys.databases WHERE source_database_id IS NOT NULL;"
        return 1
    fi
    echo "No database snapshots left behind."
}

# plain.Decimals as SQL Server's own text (all 38 digits), hashed: checks the values without going through bcpjson's export.
decimals_hash() {
    sql -d "$1" -h -1 -W -Q "SET NOCOUNT ON; SELECT CONCAT(COUNT(*), ' rows, sha256 ', CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', STRING_AGG(CAST(CONCAT(Id, '|', Whole, '|', Mixed, '|', Half, '|', Fraction, '|', Small) AS NVARCHAR(MAX)), ';') WITHIN GROUP (ORDER BY Id)), 2)) FROM plain.Decimals;" | tr -d '\r'
}

# Fails unless a database holds exactly the source's 38-digit decimals.
compare_decimals() {
    local expected actual
    expected="$(decimals_hash BcpJsonSource)"
    actual="$(decimals_hash "$1")"
    if [ "$expected" != "$actual" ]; then
        echo "::error::plain.Decimals in $1 ($actual) differs from the source ($expected)"
        sql -d "$1" -Q "SELECT * FROM plain.Decimals ORDER BY Id;"
        return 1
    fi
    echo "plain.Decimals identical to the source: $actual"
}

# Empty database with the test schemas but no tables (copy creates them). copy scripts the indexes of partitioned
# tables with their partition scheme, so the scheme of schema.sql must exist too.
create_empty_db() {
    local db="$1"
    sql -Q "IF DB_ID(N'$db') IS NOT NULL BEGIN ALTER DATABASE [$db] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [$db]; END; CREATE DATABASE [$db];"
    sql -d "$db" -Q "EXEC (N'CREATE SCHEMA rt'); EXEC (N'CREATE SCHEMA plain'); EXEC (N'CREATE SCHEMA split');
        CREATE PARTITION FUNCTION pf_SplitMonth (DATETIME2(7)) AS RANGE RIGHT
            FOR VALUES ('2026-01-01', '2026-02-01', '2026-03-01', '2026-04-01', '2026-05-01', '2026-06-01');
        CREATE PARTITION SCHEME ps_SplitMonth AS PARTITION pf_SplitMonth ALL TO ([PRIMARY]);"
}

# Object definitions compared between source and copy; each has a <kind>.sql returning one line per object.
DEFINITION_KINDS=(indexes foreign_keys check_constraints triggers)

# Writes the <kind> definitions of a database as JSON (one line per object) to <dir>/<kind>.json.
definitions() {
    local kind="$1" db="$2" dir="$3"
    rm -rf "$dir"
    bcpjson export "${SOURCE[@]}" --source-database "$db" --file-format json --export-path "$dir" \
        --output-name "$kind" --source-query "$(cat "$HERE/$kind.sql")"
}

# Every definition kind of <db> must match BcpJsonSource exactly.
compare_definitions() {
    local db="$1" label="$2" kind
    for kind in "${DEFINITION_KINDS[@]}"; do
        [ -f "$WORK/def-source-$kind/$kind.json" ] || definitions "$kind" BcpJsonSource "$WORK/def-source-$kind"
        definitions "$kind" "$db" "$WORK/def-$label-$kind"
        echo "Source has $(grep -c '^[[,]{' "$WORK/def-source-$kind/$kind.json") $kind."
        compare_exact "$WORK/def-source-$kind" "$WORK/def-$label-$kind"
    done
}

# Copies the plain.* tables of the source JSON export into a folder (the BCP round trips cover schema "plain" only).
source_json_plain() {
    mkdir -p "$WORK/source-json-plain"
    rm -f "$WORK/source-json-plain"/*
    cp "$WORK/source-json"/plain.*.json "$WORK/source-json-plain/"
}

# Fails unless every data file in a folder is a valid gzip file with the given extension and nothing else is compressed.
assert_gzip_export() {
    local dir="$1" ext="$2" f count=0
    for f in "$dir"/*; do
        case "$(basename "$f")" in
            "$MANIFEST"|*-c.fmt|*.log) continue ;;
            *"$ext.gz") gzip -t "$f" || { echo "::error::$f is not valid gzip"; return 1; }; count=$((count + 1)) ;;
            *) echo "::error::Unexpected file in a compressed export: $f"; return 1 ;;
        esac
    done
    [ "$(jq -r '.compression' "$dir/$MANIFEST")" = "gzip" ] || { echo "::error::Manifest does not record gzip"; return 1; }
    echo "$count valid $ext.gz files."
}

# Number of part files <table>.partNNNN.<ext> in a folder.
part_count() {
    local dir="$1" table="$2" ext="$3"
    ls "$dir" | grep -c "^$table\.part[0-9][0-9][0-9][0-9]\.$ext\$" || true
}

# The JSON parts of a split table joined into one array, as the unsplit export writes it: each part's "]" line is
# dropped, a following part's leading "[" becomes ",", and empty parts ("[]") are skipped.
join_parts() {
    local dir="$1" table="$2"
    LC_ALL=C awk 'FNR == 1 && $0 == "[]" { next }
                  $0 == "]" { next }
                  { printf "%s%s", (rows++ ? "\n," : "["), substr($0, 2) }
                  END { printf "%s", (rows ? "\n]" : "[]") }' "$dir/$table".part[0-9][0-9][0-9][0-9].json
}

create_target_db() {
    local db="$1"
    sql -Q "IF DB_ID(N'$db') IS NOT NULL BEGIN ALTER DATABASE [$db] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [$db]; END"
    sql -i "$HERE/schema.sql" -v DbName="$db"
}

case "${1:-}" in
    setup)
        command -v sqlcmd >/dev/null || { echo "sqlcmd not found on PATH"; exit 1; }
        command -v bcp >/dev/null || { echo "bcp not found on PATH"; exit 1; }
        sql -Q "SELECT @@VERSION"
        create_target_db BcpJsonSource
        sql -i "$HERE/data.sql" -v DbName=BcpJsonSource
        sql -d BcpJsonSource -Q "SELECT s.name + '.' + t.name AS [table], SUM(p.rows) AS [rows] FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id JOIN sys.partitions p ON p.object_id = t.object_id AND p.index_id IN (0, 1) GROUP BY s.name, t.name ORDER BY 1"
        mkdir -p "$WORK"
        export_json BcpJsonSource "$WORK/source-json"
        ls -l "$WORK/source-json"
        # 38-digit decimals must be exported with every digit (System.Decimal holds only 28-29).
        for v in '"Whole":99999999999999999999999999999999999999' '"Whole":-99999999999999999999999999999999999999' \
                 '"Fraction":0.00000000000000000000000000000000000001' '"Half":1234567890123456789.0123456789012345678'; do
            grep -qF "$v" "$WORK/source-json/plain.Decimals.json" || { echo "::error::plain.Decimals.json lacks $v"; exit 1; }
        done
        echo "38-digit decimals exported exactly."
        ;;

    json)
        create_target_db BcpJsonJson
        bcpjson import "${TARGET[@]}" --target-database BcpJsonJson --file-format json --max-tasks 2 --source-path "$WORK/source-json" --source-file '*.json' | tee "$WORK/json-import.log"
        # Tables with a primary key load with an order hint (the manifest records the export's sort key); a wrong hint fails the load.
        grep -q "rt.OrderedKey: Rows are in clustered index order" "$WORK/json-import.log" || { echo "::error::rt.OrderedKey was loaded without an order hint"; exit 1; }
        if grep -q "plain.Spatial: Rows are in clustered index order" "$WORK/json-import.log"; then echo "::error::Staged CLR table got an order hint"; exit 1; fi
        # Documented behaviour: bulk loads don't check constraints, so SQL Server marks the target's foreign keys untrusted.
        echo "After import: $(sql -d BcpJsonJson -h -1 -W -Q "SET NOCOUNT ON; SELECT CONCAT(SUM(CAST(is_not_trusted AS INT)), ' of ', COUNT(*)) FROM sys.foreign_keys;" | tr -d '\r') foreign keys are not trusted."
        compare_decimals BcpJsonJson
        export_json BcpJsonJson "$WORK/json-roundtrip"
        compare_exact "$WORK/source-json" "$WORK/json-roundtrip"
        grep -q '"Geo":"SRID=4269;POINT (-77.03 38.89)"' "$WORK/source-json/plain.Spatial.json" || { echo "::error::SRID 4269 not exported"; cat "$WORK/source-json/plain.Spatial.json"; exit 1; }

        # A target clustered on other columns gets no hint and still loads.
        sql -d BcpJsonJson -Q "SELECT TOP (0) * INTO rt.OtherOrder FROM rt.OrderedKey; CREATE CLUSTERED INDEX CX_OtherOrder ON rt.OtherOrder (Seq);"
        bcpjson import "${TARGET[@]}" --target-database BcpJsonJson --file-format json --source-path "$WORK/source-json" --source-file rt.OrderedKey.json --target-table rt.OtherOrder | tee "$WORK/json-other-order.log"
        if grep -q "Rows are in clustered index order" "$WORK/json-other-order.log"; then echo "::error::Order hint on a different clustered key"; exit 1; fi
        [ "$(sql -d BcpJsonJson -h -1 -W -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM rt.OtherOrder;" | tr -d '[:space:]')" = "7" ] || { echo "::error::rt.OtherOrder rows"; exit 1; }
        ;;

    bcp)
        create_target_db BcpJsonBcp
        rm -rf "$WORK/source-bcp"
        bcpjson export "${SOURCE[@]}" --source-database BcpJsonSource --file-format bcp --include-schemas plain --export-path "$WORK/source-bcp"
        ls -l "$WORK/source-bcp"
        bcpjson verify --path "$WORK/source-bcp"
        # Table locking is the default; this path covers --no-table-lock (JSON import and copy cover TABLOCK).
        bcpjson import "${TARGET[@]}" --target-database BcpJsonBcp --file-format bcp --no-table-lock --source-path "$WORK/source-bcp" --source-file '*.bcp'
        compare_decimals BcpJsonBcp
        export_json BcpJsonBcp "$WORK/bcp-roundtrip" --include-schemas plain
        source_json_plain
        compare_exact "$WORK/source-json-plain" "$WORK/bcp-roundtrip"
        ;;

    copy)
        create_empty_db BcpJsonCopy
        # Copies from a point-in-time snapshot, with the default table lock (including the bcp fallback for CLR types),
        # the default index and check constraint recreation, --foreign-keys and --triggers.
        bcpjson copy "${SOURCE[@]}" --source-database BcpJsonSource "${TARGET[@]}" --target-database BcpJsonCopy --snapshot --foreign-keys --triggers
        assert_no_snapshots
        compare_decimals BcpJsonCopy
        export_json BcpJsonCopy "$WORK/copy-roundtrip"
        # Primary keys are recreated, so the copy exports in the same order as the source: compare exactly.
        # This includes rt.ChildAudit, which would gain rows if a trigger had fired during the copy.
        compare_exact "$WORK/source-json" "$WORK/copy-roundtrip"
        # Every index, foreign key, check constraint and trigger must match the source definition.
        compare_definitions BcpJsonCopy copy

        # Re-running into the same database with --drop-target must work although the tables now reference each other.
        bcpjson copy "${SOURCE[@]}" --source-database BcpJsonSource "${TARGET[@]}" --target-database BcpJsonCopy --snapshot --foreign-keys --triggers --drop-target
        export_json BcpJsonCopy "$WORK/copy-rerun"
        compare_exact "$WORK/source-json" "$WORK/copy-rerun"
        compare_definitions BcpJsonCopy rerun

        # --no-indexes --no-check-constraints, without --foreign-keys/--triggers: bare tables, same rows.
        create_empty_db BcpJsonCopyNoIdx
        bcpjson copy "${SOURCE[@]}" --source-database BcpJsonSource "${TARGET[@]}" --target-database BcpJsonCopyNoIdx --no-indexes --no-check-constraints
        left="$(sql -d BcpJsonCopyNoIdx -h -1 -W -Q "SET NOCOUNT ON; SELECT (SELECT COUNT(*) FROM sys.indexes i JOIN sys.tables t ON t.object_id = i.object_id WHERE i.type > 0 AND t.is_ms_shipped = 0) + (SELECT COUNT(*) FROM sys.foreign_keys) + (SELECT COUNT(*) FROM sys.check_constraints) + (SELECT COUNT(*) FROM sys.triggers WHERE parent_class = 1);" | tr -d '[:space:]')"
        [ "$left" = "0" ] || { echo "::error::$left indexes, foreign keys, check constraints or triggers were created"; exit 1; }
        echo "--no-indexes --no-check-constraints: no indexes, foreign keys, check constraints or triggers created."
        compare_decimals BcpJsonCopyNoIdx
        export_json BcpJsonCopyNoIdx "$WORK/copy-noidx"
        compare_unordered "$WORK/source-json" "$WORK/copy-noidx"
        ;;

    snapshot)
        # Export from a database snapshot (JSON and BCP); output must match the live export and the snapshot must be gone.
        export_json BcpJsonSource "$WORK/snapshot-json" --snapshot
        assert_no_snapshots
        compare_exact "$WORK/source-json" "$WORK/snapshot-json"
        rm -rf "$WORK/snapshot-bcp"
        bcpjson export "${SOURCE[@]}" --source-database BcpJsonSource --file-format bcp --include-schemas plain --export-path "$WORK/snapshot-bcp" --snapshot
        assert_no_snapshots
        ls -l "$WORK/snapshot-bcp"
        [ -s "$WORK/snapshot-bcp/plain.Numbers.bcp" ] || { echo "::error::Snapshot BCP export wrote no data"; exit 1; }
        # The manifest names the source database, not the temporary snapshot.
        jq -e '.snapshot == true and .database == "BcpJsonSource" and .completed != null' "$WORK/snapshot-json/$MANIFEST" > /dev/null \
            || { echo "::error::Snapshot export manifest is wrong"; cat "$WORK/snapshot-json/$MANIFEST"; exit 1; }
        ;;

    consistency)
        # --snapshot must give a single point in time while the source is being written to (see consistency.sql).
        command -v jq >/dev/null || { echo "jq not found on PATH"; exit 1; }
        sql -i "$HERE/consistency.sql"
        live() { sql -d BcpJsonLive -h -1 -W -Q "SET NOCOUNT ON; $1" | tr -d '[:space:]'; }
        transfers() { live "SELECT Transfers FROM inv.Control WHERE Id = 1;"; }
        export_total() { jq -s '[.[0][].Value, .[1][].Value] | add' "$1/inv.A.json" "$1/inv.B.json"; }
        expected="$(live "SELECT SUM(Value) FROM (SELECT Value FROM inv.A UNION ALL SELECT Value FROM inv.B) AS v;")"
        echo "Invariant: SUM(inv.A.Value) + SUM(inv.B.Value) = $expected at every committed point in time."

        sql -d BcpJsonLive -i "$HERE/writer.sql" > "$WORK/writer.log" 2>&1 &
        writer=$!
        stop_writer() { sql -d BcpJsonLive -Q "UPDATE inv.Control SET [Stop] = 1 WHERE Id = 1;" > /dev/null; wait "$writer"; }
        trap 'stop_writer > /dev/null 2>&1 || true' EXIT
        for _ in $(seq 1 60); do [ "$(transfers)" -gt 100 ] && break; sleep 1; done
        echo "Writer running: $(transfers) transfers committed so far."

        # 1. With --snapshot, every export adds up exactly, although transfers commit while it runs.
        #    --max-tasks 1 reads the tables one after another, widening the gap between them.
        for run in 1 2; do
            before="$(transfers)"
            export_json BcpJsonLive "$WORK/live-snapshot-$run" --include-schemas inv --max-tasks 1 --snapshot
            after="$(transfers)"
            total="$(export_total "$WORK/live-snapshot-$run")"
            echo "export --snapshot #$run: total $total (expected $expected); $((after - before)) transfers committed during the export."
            [ "$after" -gt "$before" ] || { echo "::error::No transfers were committed during the export, so it proves nothing"; exit 1; }
            [ "$total" = "$expected" ] || { echo "::error::export --snapshot #$run is inconsistent: total $total, expected $expected"; exit 1; }
        done
        assert_no_snapshots

        # 2. Control: the same export without --snapshot must be caught drifting, which shows the check can fail.
        drift=""
        for attempt in 1 2 3; do
            if ! export_json BcpJsonLive "$WORK/live-nosnapshot-$attempt" --include-schemas inv --max-tasks 1; then
                echo "Export without --snapshot failed (e.g. deadlock victim); retrying."
                continue
            fi
            total="$(export_total "$WORK/live-nosnapshot-$attempt")"
            echo "export without --snapshot, attempt $attempt: total $total (expected $expected)."
            if [ "$total" != "$expected" ]; then drift="$((total - expected))"; break; fi
        done
        [ -n "$drift" ] || { echo "::error::Control failed: exports without --snapshot never drifted, so this test cannot detect inconsistency"; exit 1; }

        trap - EXIT
        stop_writer
        tail -n 2 "$WORK/writer.log"
        echo "Without --snapshot the export was off by $drift; both --snapshot exports were exact."
        ;;

    resilience)
        command -v jq >/dev/null || { echo "jq not found on PATH"; exit 1; }
        src="$WORK/source-json"
        tables="$(data_file_count "$src")"

        # 1. The setup export wrote a complete manifest listing every table, and verify accepts it.
        jq -e --argjson n "$tables" '.completed != null and .format == "json" and .database == "BcpJsonSource" and (.tables | length) == $n and all(.tables[]; .files[0].sha256 | length == 64)' \
            "$src/$MANIFEST" > /dev/null || { echo "::error::Manifest of the source export is wrong"; cat "$src/$MANIFEST"; exit 1; }
        bcpjson verify --path "$src"

        # 2. verify catches a same-size edit (only the hash differs) and a missing file.
        rm -rf "$WORK/tampered"
        cp -r "$src" "$WORK/tampered"
        sed -i 's/"Code":"AAA"/"Code":"AAB"/' "$WORK/tampered/rt.Parent.json"
        grep -q '"Code":"AAB"' "$WORK/tampered/rt.Parent.json" || { echo "::error::Tampering did not change rt.Parent.json"; exit 1; }
        rm "$WORK/tampered/plain.Ref.json"
        if bcpjson verify --path "$WORK/tampered" > "$WORK/verify-tampered.log" 2>&1; then
            cat "$WORK/verify-tampered.log"; echo "::error::verify accepted a tampered export"; exit 1
        fi
        cat "$WORK/verify-tampered.log"
        grep -q "rt.Parent.json: SHA-256" "$WORK/verify-tampered.log" && grep -q "plain.Ref.json: missing" "$WORK/verify-tampered.log" \
            || { echo "::error::verify did not name both damaged files"; exit 1; }

        # 3. --resume after a simulated crash: two tables missing from the manifest, one of them half-written.
        res="$WORK/resume"
        rm -rf "$res"
        cp -r "$src" "$res"
        rm "$res/plain.Ref.json" "$res/rt.Child.json"
        printf '[{"Id":1' > "$res/rt.Child.json.partial"
        jq '.completed = null | .tables |= map(select(.name != "plain.Ref" and .name != "rt.Child"))' "$src/$MANIFEST" > "$res/$MANIFEST"
        touch -d '2001-01-01' "$res/rt.Parent.json"
        if bcpjson verify --path "$res" > /dev/null 2>&1; then echo "::error::verify accepted an incomplete export"; exit 1; fi
        bcpjson export "${SOURCE[@]}" --source-database BcpJsonSource --file-format json --export-path "$res" --resume | tee "$WORK/resume.log"
        grep -qF -- "--resume: $((tables - 2)) of $tables table(s) already exported, 2 to go." "$WORK/resume.log" \
            || { echo "::error::--resume did not skip exactly the completed tables"; exit 1; }
        [ "$(date -r "$res/rt.Parent.json" +%Y)" = "2001" ] || { echo "::error::--resume rewrote a completed table"; exit 1; }
        [ -z "$(find "$res" -name '*.partial')" ] || { echo "::error::.partial files left behind"; ls -l "$res"; exit 1; }
        compare_exact "$src" "$res"
        bcpjson verify --path "$res"
        # Resuming into an export of another kind is refused.
        if bcpjson export "${SOURCE[@]}" --source-database BcpJsonSource --file-format bcp --export-path "$res" --resume > "$WORK/resume-conflict.log" 2>&1; then
            echo "::error::--resume continued a JSON export as BCP"; exit 1
        fi
        grep -q "Cannot resume: the existing export is json, not bcp" "$WORK/resume-conflict.log" || { cat "$WORK/resume-conflict.log"; exit 1; }

        # 4. Connection retry: the database only appears (renamed into place, with its table) 20 s after the export
        #    starts; until then opening it fails with error 4060, which is transient. SqlClient itself retries a login
        #    once after 10 s (ConnectRetryCount=1), so 20 s ensures bcpjson's own retries are what make it succeed.
        sql -Q "DROP DATABASE IF EXISTS BcpJsonLate; DROP DATABASE IF EXISTS BcpJsonLateStaging; CREATE DATABASE BcpJsonLateStaging;"
        sql -d BcpJsonLateStaging -Q "CREATE TABLE dbo.T (Id INT NOT NULL PRIMARY KEY); INSERT dbo.T VALUES (1), (2);"
        if bcpjson export "${SOURCE[@]}" --source-database BcpJsonLate --file-format json --export-path "$WORK/late-noretry" --retries 0 > "$WORK/late-noretry.log" 2>&1; then
            echo "::error::Control failed: the export succeeded although the database does not exist yet"; exit 1
        fi
        echo "Without retries the export fails at once: $(grep -m1 'Cannot connect' "$WORK/late-noretry.log")"
        (sleep 20; sql -Q "ALTER DATABASE BcpJsonLateStaging MODIFY NAME = BcpJsonLate;" > /dev/null) &
        renamer=$!
        bcpjson export "${SOURCE[@]}" --source-database BcpJsonLate --file-format json --export-path "$WORK/late" --retries 8 | tee "$WORK/late.log"
        wait "$renamer"
        grep -q "Connection failed" "$WORK/late.log" || { echo "::error::No connection retry was logged"; exit 1; }
        [ "$(jq '.tables[0].rows' "$WORK/late/$MANIFEST")" = "2" ] || { echo "::error::Retried export has the wrong rows"; cat "$WORK/late/$MANIFEST"; exit 1; }
        bcpjson verify --path "$WORK/late"
        sql -Q "DROP DATABASE BcpJsonLate;"
        ;;

    compression)
        command -v jq >/dev/null || { echo "jq not found on PATH"; exit 1; }
        src="$WORK/source-json"
        gz="$WORK/gz-json"

        # 1. JSON with gzip: every data file is <table>.json.gz, verify passes, and the content is the plain export's.
        export_json BcpJsonSource "$gz" --compress gzip
        ls -l "$gz"
        assert_gzip_export "$gz" .json
        bcpjson verify --path "$gz"
        echo "Plain $(du -sb --exclude="$MANIFEST" "$src" | cut -f1) bytes, gzip $(du -sb --exclude="$MANIFEST" "$gz" | cut -f1) bytes."
        rm -rf "$WORK/gz-json-unzipped"
        mkdir -p "$WORK/gz-json-unzipped"
        for f in "$gz"/*.json.gz; do
            zcat "$f" > "$WORK/gz-json-unzipped/$(basename "$f" .gz)"
        done
        compare_exact "$src" "$WORK/gz-json-unzipped"

        # 2. Import the .gz files directly (streamed), in small batches so every table spans several commits.
        create_target_db BcpJsonGz
        bcpjson import "${TARGET[@]}" --target-database BcpJsonGz --file-format json --max-tasks 2 --batch-size 1000 --source-path "$gz" --source-file '*.json.gz'
        compare_decimals BcpJsonGz
        export_json BcpJsonGz "$WORK/gz-json-roundtrip"
        compare_exact "$src" "$WORK/gz-json-roundtrip"

        # 3. --batch-size 0: each file is one transaction.
        create_target_db BcpJsonGzOne
        bcpjson import "${TARGET[@]}" --target-database BcpJsonGzOne --file-format json --batch-size 0 --source-path "$gz" --source-file 'plain.*.json.gz'
        source_json_plain
        export_json BcpJsonGzOne "$WORK/gz-one-roundtrip" --include-schemas plain
        compare_exact "$WORK/source-json-plain" "$WORK/gz-one-roundtrip"

        # 4. BCP with gzip (compressed after bcp writes the file): format files stay plain, no .bcp is left behind;
        #    import decompresses to a temporary file.
        rm -rf "$WORK/gz-bcp"
        bcpjson export "${SOURCE[@]}" --source-database BcpJsonSource --file-format bcp --include-schemas plain --export-path "$WORK/gz-bcp" --compress gzip --compression-level smallest
        ls -l "$WORK/gz-bcp"
        assert_gzip_export "$WORK/gz-bcp" .bcp
        bcpjson verify --path "$WORK/gz-bcp"
        create_target_db BcpJsonBcpGz
        bcpjson import "${TARGET[@]}" --target-database BcpJsonBcpGz --file-format bcp --batch-size 0 --source-path "$WORK/gz-bcp" --source-file '*.bcp.gz'
        compare_decimals BcpJsonBcpGz
        export_json BcpJsonBcpGz "$WORK/gz-bcp-roundtrip" --include-schemas plain
        compare_exact "$WORK/source-json-plain" "$WORK/gz-bcp-roundtrip"

        # 5. --resume continues a gzip export (one table missing) and refuses to continue it uncompressed.
        res="$WORK/gz-resume"
        rm -rf "$res"
        cp -r "$gz" "$res"
        rm "$res/plain.Ref.json.gz"
        jq '.completed = null | .tables |= map(select(.name != "plain.Ref"))' "$gz/$MANIFEST" > "$res/$MANIFEST"
        if bcpjson export "${SOURCE[@]}" --source-database BcpJsonSource --file-format json --export-path "$res" --resume > "$WORK/gz-resume-conflict.log" 2>&1; then
            echo "::error::--resume continued a gzip export uncompressed"; exit 1
        fi
        grep -q "Cannot resume: the existing export uses --compress gzip, not none" "$WORK/gz-resume-conflict.log" || { cat "$WORK/gz-resume-conflict.log"; exit 1; }
        bcpjson export "${SOURCE[@]}" --source-database BcpJsonSource --file-format json --export-path "$res" --resume --compress gzip | tee "$WORK/gz-resume.log"
        grep -q "1 to go" "$WORK/gz-resume.log" || { echo "::error::--resume did not export exactly the missing table"; exit 1; }
        bcpjson verify --path "$res"
        cmp <(zcat "$gz/plain.Ref.json.gz") <(zcat "$res/plain.Ref.json.gz") || { echo "::error::Resumed plain.Ref differs"; exit 1; }
        ;;

    split)
        command -v jq >/dev/null || { echo "jq not found on PATH"; exit 1; }
        src="$WORK/source-json"

        # 1. Boundaries travel as text through CONVERT styles; each split column type must round-trip exactly.
        sql -d BcpJsonSource -i "$HERE/convert_styles.sql"

        # 2. JSON, every split table cut into 8 parts where it can be, from a snapshot.
        sj="$WORK/split-json"
        export_json BcpJsonSource "$sj" --include-schemas split --split-tables "split.*:8" --snapshot | tee "$WORK/split-json.log"
        assert_no_snapshots
        ls -l "$sj"
        for t in Big NullKey Descending; do
            n="$(part_count "$sj" "split.$t" json)"
            [ "$n" = "8" ] || { echo "::error::split.$t has $n part files, expected 8"; exit 1; }
        done
        for t in Text Parted PartedHeap PartedCci; do
            n="$(part_count "$sj" "split.$t" json)"
            [ "$n" -ge 2 ] || { echo "::error::split.$t has $n part files, expected at least 2"; exit 1; }
        done
        [ -f "$sj/split.Heap.json" ] && [ "$(part_count "$sj" split.Heap json)" = "0" ] || { echo "::error::split.Heap was not exported as one file"; exit 1; }
        grep -q "split.Heap: not split:" "$WORK/split-json.log" || { echo "::error::No 'not split' warning for split.Heap"; exit 1; }
        bcpjson verify --path "$sj"
        jq -e 'all(.tables[] | select(.split != null); .split.partCount == (.split.parts | length))' "$sj/$MANIFEST" > /dev/null \
            || { echo "::error::A split table's manifest entry does not list all its parts"; cat "$sj/$MANIFEST"; exit 1; }
        # Row counts per table match the unsplit export's manifest: no row lost or read twice.
        diff <(jq -r '.tables[] | select(.name | startswith("split.")) | "\(.name) \(.rows)"' "$src/$MANIFEST") \
             <(jq -r '.tables[] | "\(.name) \(.rows)"' "$sj/$MANIFEST") || { echo "::error::Split export row counts differ"; exit 1; }

        # The parts joined in order equal the unsplit export where the split column leads the primary key (or the
        # sort order); partitioned tables are split on Stamp but sorted by Id, so only their rows are compared.
        joined="$WORK/split-joined"
        rm -rf "$joined"
        mkdir -p "$joined"
        for t in Big Text NullKey Descending; do
            join_parts "$sj" "split.$t" > "$joined/split.$t.json"
            cmp "$joined/split.$t.json" "$src/split.$t.json" || { echo "::error::Joined parts of split.$t differ from the unsplit export"; exit 1; }
        done
        for t in Parted PartedHeap PartedCci; do
            join_parts "$sj" "split.$t" > "$joined/split.$t.json"
            diff <(normalize_rows "$joined/split.$t.json" | grep -v '^$') <(normalize_rows "$src/split.$t.json" | grep -v '^$') > /dev/null \
                || { echo "::error::The parts of split.$t do not hold the rows of the unsplit export"; exit 1; }
        done
        echo "Joined parts equal the unsplit export (Big, Text, NullKey, Descending; Parted* ignoring order)."

        # 3. BCP: queryout per part with the boundaries as typed literals, one shared format file.
        sb="$WORK/split-bcp"
        rm -rf "$sb"
        bcpjson export "${SOURCE[@]}" --source-database BcpJsonSource --file-format bcp --export-path "$sb" \
            --include-tables split.Big,split.Text,split.Descending,split.Parted --split-tables "split.*:4" | tee "$WORK/split-bcp.log"
        ls -l "$sb"
        [ "$(part_count "$sb" split.Big bcp)" = "4" ] || { echo "::error::split.Big has no 4 BCP parts"; exit 1; }
        [ -f "$sb/split.Big-c.fmt" ] || { echo "::error::split.Big has no format file"; exit 1; }
        # What bcp wrote to the parts' error logs (row errors, or only warnings).
        for f in "$sb"/*_error.log; do
            if [ -s "$f" ]; then
                echo "--- $(basename "$f"):"
                cat "$f"
                echo
            fi
        done
        bcpjson verify --path "$sb"
        diff <(jq -r '.tables[] | select(.name == "split.Big" or .name == "split.Text" or .name == "split.Descending" or .name == "split.Parted") | "\(.name) \(.rows)"' "$src/$MANIFEST") \
             <(jq -r '.tables[] | "\(.name) \(.rows)"' "$sb/$MANIFEST") || { echo "::error::Split BCP export row counts differ"; exit 1; }

        # BCP parts name the columns the format file lists. bcp format leaves out the HIDDEN period columns of a temporal
        # table, so the parts must too: every row has exactly the format file's fields.
        sql -Q "DROP DATABASE IF EXISTS BcpJsonSplitOdd; CREATE DATABASE BcpJsonSplitOdd;"
        sql -d BcpJsonSplitOdd -Q "CREATE TABLE dbo.Versioned (Id INT NOT NULL CONSTRAINT PK_Versioned PRIMARY KEY CLUSTERED, Val INT NOT NULL,
            ValidFrom DATETIME2(7) GENERATED ALWAYS AS ROW START HIDDEN NOT NULL, ValidTo DATETIME2(7) GENERATED ALWAYS AS ROW END HIDDEN NOT NULL,
            PERIOD FOR SYSTEM_TIME (ValidFrom, ValidTo)) WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.VersionedHistory));"
        sql -d BcpJsonSplitOdd -Q "SET NOCOUNT ON; INSERT dbo.Versioned (Id, Val) SELECT TOP (20000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), 1 FROM sys.all_objects a CROSS JOIN sys.all_objects b; UPDATE STATISTICS dbo.Versioned WITH FULLSCAN;"
        st="$WORK/split-temporal"
        rm -rf "$st"
        bcpjson export "${SOURCE[@]}" --source-database BcpJsonSplitOdd --file-format bcp --export-path "$st" \
            --include-tables dbo.Versioned --split-tables "dbo.Versioned:4" | tee "$WORK/split-temporal.log"
        [ "$(part_count "$st" dbo.Versioned bcp)" = "4" ] || { echo "::error::dbo.Versioned has no 4 BCP parts"; exit 1; }
        bcpjson verify --path "$st"
        [ "$(jq '.tables[] | select(.name == "dbo.Versioned") | .rows' "$st/$MANIFEST")" = "20000" ] || { echo "::error::dbo.Versioned rows"; exit 1; }
        fields="$(sed -n 2p "$st/dbo.Versioned-c.fmt" | tr -d '[:space:]')"
        [ "$fields" = "2" ] || { echo "::error::Format file of dbo.Versioned lists $fields columns, expected 2 (hidden ones left out)"; exit 1; }
        if cat "$st"/dbo.Versioned.part*.bcp | awk -F'|' -v n="$fields" 'NF != n { bad++ } END { exit bad > 0 ? 0 : 1 }'; then
            echo "::error::BCP part rows do not have the format file's $fields fields"; exit 1
        fi
        echo "Temporal table: 4 BCP parts, 20,000 rows of $fields fields each."

        # A sparse column set changes the columns a table returns, so such a table is not split (whatever the format)
        # and is exported whole. It lives here, not in BcpJsonSource: imports cannot load a column set and its columns.
        sql -d BcpJsonSplitOdd -Q "EXEC (N'CREATE SCHEMA split'); CREATE TABLE split.ColumnSet (Id INT NOT NULL CONSTRAINT PK_ColumnSet PRIMARY KEY CLUSTERED,
            Code VARCHAR(10) NOT NULL, A INT SPARSE NULL, B NVARCHAR(20) SPARSE NULL, Props XML COLUMN_SET FOR ALL_SPARSE_COLUMNS);"
        sql -d BcpJsonSplitOdd -Q "SET NOCOUNT ON; INSERT split.ColumnSet (Id, Code, A, B) SELECT TOP (300) n, CONCAT('c', n), CASE WHEN n % 2 = 0 THEN n END, CASE WHEN n % 3 = 0 THEN CONCAT(N'b', n) END
            FROM (SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM sys.all_objects) AS x; UPDATE STATISTICS split.ColumnSet WITH FULLSCAN;"
        export_json BcpJsonSplitOdd "$WORK/split-columnset" --include-tables split.ColumnSet --split-tables "split.*:4" | tee "$WORK/split-columnset.log"
        grep -qF "split.ColumnSet: not split: the table has a sparse column set." "$WORK/split-columnset.log" || { echo "::error::split.ColumnSet was not declined"; exit 1; }
        [ -s "$WORK/split-columnset/split.ColumnSet.json" ] && [ "$(part_count "$WORK/split-columnset" split.ColumnSet json)" = "0" ] \
            || { echo "::error::split.ColumnSet was not exported as one file"; ls -l "$WORK/split-columnset"; exit 1; }
        echo "Sparse column set: not split, exported whole."
        sql -Q "ALTER DATABASE BcpJsonSplitOdd SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE BcpJsonSplitOdd;"

        # 4. --resume after losing one part, without --split-tables: the recorded split is kept and only that part redone.
        res="$WORK/split-resume"
        rm -rf "$res"
        cp -r "$sj" "$res"
        rm "$res/split.Big.part0003.json"
        jq '.completed = null | .tables |= map(if .name == "split.Big" then (.split.parts |= map(select(.part != 3)) | .files |= map(select(.name != "split.Big.part0003.json"))) else . end)' \
            "$sj/$MANIFEST" > "$res/$MANIFEST"
        bcpjson export "${SOURCE[@]}" --source-database BcpJsonSource --file-format json --include-schemas split --export-path "$res" --resume | tee "$WORK/split-resume.log"
        grep -qF "split.Big: --resume keeps its recorded split (8 parts)." "$WORK/split-resume.log" || { echo "::error::--resume did not keep the recorded split"; exit 1; }
        grep -qF "split.Big: 1 part(s) to go." "$WORK/split-resume.log" || { echo "::error::--resume did not redo exactly one part"; exit 1; }
        cmp "$sj/split.Big.part0003.json" "$res/split.Big.part0003.json" || { echo "::error::The redone part differs"; exit 1; }
        bcpjson verify --path "$res"

        # 5. Import the split JSON export: parts are grouped per table and load in parallel or one after another.
        scalar() { sql -d "$1" -h -1 -W -Q "SET NOCOUNT ON; $2" | tr -d '[:space:]'; }
        sjs="$WORK/source-json-split"
        rm -rf "$sjs"
        mkdir -p "$sjs"
        cp "$src"/split.*.json "$sjs/"
        # (a) Plain import: clustered targets load their parts in key order with an order hint each, heaps in parallel.
        create_target_db BcpJsonSplit
        bcpjson import "${TARGET[@]}" --target-database BcpJsonSplit --file-format json --max-tasks 4 --source-path "$sj" --source-file '*.json' | tee "$WORK/split-import.log"
        grep -qF "split.Big: 8 parts, loading one after another (clustered index)." "$WORK/split-import.log" || { echo "::error::split.Big did not load its parts one after another"; exit 1; }
        grep -qE "split\.PartedHeap: [0-9]+ parts, loading in parallel\." "$WORK/split-import.log" || { echo "::error::split.PartedHeap did not load its parts in parallel"; exit 1; }
        grep -qE "split\.PartedCci: [0-9]+ parts, loading one after another \(columnstore\)\." "$WORK/split-import.log" || { echo "::error::split.PartedCci was not loaded one part after another"; exit 1; }
        grep -qE "split\.Heap: [0-9,.]+ \(Import Complete\)" "$WORK/split-import.log" || { echo "::error::split.Heap was not imported"; exit 1; }
        if grep -qE "split\.Heap: [0-9]+ parts" "$WORK/split-import.log"; then echo "::error::An unsplit table logged a load mode"; exit 1; fi
        hints="$(grep -c "split.Big: Rows are in clustered index order; loading with an order hint." "$WORK/split-import.log" || true)"
        [ "$hints" = "8" ] || { echo "::error::split.Big logged $hints order hints, expected one per part (8)"; exit 1; }
        export_json BcpJsonSplit "$WORK/split-import" --include-schemas split
        compare_exact "$sjs" "$WORK/split-import"

        # (d) Parts into a target clustered on another key: no order hint, every row arrives.
        bcpjson import "${TARGET[@]}" --target-database BcpJsonSplit --file-format json --source-path "$sj" --source-file 'split.Big.part*.json' --target-table split.BigOther | tee "$WORK/split-import-other.log"
        if grep -q "Rows are in clustered index order" "$WORK/split-import-other.log"; then echo "::error::Order hint on a different clustered key"; exit 1; fi
        [ "$(scalar BcpJsonSplit "SELECT COUNT(*) FROM split.BigOther;")" = "200000" ] || { echo "::error::split.BigOther does not hold 200,000 rows"; exit 1; }
        echo "split.BigOther: 200,000 rows from the parts of split.Big, no order hint."

        # (f) --truncate-target empties a table once, before its first part: importing twice leaves 200,000 rows, not one part's.
        for run in 1 2; do
            bcpjson import "${TARGET[@]}" --target-database BcpJsonSplit --file-format json --max-tasks 4 --truncate-target --source-path "$sj" --source-file 'split.Big.part*.json' | tee "$WORK/split-import-truncate.log"
        done
        [ "$(grep -c "split.Big: Truncate Complete." "$WORK/split-import-truncate.log")" = "1" ] || { echo "::error::split.Big was not truncated exactly once"; exit 1; }
        [ "$(scalar BcpJsonSplit "SELECT COUNT(*) FROM split.Big;")" = "200000" ] || { echo "::error::split.Big does not hold 200,000 rows after two truncating imports"; exit 1; }
        echo "--truncate-target: one truncation per table, 200,000 rows."

        # (b) --disable-indexes: nonclustered indexes (not the primary key) are disabled for the load and rebuilt after it.
        create_target_db BcpJsonSplitIdx
        bcpjson import "${TARGET[@]}" --target-database BcpJsonSplitIdx --file-format json --max-tasks 4 --disable-indexes --source-path "$sj" --source-file '*.json' | tee "$WORK/split-import-idx.log"
        grep -qF "split.Big: Disabled indexes: IX_Big_Stamp, UX_Big_Code." "$WORK/split-import-idx.log" || { echo "::error::split.Big did not log its disabled indexes"; exit 1; }
        grep -qF "split.Big: Rebuilt 2 index(es)." "$WORK/split-import-idx.log" || { echo "::error::split.Big did not log the rebuild"; exit 1; }
        [ "$(scalar BcpJsonSplitIdx "SELECT COUNT(*) FROM sys.indexes WHERE is_disabled = 1;")" = "0" ] || { echo "::error::Indexes left disabled after --disable-indexes"; exit 1; }
        export_json BcpJsonSplitIdx "$WORK/split-import-idx" --include-schemas split
        compare_exact "$sjs" "$WORK/split-import-idx"

        # (c) A unique index over duplicate values cannot be rebuilt: the import fails, names the index, and leaves the
        #     primary key enabled.
        sql -d BcpJsonSplitIdx -Q "TRUNCATE TABLE split.Descending; CREATE UNIQUE NONCLUSTERED INDEX UX_Dup ON split.Descending (Val);"
        if bcpjson import "${TARGET[@]}" --target-database BcpJsonSplitIdx --file-format json --disable-indexes --truncate-target --source-path "$sj" --source-file 'split.Descending.part*.json' > "$WORK/split-import-dup.log" 2>&1; then
            cat "$WORK/split-import-dup.log"; echo "::error::The import succeeded although UX_Dup could not be rebuilt"; exit 1
        fi
        cat "$WORK/split-import-dup.log"
        grep -qE "split\.Descending: Could not rebuild index UX_Dup: .*duplicate key.* It is still disabled; run ALTER INDEX \[UX_Dup\] ON \[split\]\.\[Descending\] REBUILD\.$" "$WORK/split-import-dup.log" || { echo "::error::The failed rebuild of UX_Dup was not reported"; exit 1; }
        [ "$(scalar BcpJsonSplitIdx "SELECT CONCAT(is_disabled, '/', (SELECT is_disabled FROM sys.indexes WHERE name = N'UX_Dup')) FROM sys.indexes WHERE name = N'PK_Descending';")" = "0/1" ] \
            || { echo "::error::Expected PK_Descending enabled and UX_Dup disabled"; exit 1; }
        [ "$(scalar BcpJsonSplitIdx "SELECT COUNT(*) FROM split.Descending;")" = "20000" ] || { echo "::error::split.Descending rows"; exit 1; }
        echo "Unique index over duplicates: reported, left disabled; the primary key stays enabled."

        # (e) BCP parts: each part is a bcp in with the table's shared format file and logs named after the part.
        create_target_db BcpJsonSplitBcp
        bcpjson import "${TARGET[@]}" --target-database BcpJsonSplitBcp --file-format bcp --max-tasks 4 --source-path "$sb" --source-file '*.bcp' | tee "$WORK/split-import-bcp.log"
        grep -qF "split.Big: 4 parts, loading one after another (clustered index)." "$WORK/split-import-bcp.log" || { echo "::error::split.Big BCP parts did not load one after another"; exit 1; }
        [ -f "$sb/split.Big.part0001_import_output.log" ] || { echo "::error::No import log named after the part"; ls "$sb"; exit 1; }
        sbs="$WORK/source-json-split-bcp"
        rm -rf "$sbs"
        mkdir -p "$sbs"
        for t in Big Text Descending Parted; do cp "$src/split.$t.json" "$sbs/"; done
        export_json BcpJsonSplitBcp "$WORK/split-import-bcp" --include-tables split.Big,split.Text,split.Descending,split.Parted
        compare_exact "$sbs" "$WORK/split-import-bcp"
        ;;

    *)
        echo "usage: $0 setup|json|bcp|copy|snapshot|consistency|resilience|compression|split" >&2
        exit 2
        ;;
esac
