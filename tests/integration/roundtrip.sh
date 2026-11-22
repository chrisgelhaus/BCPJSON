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
bcpjson() {
    local shown="$*"
    echo "+ bcpjson ${shown//"$SQL_PASSWORD"/***}"
    if [[ "$BCPJSON" == *.dll ]]; then
        dotnet "$BCPJSON" "$@" --trust-server-certificate
    else
        "$BCPJSON" "$@" --trust-server-certificate
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

# Empty database with the test schemas but no tables (copy creates them).
create_empty_db() {
    local db="$1"
    sql -Q "IF DB_ID(N'$db') IS NOT NULL BEGIN ALTER DATABASE [$db] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [$db]; END; CREATE DATABASE [$db];"
    sql -d "$db" -Q "EXEC (N'CREATE SCHEMA rt'); EXEC (N'CREATE SCHEMA plain');"
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
        bcpjson import "${TARGET[@]}" --target-database BcpJsonJson --file-format json --max-tasks 2 --source-path "$WORK/source-json" --source-file '*.json'
        # Documented behaviour: bulk loads don't check constraints, so SQL Server marks the target's foreign keys untrusted.
        echo "After import: $(sql -d BcpJsonJson -h -1 -W -Q "SET NOCOUNT ON; SELECT CONCAT(SUM(CAST(is_not_trusted AS INT)), ' of ', COUNT(*)) FROM sys.foreign_keys;" | tr -d '\r') foreign keys are not trusted."
        compare_decimals BcpJsonJson
        export_json BcpJsonJson "$WORK/json-roundtrip"
        compare_exact "$WORK/source-json" "$WORK/json-roundtrip"
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
        mkdir -p "$WORK/source-json-plain"
        rm -f "$WORK/source-json-plain"/*
        cp "$WORK/source-json"/plain.*.json "$WORK/source-json-plain/"
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

        # 4. Connection retry: the database only appears (renamed into place, with its table) a few seconds after the
        #    export starts; until then opening it fails with error 4060, which is transient.
        sql -Q "DROP DATABASE IF EXISTS BcpJsonLate; DROP DATABASE IF EXISTS BcpJsonLateStaging; CREATE DATABASE BcpJsonLateStaging;"
        sql -d BcpJsonLateStaging -Q "CREATE TABLE dbo.T (Id INT NOT NULL PRIMARY KEY); INSERT dbo.T VALUES (1), (2);"
        if bcpjson export "${SOURCE[@]}" --source-database BcpJsonLate --file-format json --export-path "$WORK/late-noretry" --retries 0 > "$WORK/late-noretry.log" 2>&1; then
            echo "::error::Control failed: the export succeeded although the database does not exist yet"; exit 1
        fi
        echo "Without retries the export fails at once: $(grep -m1 'Cannot connect' "$WORK/late-noretry.log")"
        (sleep 5; sql -Q "ALTER DATABASE BcpJsonLateStaging MODIFY NAME = BcpJsonLate;" > /dev/null) &
        renamer=$!
        bcpjson export "${SOURCE[@]}" --source-database BcpJsonLate --file-format json --export-path "$WORK/late" --retries 8 | tee "$WORK/late.log"
        wait "$renamer"
        grep -q "Connection failed" "$WORK/late.log" || { echo "::error::No connection retry was logged"; exit 1; }
        [ "$(jq '.tables[0].rows' "$WORK/late/$MANIFEST")" = "2" ] || { echo "::error::Retried export has the wrong rows"; cat "$WORK/late/$MANIFEST"; exit 1; }
        bcpjson verify --path "$WORK/late"
        sql -Q "DROP DATABASE BcpJsonLate;"
        ;;

    *)
        echo "usage: $0 setup|json|bcp|copy|snapshot|consistency|resilience" >&2
        exit 2
        ;;
esac
