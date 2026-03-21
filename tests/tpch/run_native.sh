#!/bin/bash
# TPC-H benchmark — DuckDB 原生聚合（对照组）
#
# 用法:
#   ./tests/tpch/run_native.sh [sf] [runs]
#
# 参数:
#   sf    Scale factor，默认 0.1（可选 0.1 / 0.5 / 1 / 5 / 10）
#   runs  每条查询重复次数，默认 5，取中位数

set -e

SF=${1:-0.1}
RUNS=${2:-5}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"
DATA_DIR="$SCRIPT_DIR/data"
mkdir -p "$RESULTS_DIR" "$DATA_DIR"

DB_FILE="$DATA_DIR/tpch_sf${SF}.duckdb"
OUTPUT="$RESULTS_DIR/native_sf${SF}.txt"

if [ ! -f "$DB_FILE" ]; then
    echo "Generating TPC-H SF=${SF} data -> $DB_FILE ..."
    duckdb "$DB_FILE" << EOF
INSTALL tpch;
LOAD tpch;
CALL dbgen(sf=${SF});
SELECT 'lineitem rows: ' || count(*) FROM lineitem;
EOF
    echo "Data generation done."
else
    echo "Using existing data: $DB_FILE"
fi

echo ""
echo "TPC-H Native Benchmark  SF=${SF}  runs=${RUNS}  $(date)" | tee "$OUTPUT"
echo "========================================" | tee -a "$OUTPUT"

median() {
    # 对空格分隔的数字列表取中位数
    echo "$@" | tr ' ' '\n' | sort -n | awk '{a[NR]=$1} END {
        if (NR%2==1) print a[int(NR/2)+1]
        else print int((a[NR/2]+a[NR/2+1])/2)
    }'
}

QUERIES=(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22)

for Q in "${QUERIES[@]}"; do
    QFILE="$SCRIPT_DIR/queries/q$(printf '%02d' $Q).sql"
    if [ ! -f "$QFILE" ]; then
        echo "Q$(printf '%02d' $Q): SKIP (file not found)" | tee -a "$OUTPUT"
        continue
    fi

    TIMES=()
    for _ in $(seq 1 $RUNS); do
        START=$(date +%s%N)
        duckdb "$DB_FILE" < "$QFILE" > /dev/null 2>&1
        END=$(date +%s%N)
        TIMES+=( $(( (END - START) / 1000000 )) )
    done

    MED=$(median "${TIMES[@]}")
    ALL="${TIMES[*]}"
    echo "Q$(printf '%02d' $Q): ${MED} ms  (runs: ${ALL// /, } ms)" | tee -a "$OUTPUT"
done

echo "========================================" | tee -a "$OUTPUT"
echo "Done. Results saved to $OUTPUT"
