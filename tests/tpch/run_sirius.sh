#!/bin/bash
# TPC-H benchmark — sirius-rust GPU 聚合（实验组）
#
# 用法:
#   ./tests/tpch/run_sirius.sh [sf] [runs]
#
# 前置条件:
#   just build_ext
#   ./tests/tpch/run_native.sh [sf]       # 生成数据文件
#   ./tests/tpch/gen_sirius_queries.sh    # 生成 sirius 版本查询

set -e

SF=${1:-0.1}
RUNS=${2:-5}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
EXT_PATH="$REPO_ROOT/build/sirius_rust.duckdb_extension"
RESULTS_DIR="$SCRIPT_DIR/results"
DATA_DIR="$SCRIPT_DIR/data"
mkdir -p "$RESULTS_DIR"

DB_FILE="$DATA_DIR/tpch_sf${SF}.duckdb"
OUTPUT="$RESULTS_DIR/sirius_sf${SF}.txt"

if [ ! -f "$EXT_PATH" ]; then
    echo "Extension not found: $EXT_PATH"
    echo "Run: just build_ext"
    exit 1
fi

if [ ! -f "$DB_FILE" ]; then
    echo "Data not found: $DB_FILE"
    echo "Run: ./tests/tpch/run_native.sh ${SF}"
    exit 1
fi

echo "TPC-H Sirius Benchmark  SF=${SF}  runs=${RUNS}  $(date)" | tee "$OUTPUT"
echo "Extension: $EXT_PATH" | tee -a "$OUTPUT"
echo "========================================" | tee -a "$OUTPUT"

median() {
    echo "$@" | tr ' ' '\n' | sort -n | awk '{a[NR]=$1} END {
        if (NR%2==1) print a[int(NR/2)+1]
        else print int((a[NR/2]+a[NR/2+1])/2)
    }'
}

SKIP_QUERIES=(16)
QUERIES=(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22)

for Q in "${QUERIES[@]}"; do
    SKIP=0
    for S in "${SKIP_QUERIES[@]}"; do
        if [ "$Q" -eq "$S" ]; then SKIP=1; break; fi
    done
    if [ "$SKIP" -eq 1 ]; then
        echo "Q$(printf '%02d' $Q): SKIP (COUNT DISTINCT not supported)" | tee -a "$OUTPUT"
        continue
    fi

    QFILE="$SCRIPT_DIR/queries/sirius/q$(printf '%02d' $Q).sql"
    if [ ! -f "$QFILE" ]; then
        echo "Q$(printf '%02d' $Q): SKIP (run gen_sirius_queries.sh first)" | tee -a "$OUTPUT"
        continue
    fi

    TIMES=()
    for _ in $(seq 1 $RUNS); do
        START=$(date +%s%N)
        duckdb -unsigned "$DB_FILE" -c "LOAD '${EXT_PATH}'; $(cat "$QFILE")" > /dev/null 2>&1
        END=$(date +%s%N)
        TIMES+=( $(( (END - START) / 1000000 )) )
    done

    MED=$(median "${TIMES[@]}")
    ALL="${TIMES[*]}"
    echo "Q$(printf '%02d' $Q): ${MED} ms  (runs: ${ALL// /, } ms)" | tee -a "$OUTPUT"
done

echo "========================================" | tee -a "$OUTPUT"
echo "Done. Results saved to $OUTPUT"
