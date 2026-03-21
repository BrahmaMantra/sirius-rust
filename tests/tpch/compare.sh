#!/bin/bash
# 对比 native 和 sirius 的 benchmark 结果（取中位数行）
#
# 用法:
#   ./tests/tpch/compare.sh [sf]

SF=${1:-0.1}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"
NATIVE="$RESULTS_DIR/native_sf${SF}.txt"
SIRIUS="$RESULTS_DIR/sirius_sf${SF}.txt"

if [ ! -f "$NATIVE" ]; then
    echo "Native results not found: $NATIVE"
    echo "Run: ./tests/tpch/run_native.sh ${SF}"
    exit 1
fi
if [ ! -f "$SIRIUS" ]; then
    echo "Sirius results not found: $SIRIUS"
    echo "Run: ./tests/tpch/run_sirius.sh ${SF}"
    exit 1
fi

echo "TPC-H Comparison  SF=${SF}"
echo "============================================================"
printf "%-6s  %10s  %10s  %10s\n" "Query" "Native(ms)" "Sirius(ms)" "Speedup"
echo "------------------------------------------------------------"

while IFS= read -r line; do
    Q=$(echo "$line" | grep -oP 'Q\d+')
    [ -z "$Q" ] && continue
    NATIVE_MS=$(echo "$line" | grep -oP '^\S+: \K\d+')
    [ -z "$NATIVE_MS" ] && continue

    SIRIUS_LINE=$(grep "^${Q}:" "$SIRIUS" 2>/dev/null || true)
    if echo "$SIRIUS_LINE" | grep -q "SKIP"; then
        printf "%-6s  %10s  %10s  %10s\n" "$Q" "${NATIVE_MS}ms" "SKIP" "-"
        continue
    fi
    SIRIUS_MS=$(echo "$SIRIUS_LINE" | grep -oP '^\S+: \K\d+')

    if [ -z "$SIRIUS_MS" ]; then
        printf "%-6s  %10s  %10s  %10s\n" "$Q" "${NATIVE_MS}ms" "-" "-"
    else
        SPEEDUP=$(awk "BEGIN {printf \"%.2f\", $NATIVE_MS / $SIRIUS_MS}")
        printf "%-6s  %10s  %10s  %10s\n" "$Q" "${NATIVE_MS}ms" "${SIRIUS_MS}ms" "${SPEEDUP}x"
    fi
done < "$NATIVE"

echo "============================================================"
