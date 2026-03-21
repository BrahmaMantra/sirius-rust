#!/bin/bash
# 生成 sirius 版本的 TPC-H 查询（把原生 agg 函数替换为 sirius_* 前缀）
#
# 用法:
#   ./tests/tpch/gen_sirius_queries.sh
#
# 输出:
#   tests/tpch/queries/sirius/q01.sql ~ q22.sql

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="$SCRIPT_DIR/queries"
DST_DIR="$SCRIPT_DIR/queries/sirius"
mkdir -p "$DST_DIR"

# Q16 不生成（COUNT DISTINCT 不支持）
SKIP_QUERIES=(16)

for QFILE in "$SRC_DIR"/q*.sql; do
    BASENAME=$(basename "$QFILE")
    QNUM=$(echo "$BASENAME" | grep -oP '\d+' | head -1 | sed 's/^0*//')

    SKIP=0
    for S in "${SKIP_QUERIES[@]}"; do
        if [ "$QNUM" -eq "$S" ]; then SKIP=1; break; fi
    done

    if [ "$SKIP" -eq 1 ]; then
        echo "Skip Q${QNUM} (COUNT DISTINCT)"
        continue
    fi

    # 替换规则：
    #   \bsum(   -> sirius_sum(
    #   \bavg(   -> sirius_avg(
    #   \bmin(   -> sirius_min(
    #   \bmax(   -> sirius_max(
    #   \bcount( -> sirius_count(
    # 注意：count(*) 单独处理为 sirius_count_star()
    sed \
        -e 's/\bcount(\*)/sirius_count_star()/gI' \
        -e 's/\bsum(/sirius_sum(/gI' \
        -e 's/\bavg(/sirius_avg(/gI' \
        -e 's/\bmin(/sirius_min(/gI' \
        -e 's/\bmax(/sirius_max(/gI' \
        -e 's/\bcount(/sirius_count(/gI' \
        "$QFILE" > "$DST_DIR/$BASENAME"

    echo "Generated $DST_DIR/$BASENAME"
done

echo "Done. Sirius queries in $DST_DIR"
