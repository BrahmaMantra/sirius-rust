#!/bin/bash
# 构建 sirius-rust DuckDB 可加载扩展
# 用法: ./scripts/build_extension.sh [--gpu]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DUCKDB_DIR="$PROJECT_DIR/duckdb"
OUTPUT_DIR="$PROJECT_DIR/build"

FEATURES="--no-default-features"
if [[ "${1:-}" == "--gpu" ]]; then
    FEATURES="--no-default-features --features gpu"
fi

echo "==> Building sirius-rust loadable extension..."
cargo build --release $FEATURES

echo "==> Preparing metadata..."
mkdir -p "$OUTPUT_DIR"
printf '\x00' > /tmp/null.txt
printf 'linux_amd64' > /tmp/platform.txt

cp "$PROJECT_DIR/target/release/libsirius_rust.so" "$OUTPUT_DIR/sirius_rust.duckdb_extension"

cmake \
    -DEXTENSION="$OUTPUT_DIR/sirius_rust.duckdb_extension" \
    -DNULL_FILE=/tmp/null.txt \
    -DPLATFORM_FILE=/tmp/platform.txt \
    -DVERSION_FIELD=v1.2.0 \
    -DEXTENSION_VERSION=v0.1.0 \
    -DABI_TYPE="C_STRUCT" \
    -P "$DUCKDB_DIR/scripts/append_metadata.cmake"

echo "==> Done! Extension at: $OUTPUT_DIR/sirius_rust.duckdb_extension"
echo ""
echo "Usage:"
echo "  duckdb -unsigned -c \"LOAD '$OUTPUT_DIR/sirius_rust.duckdb_extension';\""
