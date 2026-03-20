#!/bin/zsh

# ============================================================
# Build
# ============================================================

# 构建 DuckDB submodule（首次或更新后执行）
build_duckdb:
  git submodule update --init --recursive
  cd duckdb && mkdir -p build_release && cd build_release && \
  cmake .. -DCMAKE_BUILD_TYPE=Release \
           -DBUILD_COMPLETE_EXTENSION_SET=OFF \
           -DSKIP_EXTENSIONS="parquet;jemalloc;core_functions" \
           -DBUILD_UNITTESTS=OFF \
           -DBUILD_SHELL=OFF && \
  make -j$(nproc)

# CPU-only debug 构建
build:
  cargo build

# CPU-only release 构建
build_release:
  cargo build --release

# GPU release 构建
build_gpu:
  cargo build --release --features gpu

# 构建可加载扩展（.duckdb_extension）
build_ext:
  bash scripts/build_extension.sh

# GPU 模式构建可加载扩展
build_ext_gpu:
  bash scripts/build_extension.sh --gpu

# ============================================================
# Test
# ============================================================

# 运行全部测试
test:
  cargo test

# 仅单元测试
test_ut:
  cargo test --lib

# 仅集成测试
test_it:
  cargo test --test integration

# 仅 mock GPU 测试
test_mock:
  cargo test --test mock_gpu

# GPU 测试（需要 CUDA）
test_gpu:
  cargo test --features gpu

# 运行指定测试
test_one name:
  cargo test {{name}}

# ============================================================
# Lint & Check
# ============================================================

# clippy 检查
lint:
  cargo clippy -- -D warnings

# 格式化检查
fmt_check:
  cargo fmt -- --check

# 格式化
fmt:
  cargo fmt

# 全部检查（fmt + clippy + test）
check:
  just fmt_check && just lint && just test

# ============================================================
# DuckDB CLI
# ============================================================

# 启动 DuckDB 并加载扩展
duckdb:
  duckdb -unsigned -c "LOAD 'build/sirius_rust.duckdb_extension';"

# 交互式 DuckDB（自动加载扩展）
duckdb_repl:
  @echo "LOAD 'build/sirius_rust.duckdb_extension';" | duckdb -unsigned -init /dev/stdin

# 一键构建 + 加载测试
run: build_ext
  duckdb -unsigned -c "\
    LOAD 'build/sirius_rust.duckdb_extension'; \
    SELECT sirius_sum(i) AS sum, sirius_count(i) AS count FROM range(1, 101) AS t(i); \
  "

# ============================================================
# Clean
# ============================================================

# 清理 Rust 构建产物
clean:
  cargo clean

# 清理扩展构建产物
clean_ext:
  rm -rf build/

# 全部清理
clean_all:
  cargo clean && rm -rf build/
