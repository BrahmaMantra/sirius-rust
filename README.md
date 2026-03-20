# sirius-rust

用 Rust + CUDA 实现的 **GPU 加速 DuckDB 聚合扩展**，参考 [sirius](https://github.com/sirius-db/sirius) 架构。

## 架构

```
SQL 查询 → DuckDB → sirius-rust 聚合函数 → GPU 执行 (fallback CPU) → 返回结果
```

sirius-rust 以 DuckDB C 扩展（`cdylib`）的形式加载，通过 Aggregate Function API 注册自定义聚合函数。运行时优先走 GPU 路径，GPU 不可用或数据量过小时自动回退 CPU。

### 执行流程

1. DuckDB 将查询中的 `sirius_sum(col)` / `sirius_count(col)` 分派到 sirius-rust 的 C 回调
2. 回调将 DuckDB Vector 转换为内部 `Column` 表示
3. `gpu_executor` 判断是否走 GPU 路径
4. GPU 路径：数据通过 `GpuMemory` 传输到 Device，CUDA kernel 执行 shared memory tree reduction
5. CPU 路径：`SumFunction` / `CountFunction` 在 host 端完成聚合
6. 结果写回 DuckDB result vector

## 已实现功能

| 函数 | 支持类型 | GPU Kernel | CPU Fallback |
|------|----------|------------|--------------|
| `sirius_sum` | TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE | `sum_reduce_i64` / `sum_reduce_f64` | SumFunction |
| `sirius_count` | TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, VARCHAR | `count_reduce` | CountFunction |

## 目录结构

```
src/
├── lib.rs                    # 模块声明
├── sirius_extension.rs       # DuckDB 扩展入口 (C_STRUCT ABI + 传统入口)
├── ext_api.rs                # DuckDB C API 函数指针表 (C_STRUCT ABI 核心)
├── sirius_context.rs         # 全局执行上下文 (GPU 检测 + AggregateRegistry)
├── gpu_executor.rs           # GPU/CPU 调度 + 分批执行
├── gpu_context.rs            # CUDA 设备管理 (feature = "gpu")
├── gpu_columns.rs            # DuckDB Vector ↔ Column 转换
├── fallback.rs               # CPU 回退执行
├── error.rs                  # 错误类型
├── ffi.rs                    # bindgen 生成的 DuckDB C API + Extension API 绑定
├── op/aggregate/             # 聚合算子
│   ├── traits.rs             # AggregateFunction trait
│   ├── sum.rs                # SUM (CPU 实现 + 状态管理)
│   ├── count.rs              # COUNT / COUNT(*) (CPU 实现)
│   ├── sum_agg.rs            # SUM 的 DuckDB C 回调
│   ├── count_agg.rs          # COUNT 的 DuckDB C 回调
│   ├── register.rs           # DuckDB Aggregate Function Set 注册
│   ├── avg.rs / min.rs / max.rs  # (预留)
│   └── mod.rs                # AggregateRegistry
├── cuda/
│   ├── backend.rs            # GpuBackend trait + MockGpuBackend
│   └── kernels/
│       ├── sum.cu            # SUM CUDA kernel (shared memory reduction)
│       ├── count.cu          # COUNT CUDA kernel
│       ├── sum.rs            # Rust wrapper (feature = "gpu")
│       └── count.rs          # Rust wrapper (feature = "gpu")
├── data/                     # 数据表示
│   ├── types.rs              # SiriusType ↔ DuckDB type 映射
│   ├── column.rs             # Column (typed columnar data + validity bitmap)
│   └── chunk.rs              # DataChunk
├── memory/gpu_memory.rs      # Host ↔ Device 内存传输 (feature = "gpu")
├── operator/                 # GPU 物理算子 (预留)
├── pipeline/                 # 执行管道 (预留)
├── plan/                     # Substrait 查询计划 (预留)
└── planner/                  # 物理计划生成 (预留)

tests/
├── integration.rs            # 真实 DuckDB 集成测试 (7 tests)
├── mock_gpu.rs               # MockGpuBackend 测试 (19 tests)
└── mock_gpu/
    ├── test_sum.rs
    ├── test_count.rs
    ├── test_aggregate_registry.rs
    └── test_mock_gpu.rs

scripts/
└── build_extension.sh        # 一键构建可加载扩展
```

## 构建

### 前置依赖

- Rust 1.70+
- Clang（bindgen 需要）
- CMake 3.15+（编译 DuckDB）

GPU 模式额外需要：
- CUDA Toolkit 11.0+
- 支持 compute capability 7.0+ 的 NVIDIA GPU

### 编译 DuckDB

```bash
git submodule update --init --recursive

cd duckdb
mkdir build_release && cd build_release
cmake .. -DCMAKE_BUILD_TYPE=Release \
         -DBUILD_COMPLETE_EXTENSION_SET=OFF \
         -DSKIP_EXTENSIONS="parquet;jemalloc;core_functions" \
         -DBUILD_UNITTESTS=OFF \
         -DBUILD_SHELL=OFF
make -j$(nproc)
cd ../..
```

### 编译 sirius-rust

```bash
# CPU-only 模式（静态链接 DuckDB，用于测试）
cargo build --release

# GPU 模式（需要 CUDA Toolkit）
cargo build --release --features gpu
```

### 构建可加载扩展

构建可在标准 DuckDB CLI 中加载的扩展文件（`.duckdb_extension`）：

```bash
./scripts/build_extension.sh

# GPU 模式
./scripts/build_extension.sh --gpu
```

生成的扩展文件位于 `build/sirius_rust.duckdb_extension`。

## 使用

### 安装 DuckDB CLI

```bash
pip install duckdb-cli
```

### 加载扩展

```bash
# 启动 DuckDB（-unsigned 允许加载未签名扩展）
duckdb -unsigned
```

在 DuckDB 交互式终端中：

```sql
-- 加载扩展
LOAD 'build/sirius_rust.duckdb_extension';

-- SUM（支持所有数值类型）
SELECT sirius_sum(price) FROM orders;

-- COUNT（自动跳过 NULL）
SELECT sirius_count(email) FROM users;

-- 类型转换
SELECT sirius_sum(amount::DOUBLE) FROM transactions;

-- 对比 DuckDB 原生聚合
SELECT sum(i) AS native, sirius_sum(i) AS sirius FROM range(1, 10001) AS t(i);
```

也可以一行命令直接执行：

```bash
duckdb -unsigned -c "
  LOAD 'build/sirius_rust.duckdb_extension';
  SELECT sirius_sum(i) FROM range(1, 101) AS t(i);
"
```

## 测试

```bash
# 运行全部测试（66 tests）
cargo test

# 仅集成测试（需要 DuckDB 静态库）
cargo test --test integration

# 仅单元测试
cargo test --lib

# GPU 测试（需要 CUDA）
cargo test --features gpu
```

测试覆盖：

| 类别 | 数量 | 说明 |
|------|------|------|
| 单元测试 | 40 | SUM/COUNT 逻辑、数据类型、GPU executor、MockGpuBackend |
| 集成测试 | 7 | 在真实 DuckDB 中执行 `sirius_sum` / `sirius_count` |
| Mock GPU 测试 | 19 | 模拟 GPU block reduction 验证正确性 |

## 技术栈

| 组件 | 选择 | 说明 |
|------|------|------|
| 语言 | Rust | 安全、高性能 |
| GPU | cudarc + CUDA kernel | Rust CUDA 绑定 + 手写 `.cu` |
| DuckDB 集成 | C API (bindgen) | 从 submodule 源码生成 FFI 绑定 |
| CUDA 编译 | cc crate | build.rs 编译 `.cu` 文件 |
| 错误处理 | thiserror | 统一错误类型 |
| 数值泛型 | num-traits | 类型抽象 |

## 路线图

- **第一阶段**（当前）：SUM / COUNT 聚合 + GPU kernel + 集成测试
- **第二阶段**：AVG, MIN, MAX, COUNT_DISTINCT + GroupBy 支持
- **第三阶段**：Join, OrderBy, Limit + 完整 Substrait 管道

## 跨平台

- Linux / Windows 均支持
- GPU 通过 Cargo feature gate 可选：`--features gpu`
- 无 GPU 环境 `cargo build` / `cargo test` 也能通过

## License

MIT
