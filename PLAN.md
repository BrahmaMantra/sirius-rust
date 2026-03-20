# Sirius-Rust 实现计划

## 项目概述

Sirius-Rust 是 [sirius](https://github.com/sirius-db/sirius) 的 Rust 实现版本，作为 DuckDB 扩展运行。第一阶段目标是实现 `SUM` 和 `COUNT` 聚合函数，同时搭建完整的扩展框架，为后续扩展预留位置。

## Sirius 原版架构分析

### 核心流程

```
SQL Query → DuckDB → Substrait Plan → Sirius Extension → GPU/CPU 执行 → 返回结果
```

### 关键发现

1. **Substrait 是核心交互格式**：sirius 通过 DuckDB 的 substrait 扩展将 SQL 查询转换为 Substrait plan，然后在 GPU 上执行
2. **三个入口函数**：`gpu_buffer_init`、`gpu_processing`、`gpu_processing_substrait`
3. **聚合函数支持**：COUNT、COUNT_STAR、COUNT_DISTINCT、SUM、SUM_NO_OVERFLOW、AVERAGE、MAX、MIN
4. **Fallback 机制**：GPU 执行失败时回退到 CPU

### Sirius-Rust 实现策略

与原版 sirius 一样，我们的 Rust 版本**做 GPU 加速**：
- 保持相同的 DuckDB 扩展接口
- 使用 Substrait 作为查询计划的中间表示
- 通过 Rust CUDA 绑定（cudarc）实现 GPU 并行计算
- GPU 不可用时自动回退 CPU 执行（Fallback 机制）
- 这是毕业设计项目，GPU 加速是核心技术亮点

## 技术架构

### 技术栈

| 组件 | 选择 | 说明 |
|------|------|------|
| 语言 | Rust | 安全、高性能 |
| GPU | `cudarc` | Rust CUDA 绑定，管理 GPU kernel 和内存 |
| DuckDB 绑定 | `duckdb-loadable-macros` / C FFI | DuckDB 扩展 API |
| Substrait | `substrait` crate | Substrait protobuf 定义 |
| 序列化 | `prost` | Protobuf 解析 |
| 构建 | Cargo + CMake bridge | DuckDB 扩展需要 CMake 集成 |

### 目录结构

```
sirius-rust/
├── Cargo.toml
├── Makefile
├── CMakeLists.txt                  # DuckDB 扩展构建集成
├── PLAN.md                         # 本文件
├── src/
│   ├── lib.rs                      # 扩展入口，注册函数
│   ├── extension.rs                # DuckDB 扩展加载/初始化
│   │
│   ├── plan/                       # 查询计划处理
│   │   ├── mod.rs
│   │   ├── substrait_consumer.rs   # Substrait plan 解析
│   │   ├── logical_plan.rs         # 内部逻辑计划表示
│   │   └── physical_plan.rs        # 物理计划生成
│   │
│   ├── operator/                   # 算子实现
│   │   ├── mod.rs
│   │   ├── traits.rs               # 算子 trait 定义
│   │   ├── aggregate/              # 聚合算子
│   │   │   ├── mod.rs
│   │   │   ├── traits.rs           # AggregateFunction trait
│   │   │   ├── sum.rs              # SUM 实现 ✅ 第一阶段
│   │   │   ├── count.rs            # COUNT 实现 ✅ 第一阶段
│   │   │   ├── avg.rs              # AVG (预留)
│   │   │   ├── min.rs              # MIN (预留)
│   │   │   └── max.rs              # MAX (预留)
│   │   ├── scan.rs                 # TableScan (预留)
│   │   ├── filter.rs               # Filter (预留)
│   │   ├── projection.rs           # Projection (预留)
│   │   ├── join.rs                 # Join (预留)
│   │   ├── order.rs                # OrderBy (预留)
│   │   └── limit.rs                # Limit (预留)
│   │
│   ├── data/                       # 数据表示
│   │   ├── mod.rs
│   │   ├── column.rs               # 列式数据结构
│   │   ├── types.rs                # 数据类型映射
│   │   └── chunk.rs                # DataChunk 抽象
│   │
│   ├── gpu/                        # GPU 加速层
│   │   ├── mod.rs
│   │   ├── context.rs              # CUDA 设备上下文管理
│   │   ├── memory.rs               # GPU 内存分配/传输
│   │   └── kernels/                # CUDA kernel
│   │       ├── mod.rs
│   │       ├── sum.cu / sum.rs     # SUM GPU kernel ✅ 第一阶段
│   │       └── count.cu / count.rs # COUNT GPU kernel ✅ 第一阶段
│   │
│   ├── executor/                   # 执行引擎
│   │   ├── mod.rs
│   │   ├── pipeline.rs             # 执行管道
│   │   └── context.rs              # 执行上下文（CPU/GPU 调度）
│   │
│   └── error.rs                    # 错误类型定义
│
└── tests/
    ├── integration/
    │   ├── test_sum.rs              # SUM 集成测试
    │   ├── test_count.rs            # COUNT 集成测试
    │   └── test_extension.rs        # 扩展加载测试
    └── sql/
        └── aggregate.test           # SQL 逻辑测试
```

## 第一阶段实现计划

### Phase 1.1: 项目脚手架搭建

1. **初始化 Cargo 项目**
   - 创建 `Cargo.toml`，添加依赖（`duckdb`, `substrait`, `prost` 等）
   - 配置为 `cdylib`（动态库）输出

2. **DuckDB 扩展框架**
   - 实现扩展入口点 (`sirius_rust_init`, `sirius_rust_version`)
   - 注册 table function: `sirius_process_substrait`
   - 实现 bind/execute 回调

3. **错误处理框架**
   - 定义 `SiriusError` 枚举
   - 实现与 DuckDB 错误的转换

### Phase 1.2: Substrait 集成

1. **Substrait Plan 消费**
   - 解析 Substrait `Plan` protobuf
   - 提取 `AggregateRel` 中的聚合函数信息
   - 映射 Substrait 类型到内部类型

2. **逻辑计划转换**
   - 将 Substrait plan 转为内部 `LogicalPlan` 表示
   - 支持 `ReadRel` → TableScan
   - 支持 `AggregateRel` → Aggregate

### Phase 1.3: 聚合函数实现

1. **AggregateFunction Trait**
   ```rust
   pub trait AggregateFunction: Send + Sync {
       fn name(&self) -> &str;
       fn return_type(&self, input_types: &[DataType]) -> DataType;
       fn create_state(&self) -> Box<dyn AggregateState>;
       fn update(&self, state: &mut dyn AggregateState, input: &Column) -> Result<()>;
       fn merge(&self, state: &mut dyn AggregateState, other: &dyn AggregateState) -> Result<()>;
       fn finalize(&self, state: &dyn AggregateState) -> Result<Value>;
   }
   ```

2. **SUM 实现**
   - 支持 INT32, INT64, FLOAT, DOUBLE 类型
   - 溢出检测（INT → INT64 提升）
   - NULL 处理

3. **COUNT 实现**
   - COUNT(*) — 计数所有行
   - COUNT(column) — 计数非 NULL 值
   - 返回 INT64

### Phase 1.4: 执行引擎

1. **Pipeline 执行**
   - 从 DuckDB 获取数据（通过 table function 参数或直接扫描）
   - 执行聚合计算
   - 将结果返回 DuckDB

2. **DataChunk 处理**
   - 与 DuckDB 的 DataChunk 格式互转
   - 列式数据的高效处理

### Phase 1.5: 测试

1. **单元测试**：每个聚合函数的正确性
2. **集成测试**：通过 DuckDB 加载扩展并执行 SQL
3. **SQL 逻辑测试**：标准 SQL 测试用例

## DuckDB 扩展交互方式

### 使用流程（与原版 sirius 类似）

```sql
-- 加载扩展
LOAD 'sirius_rust';

-- 方式1：直接处理 substrait plan
SELECT * FROM sirius_process_substrait(substrait_plan_blob);

-- 方式2：注册为标量/聚合函数（第一阶段简化方案）
SELECT sirius_sum(column1), sirius_count(column1) FROM table1;
```

### 第一阶段简化方案

考虑到完整的 Substrait 流程较复杂，第一阶段采用**双轨策略**：

1. **直接注册聚合函数**（优先实现，可快速验证）
   - 向 DuckDB 注册 `sirius_sum`, `sirius_count` 聚合函数
   - 直接在 DuckDB 的聚合框架内运行

2. **Substrait 管道**（同步搭建框架，后续完善）
   - 搭建 Substrait 解析框架
   - 预留 `sirius_process_substrait` table function

## 预留扩展点

### 聚合函数扩展

在 `operator/aggregate/mod.rs` 中维护函数注册表：

```rust
pub fn register_all_aggregates(registry: &mut FunctionRegistry) {
    // Phase 1
    registry.register(Box::new(SumFunction));
    registry.register(Box::new(CountFunction));

    // Phase 2 (预留)
    // registry.register(Box::new(AvgFunction));
    // registry.register(Box::new(MinFunction));
    // registry.register(Box::new(MaxFunction));
    // registry.register(Box::new(CountDistinctFunction));
}
```

### 算子扩展

在 `operator/mod.rs` 中预留算子注册：

```rust
pub enum PhysicalOperator {
    TableScan(TableScanOperator),
    Filter(FilterOperator),       // 预留
    Projection(ProjectionOperator), // 预留
    Aggregate(AggregateOperator),
    Join(JoinOperator),           // 预留
    OrderBy(OrderByOperator),     // 预留
    Limit(LimitOperator),         // 预留
}
```

## 实施步骤

| 步骤 | 内容 | 预期产出 |
|------|------|---------|
| 1 | Cargo 项目初始化 + DuckDB 扩展入口 | 可编译的空扩展 |
| 2 | 数据类型 + Column 抽象 | `data/` 模块 |
| 3 | AggregateFunction trait + SUM/COUNT | `operator/aggregate/` |
| 4 | DuckDB 聚合函数注册 | 可在 DuckDB 中调用 |
| 5 | Substrait 解析框架（预留） | `plan/` 模块骨架 |
| 6 | 集成测试 | 端到端验证 |
| 7 | 预留算子骨架文件 | 完整目录结构 |

## 依赖清单

```toml
[dependencies]
duckdb = "1.1"                    # DuckDB Rust bindings
duckdb-loadable-macros = "0.1"    # DuckDB 扩展宏
cudarc = "0.12"                   # Rust CUDA 绑定（GPU kernel 管理）
substrait = "0.46"                # Substrait protobuf 定义
prost = "0.13"                    # Protobuf runtime
thiserror = "2"                   # 错误处理
num-traits = "0.2"                # 数值 trait（SUM 泛型实现）

[dev-dependencies]
duckdb = { version = "1.1", features = ["bundled"] }

[build-dependencies]
cc = { version = "1", features = ["cuda"] }  # 编译 .cu kernel 文件
```

## 跨平台支持

代码必须同时在 **Linux** 和 **Windows** 上编译运行。

### 策略
- GPU 通过 Cargo feature gate 可选：`features = ["gpu"]`
- `#[cfg(feature = "gpu")]` 条件编译隔离 CUDA 代码
- 无 GPU 时 `cargo build` 和 `cargo test` 也能通过（CPU-only 模式）
- 路径处理用 `std::path::Path`，不硬编码分隔符
- DuckDB 扩展输出由 Cargo `cdylib` 自动处理（.so / .dll）

### Windows 环境要求
- Rust MSVC 工具链（`rustup default stable-x86_64-pc-windows-msvc`）
- Visual Studio Build Tools（C++ 桌面开发）
- CUDA Toolkit 12.x
- DuckDB Windows 版

## Benchmark 方案

### 测试集
**TPC-H 多规模**：SF0.1 → SF0.5 → SF1 → SF5 → SF10

论文定位：SF1 在学术界属于小数据集（功能验证级），需要多规模测试展示可扩展性。

### 硬件
用户个人笔记本：**Intel i5 CPU + NVIDIA RTX 3050 Ti (4GB VRAM)**

### TPC-H 各规模 lineitem 表数据量（主要测试对象）

| Scale Factor | lineitem 行数 | 数据大小 | 是否超 VRAM (4GB) | 定位 |
|--------------|--------------|---------|-------------------|------|
| SF0.1 | ~60 万行 | ~73MB | 否 | 小数据基准，预期 GPU 无优势 |
| SF0.5 | ~300 万行 | ~362MB | 否 | 中间过渡 |
| SF1 | ~600 万行 | ~725MB | 否 | GPU 开始展示优势 |
| SF5 | ~3000 万行 | ~3.6GB | 临界 | GPU 主战场 |
| SF10 | ~6000 万行 | ~7.2GB | 是 | 需分批传输，展示内存管理能力 |

### GPU 显存策略

- **SF0.1 ~ SF5**：数据一次性加载到 GPU 显存
- **SF10**：数据超出 4GB VRAM，需实现**分批传输机制**（Host→Device 分块 + 部分归约 + 合并）
- 分批处理本身是论文的技术亮点之一，展示工程能力

### 对比组

| 编号 | 引擎 | 说明 |
|------|------|------|
| A | DuckDB 原生 | Baseline，DuckDB 内置聚合 |
| B | sirius-rust CPU | GPU feature 关闭，纯 Rust CPU 实现 |
| C | sirius-rust GPU | RTX 3050 Ti 加速 |

### 测试查询（第一阶段：SUM + COUNT）

```sql
-- Q1: 单列 SUM（lineitem 600万行）
SELECT SUM(l_extendedprice) FROM lineitem;

-- Q2: 单列 COUNT
SELECT COUNT(l_orderkey) FROM lineitem;

-- Q3: COUNT(*)
SELECT COUNT(*) FROM lineitem;

-- Q4: 带 GROUP BY 的 SUM（模拟 TPC-H Q1 简化版）
SELECT l_returnflag, l_linestatus,
       SUM(l_quantity), SUM(l_extendedprice), COUNT(*)
FROM lineitem
GROUP BY l_returnflag, l_linestatus;

-- Q5: 跨规模对比（在 SF0.1 / SF0.5 / SF1 / SF5 / SF10 上各跑一遍）
-- 同一条查询，不同数据规模，画出可扩展性曲线
```

### 指标

| 指标 | 说明 |
|------|------|
| 执行时间 | 毫秒，取 10 次中位数（排除首次 warmup） |
| 吞吐量 | million rows/sec |
| 加速比 | GPU 时间 / CPU 时间 |
| GPU 利用率 | nvidia-smi 采样 |

### 预期结果分析

#### 预期加速比（按规模）

| Scale Factor | lineitem 行数 | 预期加速比 | 分析 |
|--------------|--------------|-----------|------|
| SF0.1 | 60 万 | **0.3-0.8x** | GPU 反而慢，传输开销 > 计算收益 |
| SF0.5 | 300 万 | **1-3x** | 临界点，开始持平或小幅领先 |
| SF1 | 600 万 | **2-5x** | GPU 优势显现 |
| SF5 | 3000 万 | **4-8x** | GPU 主战场，CUDA core 充分利用 |
| SF10 | 6000 万 | **3-7x** | 受分批传输开销影响，但仍有明显加速 |

#### 论文核心图表（3 张）

1. **Scale Factor vs 执行时间**：三条线（DuckDB / CPU / GPU），展示 GPU 在大规模时绝对时间优势
2. **Scale Factor vs 加速比**：一条曲线，从 <1x 上升到 peak 再因分批传输略降 — 展示 GPU 适用边界
3. **SF10 分批传输分析**：batch size vs 执行时间，展示显存管理策略的工程决策

#### 论文叙事策略

**核心论点：** "GPU 加速比随数据规模增长呈上升趋势，在 SF5 (3000万行) 达到峰值加速比；当数据超出 GPU 显存时，通过分批传输机制仍能维持显著加速。"

**诚实讨论（加分项）：**
- SF0.1 GPU 反而慢 → 分析 Host→Device 传输延迟 vs 计算收益的交叉点
- SF10 加速比略降 → 分析分批传输的额外开销，讨论更大 VRAM 的潜在收益
- SUM/COUNT 是 memory-bound 操作 → 加速比上限受显存带宽（192 GB/s）限制，不是计算瓶颈
- 3050 Ti 笔记本功耗受限（60-80W）→ 桌面 GPU 预期更好

**这种诚实分析比单纯报一个大加速数字更有说服力，体现你理解 GPU 计算的本质。**

## 关键设计决策

1. **Substrait 是桥梁**：与原版一致，Substrait 作为查询计划的中间表示，实现与 DuckDB 的解耦
2. **先注册聚合函数，再走 Substrait 管道**：降低第一阶段复杂度
3. **GPU 加速是核心**：通过 cudarc 管理 CUDA kernel，SUM/COUNT 在 GPU 上并行计算
4. **CPU Fallback**：GPU 不可用或数据量太小时回退 CPU，保证鲁棒性
5. **列式处理**：与 DuckDB 的向量化引擎对齐，使用列式数据结构
6. **Trait 驱动**：通过 trait 定义算子和函数接口，GPU/CPU 实现统一接口
