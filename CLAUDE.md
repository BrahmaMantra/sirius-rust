# Sirius-Rust

## 项目简介

用 Rust + CUDA 实现的 **GPU 加速 DuckDB 扩展**，参考 [sirius](https://github.com/sirius-db/sirius) 架构。毕业设计项目，核心亮点是 GPU 加速聚合查询。

## 记忆层

项目记忆存储在 `.memory/` 目录，索引文件为 `.memory/MEMORY.md`。
- 每次会话开始时读取 `.memory/MEMORY.md`
- 工作结束时更新进展日志 `progress_YYYY-MM-DD.md`
- 详细规划见 `PLAN.md`

## 架构

```
SQL → DuckDB → Substrait Plan → sirius-rust → GPU 执行 (fallback CPU) → 返回结果
```

- **DuckDB 扩展**：`cdylib` 形式，通过 DuckDB 扩展 API 加载
- **Substrait 交互**：查询计划通过 Substrait protobuf 在 DuckDB 和 sirius-rust 之间传递
- **GPU 加速**：CUDA kernel 实现聚合算子并行计算（通过 cudarc）
- **CPU Fallback**：GPU 不可用或数据量太小时回退 CPU

## 开发阶段

### 第一阶段（当前）
- [x] 项目规划与架构设计
- [ ] Cargo 项目初始化 + DuckDB 扩展框架
- [ ] 数据类型抽象层（Column, DataChunk, Types）
- [ ] GPU 层搭建（CUDA context, 内存管理）
- [ ] SUM 聚合函数（GPU kernel + CPU fallback）
- [ ] COUNT 聚合函数（GPU kernel + CPU fallback）
- [ ] Substrait 解析骨架（预留）
- [ ] 集成测试 + Benchmark

### 第二阶段（预留）
- AVG, MIN, MAX, COUNT_DISTINCT
- Filter, Projection 算子
- GroupBy 支持

### 第三阶段（预留）
- Join, OrderBy, Limit 算子
- 完整 Substrait 管道

## 技术栈

| 组件 | 选择 | 说明 |
|------|------|------|
| 语言 | Rust | 安全、高性能 |
| GPU | cudarc | Rust CUDA 绑定 |
| DuckDB | duckdb-loadable-macros + C FFI | 扩展 API |
| 查询计划 | substrait + prost | Substrait protobuf |
| CUDA 编译 | cc (cuda feature) | build.rs 编译 .cu |
| 错误处理 | thiserror | 错误类型 |

## 跨平台

必须同时支持 **Linux** 和 **Windows**（用户有两台机器）。
- GPU 通过 Cargo feature gate 可选：`--features gpu`
- `#[cfg(feature = "gpu")]` 条件编译隔离 CUDA 代码
- 无 GPU 环境 `cargo build` / `cargo test` 也能通过

## Benchmark

**测试集**：TPC-H 多规模 SF0.1 / SF0.5 / SF1 / SF5 / SF10
**硬件**：Intel i5 + RTX 3050 Ti (4GB VRAM)
**对比组**：DuckDB 原生 / sirius-rust CPU / sirius-rust GPU

关键点：
- SF1 在学术界算小数据集，必须多规模展示可扩展性
- SF10 (7.2GB) 超出 VRAM，需实现分批传输（Host→Device 分块 + 部分归约 + 合并）
- 论文画 3 张图：SF vs 执行时间、SF vs 加速比、分批传输分析

## 编码规范

- Rust 惯用写法，`cargo clippy` 零警告
- 不可变优先
- 文件 < 800 行，函数 < 50 行
- GPU kernel 和 CPU 实现通过 trait 统一接口
- 所有公开 API 有文档注释

## 构建命令

```bash
# CPU-only 模式
cargo build --release

# GPU 模式（需要 CUDA Toolkit）
cargo build --release --features gpu

# 测试
cargo test                    # CPU 测试
cargo test --features gpu     # GPU + CPU 测试
```

## 目录结构

```
src/
├── lib.rs                     # 扩展入口
├── extension.rs               # DuckDB 扩展初始化
├── error.rs                   # 错误类型
├── data/                      # 数据表示
│   ├── types.rs               # 数据类型映射
│   ├── column.rs              # 列式数据
│   └── chunk.rs               # DataChunk
├── operator/                  # 算子
│   ├── traits.rs              # 算子 trait
│   └── aggregate/             # 聚合算子
│       ├── traits.rs          # AggregateFunction trait
│       ├── sum.rs             # SUM ✅
│       ├── count.rs           # COUNT ✅
│       ├── avg.rs             # (预留)
│       ├── min.rs             # (预留)
│       └── max.rs             # (预留)
├── gpu/                       # GPU 加速层
│   ├── context.rs             # CUDA 设备管理
│   ├── memory.rs              # GPU 内存/传输
│   └── kernels/               # CUDA kernel
│       ├── sum.cu             # SUM kernel ✅
│       └── count.cu           # COUNT kernel ✅
├── executor/                  # 执行引擎
│   ├── pipeline.rs            # 执行管道
│   └── context.rs             # CPU/GPU 调度
└── plan/                      # 查询计划（预留）
    ├── substrait_consumer.rs  # Substrait 解析
    ├── logical_plan.rs        # 逻辑计划
    └── physical_plan.rs       # 物理计划
```
