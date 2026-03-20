// sirius-rust — GPU 加速 DuckDB 扩展
//
// 目录结构对照 sirius (C++):
//   sirius_extension  — DuckDB 扩展入口
//   sirius_context    — 全局执行上下文
//   gpu_executor      — GPU/CPU 调度执行
//   gpu_context       — CUDA 设备管理
//   gpu_columns       — DuckDB ↔ sirius 数据转换
//   fallback          — GPU → CPU 回退
//   op/               — 算子实现 (aggregate, scan, ...)
//   operator/         — GPU 物理算子
//   cuda/             — CUDA kernel (.cu + Rust wrapper)
//   data/             — 数据表示 (Column, DataChunk, Types)
//   plan/             — 查询计划 (Substrait → GPU plan)
//   planner/          — 物理计划生成器
//   pipeline/         — 执行管道
//   memory/           — GPU 内存管理

pub mod data;
pub mod error;
pub mod ext_api;
pub mod ffi;

pub mod op;
pub mod operator;

pub mod cuda;
pub mod gpu_columns;
pub mod gpu_context;
pub mod gpu_executor;
pub mod fallback;

pub mod pipeline;
pub mod plan;
pub mod planner;
pub mod memory;

mod sirius_context;
mod sirius_extension;

pub use sirius_context::SiriusContext;
pub use sirius_extension::sirius_rust_init;
pub use sirius_extension::sirius_rust_init_c_api;
pub use sirius_extension::sirius_rust_version;
