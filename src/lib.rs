// sirius-rust — GPU 加速 DuckDB 扩展
//
// 目录结构对齐 sirius C++：
//   sirius_extension  — DuckDB 扩展入口 (C_STRUCT ABI + 传统入口)
//   sirius_context    — 全局执行上下文（持有 executor）
//   executor/         — 聚合执行器 (CPU / GPU / MockGPU)
//   gpu_context       — GPU 设备管理（对应 sirius/src/gpu_context.cpp）
//   cuda/             — CUDA kernel 代码（对应 sirius/src/cuda/，只有 .cu）
//   memory/           — GPU 内存管理（对应 sirius/src/memory/）
//   op/               — 算子实现 (aggregate)
//   data/             — 数据表示 (Column, DataChunk, Types)
//   ext_api           — DuckDB C API 函数指针表
//   ffi               — bindgen FFI 绑定
//   gpu_columns       — DuckDB ↔ sirius 数据转换

pub mod data;
pub mod error;
pub mod ext_api;
pub mod ffi;

pub mod executor;
pub mod op;

pub mod gpu_context;
pub mod cuda;
pub mod gpu_columns;

pub mod memory;
pub mod operator;
pub mod pipeline;
pub mod plan;
pub mod planner;

mod sirius_context;
mod sirius_extension;

pub use sirius_context::SiriusContext;
pub use sirius_extension::sirius_rust_init;
pub use sirius_extension::sirius_rust_init_c_api;
pub use sirius_extension::sirius_rust_version;
