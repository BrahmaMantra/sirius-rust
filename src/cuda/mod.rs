//! CUDA 层 — 只包含 CUDA kernel 代码
//!
//! 对应 sirius/src/cuda/，只有 .cu kernel 和 Rust wrapper。
//! 设备管理（GpuContext）在 src/gpu_context.rs，不在这里。

#[cfg(feature = "gpu")]
pub mod kernels;
