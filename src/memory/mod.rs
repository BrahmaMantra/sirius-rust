//! GPU 内存管理 — 对应 sirius/src/memory/

#[cfg(feature = "gpu")]
mod gpu_memory;

#[cfg(feature = "gpu")]
pub use gpu_memory::GpuMemory;

// CPU-only 模式下提供空占位
#[cfg(not(feature = "gpu"))]
pub struct GpuMemory;
