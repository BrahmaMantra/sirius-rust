//! CUDA 设备管理 — 对应 sirius/src/gpu_context.cpp

#[cfg(feature = "gpu")]
pub use gpu_impl::GpuContext;

#[cfg(feature = "gpu")]
mod gpu_impl {
    use crate::error::{Result, SiriusError};
    use cudarc::driver::{CudaDevice, CudaStream};
    use std::sync::Arc;

    /// GPU 执行上下文 — 管理 CUDA 设备和流
    pub struct GpuContext {
        device: Arc<CudaDevice>,
    }

    impl GpuContext {
        pub fn try_init() -> Result<Self> {
            let device = CudaDevice::new(0).map_err(|e| {
                SiriusError::GpuError(format!("Failed to initialize CUDA device: {e}"))
            })?;
            Ok(Self { device })
        }

        pub fn device(&self) -> &Arc<CudaDevice> {
            &self.device
        }

        pub fn create_stream(&self) -> Result<CudaStream> {
            self.device
                .fork_default_stream()
                .map_err(|e| SiriusError::GpuError(format!("Failed to create stream: {e}")))
        }
    }
}
