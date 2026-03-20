//! GPU 设备管理 — 对应 sirius/src/gpu_context.cpp
//!
//! CUDA 设备初始化和流管理，不属于 cuda/ 目录（cuda/ 只放 kernel 代码）。

#[cfg(feature = "gpu")]
use crate::error::{Result, SiriusError};
#[cfg(feature = "gpu")]
use cudarc::driver::{CudaDevice, CudaStream};
#[cfg(feature = "gpu")]
use std::sync::Arc;

/// GPU 执行上下文 — 管理 CUDA 设备和流
#[cfg(feature = "gpu")]
pub struct GpuContext {
    device: Arc<CudaDevice>,
}

#[cfg(feature = "gpu")]
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
