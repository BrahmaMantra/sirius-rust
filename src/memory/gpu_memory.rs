//! Host↔Device 数据传输

use crate::error::{Result, SiriusError};
use cudarc::driver::{CudaDevice, CudaSlice};
use std::sync::Arc;

pub struct GpuMemory {
    device: Arc<CudaDevice>,
}

impl GpuMemory {
    pub fn new(device: Arc<CudaDevice>) -> Self {
        Self { device }
    }

    pub fn host_to_device<T: cudarc::driver::DeviceRepr>(
        &self,
        data: &[T],
    ) -> Result<CudaSlice<T>> {
        self.device
            .htod_sync_copy(data)
            .map_err(|e| SiriusError::GpuError(format!("Host to device copy failed: {e}")))
    }

    pub fn device_to_host<T: cudarc::driver::DeviceRepr + Default + Clone>(
        &self,
        gpu_data: &CudaSlice<T>,
    ) -> Result<Vec<T>> {
        self.device
            .dtoh_sync_copy(gpu_data)
            .map_err(|e| SiriusError::GpuError(format!("Device to host copy failed: {e}")))
    }

    pub fn alloc_zeros<T: cudarc::driver::DeviceRepr + cudarc::driver::ValidAsZeroBits>(
        &self,
        len: usize,
    ) -> Result<CudaSlice<T>> {
        self.device
            .alloc_zeros(len)
            .map_err(|e| SiriusError::GpuError(format!("GPU allocation failed: {e}")))
    }

    pub fn available_memory(&self) -> Result<(usize, usize)> {
        cudarc::driver::result::mem_get_info()
            .map_err(|e| SiriusError::GpuError(format!("Failed to query memory: {e}")))
    }

    pub fn compute_batch_size(&self, element_size: usize, total_elements: usize) -> Result<usize> {
        let (free, _total) = self.available_memory()?;
        let usable = free * 3 / 4;
        let max_elements = usable / element_size;
        if max_elements >= total_elements {
            Ok(total_elements)
        } else {
            Ok(max_elements)
        }
    }
}
