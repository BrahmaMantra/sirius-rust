//! GPU COUNT kernel Rust wrapper

use crate::data::column::Column;
use crate::error::{Result, SiriusError};
use crate::gpu_context::GpuContext;
use crate::memory::GpuMemory;
use crate::op::aggregate::traits::AggregateValue;

const BLOCK_SIZE: u32 = 256;

pub fn gpu_count(ctx: &GpuContext, mem: &GpuMemory, input: &Column) -> Result<AggregateValue> {
    if input.is_empty() {
        return Ok(AggregateValue::Int64(0));
    }

    let validity = input.validity();
    let n = input.len() as u32;

    let d_validity = mem.host_to_device(validity)?;
    let grid_size = (n + BLOCK_SIZE * 2 - 1) / (BLOCK_SIZE * 2);
    let d_output = mem.alloc_zeros::<u64>(grid_size as usize)?;

    let device = ctx.device();
    let ptx = cudarc::nvrtc::compile_ptx(include_str!("count.cu"))
        .map_err(|e| SiriusError::GpuError(format!("PTX compilation failed: {e}")))?;
    device
        .load_ptx(ptx, "count_module", &["count_reduce"])
        .map_err(|e| SiriusError::GpuError(format!("Module load failed: {e}")))?;

    let func = device
        .get_func("count_module", "count_reduce")
        .ok_or_else(|| SiriusError::GpuError("Kernel function not found".into()))?;

    let cfg = cudarc::driver::LaunchConfig {
        grid_dim: (grid_size, 1, 1),
        block_dim: (BLOCK_SIZE, 1, 1),
        shared_mem_bytes: BLOCK_SIZE * 8,
    };

    unsafe {
        func.launch(cfg, (&d_validity, &d_output, n))
            .map_err(|e| SiriusError::GpuError(format!("Kernel launch failed: {e}")))?;
    }

    let partial_counts = mem.device_to_host(&d_output)?;
    Ok(AggregateValue::Int64(partial_counts.iter().sum::<u64>() as i64))
}

pub fn gpu_count_star(input: &Column) -> AggregateValue {
    AggregateValue::Int64(input.len() as i64)
}
