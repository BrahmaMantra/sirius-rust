//! GPU SUM kernel Rust wrapper

use crate::data::column::Column;
use crate::data::types::DataType;
use crate::error::{Result, SiriusError};
use crate::gpu_context::GpuContext;
use crate::memory::GpuMemory;
use crate::op::aggregate::traits::AggregateValue;

const BLOCK_SIZE: u32 = 256;

pub fn gpu_sum(ctx: &GpuContext, mem: &GpuMemory, input: &Column) -> Result<AggregateValue> {
    if input.is_empty() {
        return Ok(AggregateValue::Null);
    }
    match input.data_type() {
        DataType::Int32 => gpu_sum_i64_from_i32(ctx, mem, input),
        DataType::Int64 | DataType::BigInt => gpu_sum_i64(ctx, mem, input),
        DataType::Float32 => gpu_sum_f64_from_f32(ctx, mem, input),
        DataType::Float64 => gpu_sum_f64(ctx, mem, input),
        other => Err(SiriusError::TypeError(format!("GPU SUM does not support type {other}"))),
    }
}

fn gpu_sum_i64(ctx: &GpuContext, mem: &GpuMemory, input: &Column) -> Result<AggregateValue> {
    let data = input.as_i64_slice();
    let validity = input.validity();
    let n = data.len() as u32;

    let d_data = mem.host_to_device(data)?;
    let d_validity = mem.host_to_device(validity)?;
    let grid_size = (n + BLOCK_SIZE * 2 - 1) / (BLOCK_SIZE * 2);
    let d_output = mem.alloc_zeros::<i64>(grid_size as usize)?;

    let device = ctx.device();
    let ptx = cudarc::nvrtc::compile_ptx(include_str!("sum.cu"))
        .map_err(|e| SiriusError::GpuError(format!("PTX compilation failed: {e}")))?;
    device
        .load_ptx(ptx, "sum_module", &["sum_reduce_i64"])
        .map_err(|e| SiriusError::GpuError(format!("Module load failed: {e}")))?;

    let func = device
        .get_func("sum_module", "sum_reduce_i64")
        .ok_or_else(|| SiriusError::GpuError("Kernel function not found".into()))?;

    let cfg = cudarc::driver::LaunchConfig {
        grid_dim: (grid_size, 1, 1),
        block_dim: (BLOCK_SIZE, 1, 1),
        shared_mem_bytes: BLOCK_SIZE * 8,
    };

    unsafe {
        func.launch(cfg, (&d_data, &d_validity, &d_output, n))
            .map_err(|e| SiriusError::GpuError(format!("Kernel launch failed: {e}")))?;
    }

    let partial_sums = mem.device_to_host(&d_output)?;
    Ok(AggregateValue::Int64(partial_sums.iter().sum()))
}

fn gpu_sum_i64_from_i32(ctx: &GpuContext, mem: &GpuMemory, input: &Column) -> Result<AggregateValue> {
    let data_i64: Vec<i64> = input.as_i32_slice().iter().map(|&v| v as i64).collect();
    let promoted = Column::from_raw_with_validity(
        DataType::Int64,
        unsafe { std::slice::from_raw_parts(data_i64.as_ptr() as *const u8, data_i64.len() * 8).to_vec() },
        input.validity().to_vec(),
        input.len(),
    );
    gpu_sum_i64(ctx, mem, &promoted)
}

fn gpu_sum_f64(ctx: &GpuContext, mem: &GpuMemory, input: &Column) -> Result<AggregateValue> {
    let data = input.as_f64_slice();
    let validity = input.validity();
    let n = data.len() as u32;

    let d_data = mem.host_to_device(data)?;
    let d_validity = mem.host_to_device(validity)?;
    let grid_size = (n + BLOCK_SIZE * 2 - 1) / (BLOCK_SIZE * 2);
    let d_output = mem.alloc_zeros::<f64>(grid_size as usize)?;

    let device = ctx.device();
    let ptx = cudarc::nvrtc::compile_ptx(include_str!("sum.cu"))
        .map_err(|e| SiriusError::GpuError(format!("PTX compilation failed: {e}")))?;
    device
        .load_ptx(ptx, "sum_module_f64", &["sum_reduce_f64"])
        .map_err(|e| SiriusError::GpuError(format!("Module load failed: {e}")))?;

    let func = device
        .get_func("sum_module_f64", "sum_reduce_f64")
        .ok_or_else(|| SiriusError::GpuError("Kernel function not found".into()))?;

    let cfg = cudarc::driver::LaunchConfig {
        grid_dim: (grid_size, 1, 1),
        block_dim: (BLOCK_SIZE, 1, 1),
        shared_mem_bytes: BLOCK_SIZE * 8,
    };

    unsafe {
        func.launch(cfg, (&d_data, &d_validity, &d_output, n))
            .map_err(|e| SiriusError::GpuError(format!("Kernel launch failed: {e}")))?;
    }

    let partial_sums = mem.device_to_host(&d_output)?;
    Ok(AggregateValue::Float64(partial_sums.iter().sum()))
}

fn gpu_sum_f64_from_f32(ctx: &GpuContext, mem: &GpuMemory, input: &Column) -> Result<AggregateValue> {
    let data_f64: Vec<f64> = input.as_f32_slice().iter().map(|&v| v as f64).collect();
    let promoted = Column::from_raw_with_validity(
        DataType::Float64,
        unsafe { std::slice::from_raw_parts(data_f64.as_ptr() as *const u8, data_f64.len() * 8).to_vec() },
        input.validity().to_vec(),
        input.len(),
    );
    gpu_sum_f64(ctx, mem, &promoted)
}
