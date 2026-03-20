use sirius_rust::data::column::Column;
use sirius_rust::data::types::DataType;
use sirius_rust::cuda::backend::{GpuBackend, MockGpuBackend};
use sirius_rust::gpu_executor::execute_aggregate;
use sirius_rust::op::aggregate::count::CountFunction;
use sirius_rust::op::aggregate::sum::SumFunction;
use sirius_rust::SiriusContext;

fn gpu_ctx() -> SiriusContext {
    SiriusContext::with_backend(Box::new(MockGpuBackend::new()))
}

#[test]
fn test_gpu_sum_matches_cpu() {
    let cpu_ctx = SiriusContext::new();
    let gpu_ctx = gpu_ctx();
    let func = SumFunction;
    let col = Column::from_i32((1..=100_000).collect());
    let cpu = execute_aggregate(&cpu_ctx, &func, &col).unwrap();
    let gpu = execute_aggregate(&gpu_ctx, &func, &col).unwrap();
    assert_eq!(cpu.as_i64(), gpu.as_i64());
}

#[test]
fn test_gpu_count_matches_cpu() {
    let cpu_ctx = SiriusContext::new();
    let gpu_ctx = gpu_ctx();
    let func = CountFunction;
    let col = Column::from_i32((1..=100_000).collect());
    let cpu = execute_aggregate(&cpu_ctx, &func, &col).unwrap();
    let gpu = execute_aggregate(&gpu_ctx, &func, &col).unwrap();
    assert_eq!(cpu.as_i64(), gpu.as_i64());
}

#[test]
fn test_gpu_sum_with_nulls() {
    let ctx = gpu_ctx();
    let func = SumFunction;
    let data: Vec<u8> = [10i32, 0, 30, 0, 50].iter().flat_map(|v| v.to_ne_bytes()).collect();
    let col = Column::from_raw_with_validity(DataType::Int32, data, vec![0b10101u64], 5);
    let result = execute_aggregate(&ctx, &func, &col).unwrap();
    assert_eq!(result.as_i64(), Some(90));
}

#[test]
fn test_gpu_count_with_nulls() {
    let ctx = gpu_ctx();
    let func = CountFunction;
    let col = Column::from_raw_with_validity(DataType::Int32, vec![0u8; 4 * 5], vec![0b10101u64], 5);
    let result = execute_aggregate(&ctx, &func, &col).unwrap();
    assert_eq!(result.as_i64(), Some(3));
}

#[test]
fn test_gpu_sum_f64() {
    let ctx = gpu_ctx();
    let func = SumFunction;
    let col = Column::from_f64(vec![1.5, 2.5, 3.0, 4.0]);
    let result = execute_aggregate(&ctx, &func, &col).unwrap();
    assert!((result.as_f64().unwrap() - 11.0).abs() < f64::EPSILON);
}

#[test]
fn test_gpu_sum_all_null_returns_null() {
    let ctx = gpu_ctx();
    let func = SumFunction;
    let col = Column::from_raw_with_validity(DataType::Int32, vec![0u8; 4 * 3], vec![0u64], 3);
    let result = execute_aggregate(&ctx, &func, &col).unwrap();
    assert!(result.is_null());
}

#[test]
fn test_batch_size_calculation() {
    let backend = MockGpuBackend::new();
    assert_eq!(backend.compute_batch_size(8, 1_000_000).unwrap(), 1_000_000);

    let small = MockGpuBackend::with_vram(8000);
    let batch = small.compute_batch_size(8, 1_000_000).unwrap();
    assert!(batch < 1_000_000);
    assert_eq!(batch, 750);
}
