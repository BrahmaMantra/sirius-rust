use sirius_rust::data::column::Column;
use sirius_rust::data::types::DataType;
use sirius_rust::executor::mock_gpu::MockGpuExecutor;
use sirius_rust::executor::AggregateExecutor;
use sirius_rust::SiriusContext;

fn gpu_ctx() -> SiriusContext {
    SiriusContext::with_executor(Box::new(MockGpuExecutor::new()))
}

#[test]
fn test_gpu_sum_matches_cpu() {
    let cpu_ctx = SiriusContext::new();
    let gpu_ctx = gpu_ctx();
    let col = Column::from_i32((1..=100_000).collect());
    let cpu = cpu_ctx.executor().sum(&col).unwrap();
    let gpu = gpu_ctx.executor().sum(&col).unwrap();
    assert_eq!(cpu.as_i64(), gpu.as_i64());
}

#[test]
fn test_gpu_count_matches_cpu() {
    let cpu_ctx = SiriusContext::new();
    let gpu_ctx = gpu_ctx();
    let col = Column::from_i32((1..=100_000).collect());
    let cpu = cpu_ctx.executor().count(&col).unwrap();
    let gpu = gpu_ctx.executor().count(&col).unwrap();
    assert_eq!(cpu.as_i64(), gpu.as_i64());
}

#[test]
fn test_gpu_sum_with_nulls() {
    let ctx = gpu_ctx();
    let data: Vec<u8> = [10i32, 0, 30, 0, 50].iter().flat_map(|v| v.to_ne_bytes()).collect();
    let col = Column::from_raw_with_validity(DataType::Int32, data, vec![0b10101u64], 5);
    assert_eq!(ctx.executor().sum(&col).unwrap().as_i64(), Some(90));
}

#[test]
fn test_gpu_count_with_nulls() {
    let ctx = gpu_ctx();
    let col = Column::from_raw_with_validity(DataType::Int32, vec![0u8; 4 * 5], vec![0b10101u64], 5);
    assert_eq!(ctx.executor().count(&col).unwrap().as_i64(), Some(3));
}

#[test]
fn test_gpu_sum_f64() {
    let ctx = gpu_ctx();
    let col = Column::from_f64(vec![1.5, 2.5, 3.0, 4.0]);
    assert!((ctx.executor().sum(&col).unwrap().as_f64().unwrap() - 11.0).abs() < f64::EPSILON);
}

#[test]
fn test_gpu_sum_all_null_returns_null() {
    let ctx = gpu_ctx();
    let col = Column::from_raw_with_validity(DataType::Int32, vec![0u8; 4 * 3], vec![0u64], 3);
    assert!(ctx.executor().sum(&col).unwrap().is_null());
}

#[test]
fn test_batch_size_calculation() {
    let exec = MockGpuExecutor::new();
    assert_eq!(exec.compute_batch_size(8, 1_000_000), 1_000_000);

    let small = MockGpuExecutor::with_vram(8000);
    let batch = small.compute_batch_size(8, 1_000_000);
    assert!(batch < 1_000_000);
    assert_eq!(batch, 750);
}
