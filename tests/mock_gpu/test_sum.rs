use sirius_rust::data::column::Column;
use sirius_rust::data::types::DataType;
use sirius_rust::gpu_executor::{execute_aggregate, execute_aggregate_batched};
use sirius_rust::op::aggregate::sum::SumFunction;
use sirius_rust::op::aggregate::traits::AggregateFunction;
use sirius_rust::SiriusContext;

#[test]
fn test_sum_large_dataset() {
    let ctx = SiriusContext::new();
    let func = SumFunction;
    let data: Vec<i32> = (1..=100_000).collect();
    let expected: i64 = (1..=100_000i64).sum();
    let col = Column::from_i32(data);
    let result = execute_aggregate(&ctx, &func, &col).unwrap();
    assert_eq!(result.as_i64(), Some(expected));
}

#[test]
fn test_sum_batched_simulates_gpu_chunking() {
    let ctx = SiriusContext::new();
    let func = SumFunction;
    let chunks: Vec<Column> = (0..10)
        .map(|i| {
            let start = i * 10_000 + 1;
            let end = (i + 1) * 10_000;
            Column::from_i32((start..=end).collect())
        })
        .collect();
    let expected: i64 = (1..=100_000i64).sum();
    let result = execute_aggregate_batched(&ctx, &func, &chunks).unwrap();
    assert_eq!(result.as_i64(), Some(expected));
}

#[test]
fn test_sum_mixed_null_batches() {
    let ctx = SiriusContext::new();
    let func = SumFunction;
    let data1: Vec<u8> = [10i32, 0i32, 30i32].iter().flat_map(|v| v.to_ne_bytes()).collect();
    let col1 = Column::from_raw_with_validity(DataType::Int32, data1, vec![0b101], 3);
    let data2: Vec<u8> = [0i32, 50i32, 60i32].iter().flat_map(|v| v.to_ne_bytes()).collect();
    let col2 = Column::from_raw_with_validity(DataType::Int32, data2, vec![0b110], 3);
    let result = execute_aggregate_batched(&ctx, &func, &[col1, col2]).unwrap();
    assert_eq!(result.as_i64(), Some(150));
}

#[test]
fn test_sum_f64_precision() {
    let func = SumFunction;
    let mut state = func.create_state();
    let col = Column::from_f64(vec![0.1; 1000]);
    func.update(state.as_mut(), &col).unwrap();
    let result = func.finalize(state.as_ref()).unwrap();
    let sum = result.as_f64().unwrap();
    assert!((sum - 100.0).abs() < 0.01, "Expected ~100.0, got {sum}");
}

#[test]
fn test_sum_return_type_promotion() {
    let func = SumFunction;
    assert_eq!(func.return_type(&[DataType::Int8]).unwrap(), DataType::BigInt);
    assert_eq!(func.return_type(&[DataType::Int16]).unwrap(), DataType::BigInt);
    assert_eq!(func.return_type(&[DataType::Int32]).unwrap(), DataType::BigInt);
    assert_eq!(func.return_type(&[DataType::Int64]).unwrap(), DataType::BigInt);
    assert_eq!(func.return_type(&[DataType::Float32]).unwrap(), DataType::Float64);
    assert_eq!(func.return_type(&[DataType::Float64]).unwrap(), DataType::Float64);
    assert!(func.return_type(&[DataType::Boolean]).is_err());
    assert!(func.return_type(&[]).is_err());
    assert!(func.return_type(&[DataType::Int32, DataType::Int32]).is_err());
}
