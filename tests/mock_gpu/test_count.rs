use sirius_rust::data::column::Column;
use sirius_rust::data::types::DataType;
use sirius_rust::gpu_executor::execute_aggregate;
use sirius_rust::op::aggregate::count::{CountFunction, CountStarFunction};
use sirius_rust::op::aggregate::traits::AggregateFunction as _;
use sirius_rust::SiriusContext;

#[test]
fn test_count_large_dataset() {
    let ctx = SiriusContext::new();
    let func = CountFunction;
    let col = Column::from_i64((1..=100_000).collect());
    let result = execute_aggregate(&ctx, &func, &col).unwrap();
    assert_eq!(result.as_i64(), Some(100_000));
}

#[test]
fn test_count_star_includes_nulls() {
    let ctx = SiriusContext::new();
    let func = CountStarFunction;
    let col = Column::from_raw_with_validity(DataType::Int32, vec![0u8; 4 * 5], vec![0b10101u64], 5);
    let result = execute_aggregate(&ctx, &func, &col).unwrap();
    assert_eq!(result.as_i64(), Some(5));
}

#[test]
fn test_count_excludes_nulls() {
    let ctx = SiriusContext::new();
    let func = CountFunction;
    let col = Column::from_raw_with_validity(DataType::Int32, vec![0u8; 4 * 5], vec![0b10101u64], 5);
    let result = execute_aggregate(&ctx, &func, &col).unwrap();
    assert_eq!(result.as_i64(), Some(3));
}

#[test]
fn test_count_all_nulls() {
    let ctx = SiriusContext::new();
    let func = CountFunction;
    let col = Column::from_raw_with_validity(DataType::Int32, vec![0u8; 4 * 3], vec![0u64], 3);
    let result = execute_aggregate(&ctx, &func, &col).unwrap();
    assert_eq!(result.as_i64(), Some(0));
}

#[test]
fn test_count_vs_count_star_consistency() {
    let count_func = CountFunction;
    let count_star_func = CountStarFunction;

    let col = Column::from_i32(vec![1, 2, 3, 4, 5]);
    let mut s1 = count_func.create_state();
    count_func.update(s1.as_mut(), &col).unwrap();
    let mut s2 = count_star_func.create_state();
    count_star_func.update(s2.as_mut(), &col).unwrap();
    assert_eq!(count_func.finalize(s1.as_ref()).unwrap().as_i64(),
               count_star_func.finalize(s2.as_ref()).unwrap().as_i64());

    let col_null = Column::from_raw_with_validity(DataType::Int32, vec![0u8; 4 * 4], vec![0b1010u64], 4);
    let mut s1 = count_func.create_state();
    count_func.update(s1.as_mut(), &col_null).unwrap();
    let mut s2 = count_star_func.create_state();
    count_star_func.update(s2.as_mut(), &col_null).unwrap();
    assert_eq!(count_func.finalize(s1.as_ref()).unwrap().as_i64(), Some(2));
    assert_eq!(count_star_func.finalize(s2.as_ref()).unwrap().as_i64(), Some(4));
}
