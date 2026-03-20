use sirius_rust::data::column::Column;
use sirius_rust::op::aggregate::AggregateRegistry;

#[test]
fn test_registry_lookup_and_execute() {
    let registry = AggregateRegistry::new();
    let sum_func = registry.get("sum").expect("sum should be registered");
    let mut state = sum_func.create_state();
    let col = Column::from_i32(vec![10, 20, 30]);
    sum_func.update(state.as_mut(), &col).unwrap();
    assert_eq!(sum_func.finalize(state.as_ref()).unwrap().as_i64(), Some(60));

    let count_func = registry.get("count").expect("count should be registered");
    let mut state = count_func.create_state();
    count_func.update(state.as_mut(), &col).unwrap();
    assert_eq!(count_func.finalize(state.as_ref()).unwrap().as_i64(), Some(3));
}

#[test]
fn test_registry_unknown_function() {
    let registry = AggregateRegistry::new();
    assert!(registry.get("unknown_func").is_none());
    assert!(registry.get("avg").is_none());
}
