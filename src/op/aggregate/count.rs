use crate::data::column::Column;
use crate::data::types::DataType;
use crate::error::{Result, SiriusError};
use crate::op::aggregate::traits::{
    AggregateFunction, AggregateState, AggregateValue,
};
use std::any::Any;

#[derive(Debug, Clone)]
pub struct CountState {
    count: u64,
}

impl Default for CountState {
    fn default() -> Self {
        Self::new()
    }
}

impl CountState {
    pub fn new() -> Self {
        Self { count: 0 }
    }
}

impl AggregateState for CountState {
    fn merge(&mut self, other: &dyn AggregateState) -> Result<()> {
        let other = other
            .as_any()
            .downcast_ref::<CountState>()
            .ok_or_else(|| SiriusError::TypeError("Cannot merge: type mismatch".into()))?;
        self.count += other.count;
        Ok(())
    }

    fn reset(&mut self) {
        self.count = 0;
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// COUNT(column) — 计数非 NULL 值
pub struct CountFunction;

impl AggregateFunction for CountFunction {
    fn name(&self) -> &str {
        "count"
    }

    fn return_type(&self, _input_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::BigInt)
    }

    fn create_state(&self) -> Box<dyn AggregateState> {
        Box::new(CountState::new())
    }

    fn update(&self, state: &mut dyn AggregateState, input: &Column) -> Result<()> {
        let state = state
            .as_any_mut()
            .downcast_mut::<CountState>()
            .ok_or_else(|| SiriusError::TypeError("Invalid state type for COUNT".into()))?;
        state.count += input.valid_count() as u64;
        Ok(())
    }

    fn finalize(&self, state: &dyn AggregateState) -> Result<AggregateValue> {
        let state = state
            .as_any()
            .downcast_ref::<CountState>()
            .ok_or_else(|| SiriusError::TypeError("Invalid state type for COUNT".into()))?;
        Ok(AggregateValue::Int64(state.count as i64))
    }
}

/// COUNT(*) — 计数所有行（包括 NULL）
pub struct CountStarFunction;

impl AggregateFunction for CountStarFunction {
    fn name(&self) -> &str {
        "count_star"
    }

    fn return_type(&self, _input_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::BigInt)
    }

    fn create_state(&self) -> Box<dyn AggregateState> {
        Box::new(CountState::new())
    }

    fn update(&self, state: &mut dyn AggregateState, input: &Column) -> Result<()> {
        let state = state
            .as_any_mut()
            .downcast_mut::<CountState>()
            .ok_or_else(|| {
                SiriusError::TypeError("Invalid state type for COUNT(*)".into())
            })?;
        state.count += input.len() as u64;
        Ok(())
    }

    fn finalize(&self, state: &dyn AggregateState) -> Result<AggregateValue> {
        let state = state
            .as_any()
            .downcast_ref::<CountState>()
            .ok_or_else(|| {
                SiriusError::TypeError("Invalid state type for COUNT(*)".into())
            })?;
        Ok(AggregateValue::Int64(state.count as i64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_no_nulls() {
        let func = CountFunction;
        let mut state = func.create_state();
        let col = Column::from_i32(vec![1, 2, 3, 4, 5]);
        func.update(state.as_mut(), &col).unwrap();
        let result = func.finalize(state.as_ref()).unwrap();
        assert_eq!(result.as_i64(), Some(5));
    }

    #[test]
    fn test_count_with_nulls() {
        let func = CountFunction;
        let mut state = func.create_state();
        let data = vec![0u8; 4 * 5];
        let validity = vec![0b10101u64];
        let col = Column::from_raw_with_validity(DataType::Int32, data, validity, 5);
        func.update(state.as_mut(), &col).unwrap();
        let result = func.finalize(state.as_ref()).unwrap();
        assert_eq!(result.as_i64(), Some(3));
    }

    #[test]
    fn test_count_star() {
        let func = CountStarFunction;
        let mut state = func.create_state();
        let data = vec![0u8; 4 * 5];
        let validity = vec![0b10101u64];
        let col = Column::from_raw_with_validity(DataType::Int32, data, validity, 5);
        func.update(state.as_mut(), &col).unwrap();
        let result = func.finalize(state.as_ref()).unwrap();
        assert_eq!(result.as_i64(), Some(5));
    }

    #[test]
    fn test_count_empty() {
        let func = CountFunction;
        let state = func.create_state();
        let result = func.finalize(state.as_ref()).unwrap();
        assert_eq!(result.as_i64(), Some(0));
    }

    #[test]
    fn test_count_multiple_batches() {
        let func = CountFunction;
        let mut state = func.create_state();
        let col1 = Column::from_i32(vec![1, 2, 3]);
        let col2 = Column::from_i32(vec![4, 5]);
        func.update(state.as_mut(), &col1).unwrap();
        func.update(state.as_mut(), &col2).unwrap();
        let result = func.finalize(state.as_ref()).unwrap();
        assert_eq!(result.as_i64(), Some(5));
    }

    #[test]
    fn test_count_merge_states() {
        let func = CountFunction;
        let mut state1 = func.create_state();
        let col1 = Column::from_i32(vec![1, 2, 3]);
        func.update(state1.as_mut(), &col1).unwrap();

        let mut state2 = func.create_state();
        let col2 = Column::from_i32(vec![4, 5]);
        func.update(state2.as_mut(), &col2).unwrap();

        state1.merge(state2.as_ref()).unwrap();
        let result = func.finalize(state1.as_ref()).unwrap();
        assert_eq!(result.as_i64(), Some(5));
    }

    #[test]
    fn test_count_return_type() {
        let func = CountFunction;
        assert_eq!(func.return_type(&[DataType::Float64]).unwrap(), DataType::BigInt);
        assert_eq!(func.return_type(&[DataType::Boolean]).unwrap(), DataType::BigInt);
    }
}
