use crate::data::column::Column;
use crate::data::types::DataType;
use crate::error::{Result, SiriusError};
use crate::op::aggregate::traits::{
    AggregateFunction, AggregateState, AggregateValue,
};
use std::any::Any;

#[derive(Debug, Clone)]
pub struct SumState {
    int_sum: i64,
    float_sum: f64,
    is_float: bool,
    has_value: bool,
}

impl SumState {
    pub fn new(is_float: bool) -> Self {
        Self {
            int_sum: 0,
            float_sum: 0.0,
            is_float,
            has_value: false,
        }
    }
}

impl AggregateState for SumState {
    fn merge(&mut self, other: &dyn AggregateState) -> Result<()> {
        let other = other
            .as_any()
            .downcast_ref::<SumState>()
            .ok_or_else(|| SiriusError::TypeError("Cannot merge: type mismatch".into()))?;

        if other.has_value {
            if self.is_float {
                self.float_sum += other.float_sum;
            } else {
                self.int_sum = self.int_sum.wrapping_add(other.int_sum);
            }
            self.has_value = true;
        }
        Ok(())
    }

    fn reset(&mut self) {
        self.int_sum = 0;
        self.float_sum = 0.0;
        self.has_value = false;
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

pub struct SumFunction;

impl AggregateFunction for SumFunction {
    fn name(&self) -> &str {
        "sum"
    }

    fn return_type(&self, input_types: &[DataType]) -> Result<DataType> {
        if input_types.len() != 1 {
            return Err(SiriusError::TypeError(
                "SUM requires exactly one argument".into(),
            ));
        }
        input_types[0]
            .sum_result_type()
            .ok_or_else(|| {
                SiriusError::TypeError(format!(
                    "SUM does not support type {}",
                    input_types[0]
                ))
            })
    }

    fn create_state(&self) -> Box<dyn AggregateState> {
        Box::new(SumState::new(false))
    }

    fn update(&self, state: &mut dyn AggregateState, input: &Column) -> Result<()> {
        let state = state
            .as_any_mut()
            .downcast_mut::<SumState>()
            .ok_or_else(|| SiriusError::TypeError("Invalid state type for SUM".into()))?;

        if input.is_empty() {
            return Ok(());
        }

        match input.data_type() {
            DataType::Int8 => {
                let data = input.raw_data();
                for i in 0..input.len() {
                    if !input.is_null(i) {
                        state.int_sum += data[i] as i8 as i64;
                        state.has_value = true;
                    }
                }
            }
            DataType::Int16 => {
                let data = input.raw_data();
                for i in 0..input.len() {
                    if !input.is_null(i) {
                        let bytes = [data[i * 2], data[i * 2 + 1]];
                        state.int_sum += i16::from_ne_bytes(bytes) as i64;
                        state.has_value = true;
                    }
                }
            }
            DataType::Int32 => {
                let slice = input.as_i32_slice();
                for i in 0..input.len() {
                    if !input.is_null(i) {
                        state.int_sum += slice[i] as i64;
                        state.has_value = true;
                    }
                }
            }
            DataType::Int64 | DataType::BigInt => {
                let slice = input.as_i64_slice();
                for i in 0..input.len() {
                    if !input.is_null(i) {
                        state.int_sum = state.int_sum.wrapping_add(slice[i]);
                        state.has_value = true;
                    }
                }
            }
            DataType::Float32 => {
                state.is_float = true;
                let slice = input.as_f32_slice();
                for i in 0..input.len() {
                    if !input.is_null(i) {
                        state.float_sum += slice[i] as f64;
                        state.has_value = true;
                    }
                }
            }
            DataType::Float64 => {
                state.is_float = true;
                let slice = input.as_f64_slice();
                for i in 0..input.len() {
                    if !input.is_null(i) {
                        state.float_sum += slice[i];
                        state.has_value = true;
                    }
                }
            }
            other => {
                return Err(SiriusError::TypeError(format!(
                    "SUM does not support type {other}"
                )));
            }
        }

        Ok(())
    }

    fn finalize(&self, state: &dyn AggregateState) -> Result<AggregateValue> {
        let state = state
            .as_any()
            .downcast_ref::<SumState>()
            .ok_or_else(|| SiriusError::TypeError("Invalid state type for SUM".into()))?;

        if !state.has_value {
            return Ok(AggregateValue::Null);
        }

        if state.is_float {
            Ok(AggregateValue::Float64(state.float_sum))
        } else {
            Ok(AggregateValue::Int64(state.int_sum))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sum_i32() {
        let func = SumFunction;
        let mut state = func.create_state();
        let col = Column::from_i32(vec![1, 2, 3, 4, 5]);
        func.update(state.as_mut(), &col).unwrap();
        let result = func.finalize(state.as_ref()).unwrap();
        assert_eq!(result.as_i64(), Some(15));
    }

    #[test]
    fn test_sum_i64() {
        let func = SumFunction;
        let mut state = func.create_state();
        let col = Column::from_i64(vec![100, 200, 300]);
        func.update(state.as_mut(), &col).unwrap();
        let result = func.finalize(state.as_ref()).unwrap();
        assert_eq!(result.as_i64(), Some(600));
    }

    #[test]
    fn test_sum_f64() {
        let func = SumFunction;
        let mut state = func.create_state();
        let col = Column::from_f64(vec![1.5, 2.5, 3.0]);
        func.update(state.as_mut(), &col).unwrap();
        let result = func.finalize(state.as_ref()).unwrap();
        assert!((result.as_f64().unwrap() - 7.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_sum_with_nulls() {
        let func = SumFunction;
        let mut state = func.create_state();
        let data: Vec<u8> = [10i32, 0i32, 30i32]
            .iter()
            .flat_map(|v| v.to_ne_bytes())
            .collect();
        let col = Column::from_raw_with_validity(DataType::Int32, data, vec![0b101u64], 3);
        func.update(state.as_mut(), &col).unwrap();
        let result = func.finalize(state.as_ref()).unwrap();
        assert_eq!(result.as_i64(), Some(40));
    }

    #[test]
    fn test_sum_all_null() {
        let func = SumFunction;
        let mut state = func.create_state();
        let data = vec![0u8; 4 * 3];
        let col = Column::from_raw_with_validity(DataType::Int32, data, vec![0u64], 3);
        func.update(state.as_mut(), &col).unwrap();
        let result = func.finalize(state.as_ref()).unwrap();
        assert!(result.is_null());
    }

    #[test]
    fn test_sum_empty() {
        let func = SumFunction;
        let state = func.create_state();
        let result = func.finalize(state.as_ref()).unwrap();
        assert!(result.is_null());
    }

    #[test]
    fn test_sum_multiple_batches() {
        let func = SumFunction;
        let mut state = func.create_state();
        let col1 = Column::from_i32(vec![1, 2, 3]);
        let col2 = Column::from_i32(vec![4, 5, 6]);
        func.update(state.as_mut(), &col1).unwrap();
        func.update(state.as_mut(), &col2).unwrap();
        let result = func.finalize(state.as_ref()).unwrap();
        assert_eq!(result.as_i64(), Some(21));
    }

    #[test]
    fn test_sum_merge_states() {
        let func = SumFunction;
        let mut state1 = func.create_state();
        let col1 = Column::from_i32(vec![1, 2, 3]);
        func.update(state1.as_mut(), &col1).unwrap();

        let mut state2 = func.create_state();
        let col2 = Column::from_i32(vec![4, 5, 6]);
        func.update(state2.as_mut(), &col2).unwrap();

        state1.merge(state2.as_ref()).unwrap();
        let result = func.finalize(state1.as_ref()).unwrap();
        assert_eq!(result.as_i64(), Some(21));
    }

    #[test]
    fn test_sum_return_type() {
        let func = SumFunction;
        assert_eq!(func.return_type(&[DataType::Int32]).unwrap(), DataType::BigInt);
        assert_eq!(func.return_type(&[DataType::Float64]).unwrap(), DataType::Float64);
        assert!(func.return_type(&[DataType::Boolean]).is_err());
    }
}
