use crate::data::column::Column;
use crate::data::types::DataType;
use crate::error::{Result, SiriusError};
use crate::op::aggregate::traits::{
    AggregateFunction, AggregateState, AggregateValue,
};
use std::any::Any;

#[derive(Debug, Clone)]
pub struct MinState {
    int_min: i64,
    float_min: f64,
    is_float: bool,
    has_value: bool,
}

impl MinState {
    pub fn new() -> Self {
        Self {
            int_min: i64::MAX,
            float_min: f64::INFINITY,
            is_float: false,
            has_value: false,
        }
    }
}

impl AggregateState for MinState {
    fn merge(&mut self, other: &dyn AggregateState) -> Result<()> {
        let other = other
            .as_any()
            .downcast_ref::<MinState>()
            .ok_or_else(|| SiriusError::TypeError("Cannot merge: type mismatch".into()))?;

        if other.has_value {
            if self.is_float || other.is_float {
                self.is_float = true;
                if !self.has_value || other.float_min < self.float_min {
                    self.float_min = other.float_min;
                }
            } else if !self.has_value || other.int_min < self.int_min {
                self.int_min = other.int_min;
            }
            self.has_value = true;
        }
        Ok(())
    }

    fn reset(&mut self) {
        self.int_min = i64::MAX;
        self.float_min = f64::INFINITY;
        self.is_float = false;
        self.has_value = false;
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

pub struct MinFunction;

impl AggregateFunction for MinFunction {
    fn name(&self) -> &str {
        "min"
    }

    fn return_type(&self, input_types: &[DataType]) -> Result<DataType> {
        if input_types.len() != 1 {
            return Err(SiriusError::TypeError(
                "MIN requires exactly one argument".into(),
            ));
        }
        if !input_types[0].is_numeric() {
            return Err(SiriusError::TypeError(format!(
                "MIN does not support type {}",
                input_types[0]
            )));
        }
        Ok(input_types[0])
    }

    fn create_state(&self) -> Box<dyn AggregateState> {
        Box::new(MinState::new())
    }

    fn update(&self, state: &mut dyn AggregateState, input: &Column) -> Result<()> {
        let state = state
            .as_any_mut()
            .downcast_mut::<MinState>()
            .ok_or_else(|| SiriusError::TypeError("Invalid state type for MIN".into()))?;

        if input.is_empty() {
            return Ok(());
        }

        match input.data_type() {
            DataType::Int32 => {
                let slice = input.as_i32_slice();
                for i in 0..input.len() {
                    if !input.is_null(i) {
                        let v = slice[i] as i64;
                        if !state.has_value || v < state.int_min {
                            state.int_min = v;
                        }
                        state.has_value = true;
                    }
                }
            }
            DataType::Int64 | DataType::BigInt => {
                let slice = input.as_i64_slice();
                for i in 0..input.len() {
                    if !input.is_null(i) {
                        if !state.has_value || slice[i] < state.int_min {
                            state.int_min = slice[i];
                        }
                        state.has_value = true;
                    }
                }
            }
            DataType::Float32 => {
                state.is_float = true;
                let slice = input.as_f32_slice();
                for i in 0..input.len() {
                    if !input.is_null(i) {
                        let v = slice[i] as f64;
                        if !state.has_value || v < state.float_min {
                            state.float_min = v;
                        }
                        state.has_value = true;
                    }
                }
            }
            DataType::Float64 => {
                state.is_float = true;
                let slice = input.as_f64_slice();
                for i in 0..input.len() {
                    if !input.is_null(i) {
                        if !state.has_value || slice[i] < state.float_min {
                            state.float_min = slice[i];
                        }
                        state.has_value = true;
                    }
                }
            }
            other => {
                return Err(SiriusError::TypeError(format!(
                    "MIN does not support type {other}"
                )));
            }
        }

        Ok(())
    }

    fn finalize(&self, state: &dyn AggregateState) -> Result<AggregateValue> {
        let state = state
            .as_any()
            .downcast_ref::<MinState>()
            .ok_or_else(|| SiriusError::TypeError("Invalid state type for MIN".into()))?;

        if !state.has_value {
            return Ok(AggregateValue::Null);
        }

        if state.is_float {
            Ok(AggregateValue::Float64(state.float_min))
        } else {
            Ok(AggregateValue::Int64(state.int_min))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_min_i32() {
        let func = MinFunction;
        let mut state = func.create_state();
        let col = Column::from_i32(vec![5, 1, 8, 3, 9]);
        func.update(state.as_mut(), &col).unwrap();
        assert_eq!(func.finalize(state.as_ref()).unwrap().as_i64(), Some(1));
    }

    #[test]
    fn test_min_f64() {
        let func = MinFunction;
        let mut state = func.create_state();
        let col = Column::from_f64(vec![3.5, 1.2, 7.8]);
        func.update(state.as_mut(), &col).unwrap();
        assert!((func.finalize(state.as_ref()).unwrap().as_f64().unwrap() - 1.2).abs() < f64::EPSILON);
    }

    #[test]
    fn test_min_with_nulls() {
        let func = MinFunction;
        let mut state = func.create_state();
        // values: [100, NULL, 5] -> min = 5
        let data: Vec<u8> = [100i32, 0i32, 5i32]
            .iter()
            .flat_map(|v| v.to_ne_bytes())
            .collect();
        let col = Column::from_raw_with_validity(DataType::Int32, data, vec![0b101u64], 3);
        func.update(state.as_mut(), &col).unwrap();
        assert_eq!(func.finalize(state.as_ref()).unwrap().as_i64(), Some(5));
    }

    #[test]
    fn test_min_all_null() {
        let func = MinFunction;
        let mut state = func.create_state();
        let col = Column::from_raw_with_validity(DataType::Int32, vec![0u8; 12], vec![0u64], 3);
        func.update(state.as_mut(), &col).unwrap();
        assert!(func.finalize(state.as_ref()).unwrap().is_null());
    }

    #[test]
    fn test_min_empty() {
        let func = MinFunction;
        let state = func.create_state();
        assert!(func.finalize(state.as_ref()).unwrap().is_null());
    }

    #[test]
    fn test_min_negative() {
        let func = MinFunction;
        let mut state = func.create_state();
        let col = Column::from_i32(vec![3, -10, 7, -2]);
        func.update(state.as_mut(), &col).unwrap();
        assert_eq!(func.finalize(state.as_ref()).unwrap().as_i64(), Some(-10));
    }

    #[test]
    fn test_min_merge_states() {
        let func = MinFunction;
        let mut s1 = func.create_state();
        func.update(s1.as_mut(), &Column::from_i32(vec![10, 20])).unwrap();

        let mut s2 = func.create_state();
        func.update(s2.as_mut(), &Column::from_i32(vec![5, 30])).unwrap();

        s1.merge(s2.as_ref()).unwrap();
        assert_eq!(func.finalize(s1.as_ref()).unwrap().as_i64(), Some(5));
    }

    #[test]
    fn test_min_return_type() {
        let func = MinFunction;
        assert_eq!(func.return_type(&[DataType::Int32]).unwrap(), DataType::Int32);
        assert_eq!(func.return_type(&[DataType::Float64]).unwrap(), DataType::Float64);
        assert!(func.return_type(&[DataType::Boolean]).is_err());
    }
}
