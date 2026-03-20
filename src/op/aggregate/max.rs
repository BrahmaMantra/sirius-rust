use crate::data::column::Column;
use crate::data::types::DataType;
use crate::error::{Result, SiriusError};
use crate::op::aggregate::traits::{
    AggregateFunction, AggregateState, AggregateValue,
};
use std::any::Any;

#[derive(Debug, Clone)]
pub struct MaxState {
    int_max: i64,
    float_max: f64,
    is_float: bool,
    has_value: bool,
}

impl MaxState {
    pub fn new() -> Self {
        Self {
            int_max: i64::MIN,
            float_max: f64::NEG_INFINITY,
            is_float: false,
            has_value: false,
        }
    }
}

impl AggregateState for MaxState {
    fn merge(&mut self, other: &dyn AggregateState) -> Result<()> {
        let other = other
            .as_any()
            .downcast_ref::<MaxState>()
            .ok_or_else(|| SiriusError::TypeError("Cannot merge: type mismatch".into()))?;

        if other.has_value {
            if self.is_float || other.is_float {
                self.is_float = true;
                if !self.has_value || other.float_max > self.float_max {
                    self.float_max = other.float_max;
                }
            } else if !self.has_value || other.int_max > self.int_max {
                self.int_max = other.int_max;
            }
            self.has_value = true;
        }
        Ok(())
    }

    fn reset(&mut self) {
        self.int_max = i64::MIN;
        self.float_max = f64::NEG_INFINITY;
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

pub struct MaxFunction;

impl AggregateFunction for MaxFunction {
    fn name(&self) -> &str {
        "max"
    }

    fn return_type(&self, input_types: &[DataType]) -> Result<DataType> {
        if input_types.len() != 1 {
            return Err(SiriusError::TypeError(
                "MAX requires exactly one argument".into(),
            ));
        }
        if !input_types[0].is_numeric() {
            return Err(SiriusError::TypeError(format!(
                "MAX does not support type {}",
                input_types[0]
            )));
        }
        Ok(input_types[0])
    }

    fn create_state(&self) -> Box<dyn AggregateState> {
        Box::new(MaxState::new())
    }

    fn update(&self, state: &mut dyn AggregateState, input: &Column) -> Result<()> {
        let state = state
            .as_any_mut()
            .downcast_mut::<MaxState>()
            .ok_or_else(|| SiriusError::TypeError("Invalid state type for MAX".into()))?;

        if input.is_empty() {
            return Ok(());
        }

        match input.data_type() {
            DataType::Int32 => {
                let slice = input.as_i32_slice();
                for i in 0..input.len() {
                    if !input.is_null(i) {
                        let v = slice[i] as i64;
                        if !state.has_value || v > state.int_max {
                            state.int_max = v;
                        }
                        state.has_value = true;
                    }
                }
            }
            DataType::Int64 | DataType::BigInt => {
                let slice = input.as_i64_slice();
                for i in 0..input.len() {
                    if !input.is_null(i) {
                        if !state.has_value || slice[i] > state.int_max {
                            state.int_max = slice[i];
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
                        if !state.has_value || v > state.float_max {
                            state.float_max = v;
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
                        if !state.has_value || slice[i] > state.float_max {
                            state.float_max = slice[i];
                        }
                        state.has_value = true;
                    }
                }
            }
            other => {
                return Err(SiriusError::TypeError(format!(
                    "MAX does not support type {other}"
                )));
            }
        }

        Ok(())
    }

    fn finalize(&self, state: &dyn AggregateState) -> Result<AggregateValue> {
        let state = state
            .as_any()
            .downcast_ref::<MaxState>()
            .ok_or_else(|| SiriusError::TypeError("Invalid state type for MAX".into()))?;

        if !state.has_value {
            return Ok(AggregateValue::Null);
        }

        if state.is_float {
            Ok(AggregateValue::Float64(state.float_max))
        } else {
            Ok(AggregateValue::Int64(state.int_max))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max_i32() {
        let func = MaxFunction;
        let mut state = func.create_state();
        let col = Column::from_i32(vec![5, 1, 8, 3, 9]);
        func.update(state.as_mut(), &col).unwrap();
        assert_eq!(func.finalize(state.as_ref()).unwrap().as_i64(), Some(9));
    }

    #[test]
    fn test_max_f64() {
        let func = MaxFunction;
        let mut state = func.create_state();
        let col = Column::from_f64(vec![3.5, 1.2, 7.8]);
        func.update(state.as_mut(), &col).unwrap();
        assert!((func.finalize(state.as_ref()).unwrap().as_f64().unwrap() - 7.8).abs() < f64::EPSILON);
    }

    #[test]
    fn test_max_with_nulls() {
        let func = MaxFunction;
        let mut state = func.create_state();
        // values: [5, NULL, 100] -> max = 100
        let data: Vec<u8> = [5i32, 0i32, 100i32]
            .iter()
            .flat_map(|v| v.to_ne_bytes())
            .collect();
        let col = Column::from_raw_with_validity(DataType::Int32, data, vec![0b101u64], 3);
        func.update(state.as_mut(), &col).unwrap();
        assert_eq!(func.finalize(state.as_ref()).unwrap().as_i64(), Some(100));
    }

    #[test]
    fn test_max_all_null() {
        let func = MaxFunction;
        let mut state = func.create_state();
        let col = Column::from_raw_with_validity(DataType::Int32, vec![0u8; 12], vec![0u64], 3);
        func.update(state.as_mut(), &col).unwrap();
        assert!(func.finalize(state.as_ref()).unwrap().is_null());
    }

    #[test]
    fn test_max_empty() {
        let func = MaxFunction;
        let state = func.create_state();
        assert!(func.finalize(state.as_ref()).unwrap().is_null());
    }

    #[test]
    fn test_max_negative() {
        let func = MaxFunction;
        let mut state = func.create_state();
        let col = Column::from_i32(vec![-3, -10, -7, -2]);
        func.update(state.as_mut(), &col).unwrap();
        assert_eq!(func.finalize(state.as_ref()).unwrap().as_i64(), Some(-2));
    }

    #[test]
    fn test_max_merge_states() {
        let func = MaxFunction;
        let mut s1 = func.create_state();
        func.update(s1.as_mut(), &Column::from_i32(vec![10, 20])).unwrap();

        let mut s2 = func.create_state();
        func.update(s2.as_mut(), &Column::from_i32(vec![5, 30])).unwrap();

        s1.merge(s2.as_ref()).unwrap();
        assert_eq!(func.finalize(s1.as_ref()).unwrap().as_i64(), Some(30));
    }

    #[test]
    fn test_max_return_type() {
        let func = MaxFunction;
        assert_eq!(func.return_type(&[DataType::Int32]).unwrap(), DataType::Int32);
        assert_eq!(func.return_type(&[DataType::Float64]).unwrap(), DataType::Float64);
        assert!(func.return_type(&[DataType::Boolean]).is_err());
    }
}
