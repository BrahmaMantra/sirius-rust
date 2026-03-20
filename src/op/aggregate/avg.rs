use crate::data::column::Column;
use crate::data::types::DataType;
use crate::error::{Result, SiriusError};
use crate::op::aggregate::traits::{
    AggregateFunction, AggregateState, AggregateValue,
};
use std::any::Any;

#[derive(Debug, Clone)]
pub struct AvgState {
    sum: f64,
    count: u64,
}

impl AvgState {
    pub fn new() -> Self {
        Self { sum: 0.0, count: 0 }
    }
}

impl AggregateState for AvgState {
    fn merge(&mut self, other: &dyn AggregateState) -> Result<()> {
        let other = other
            .as_any()
            .downcast_ref::<AvgState>()
            .ok_or_else(|| SiriusError::TypeError("Cannot merge: type mismatch".into()))?;
        self.sum += other.sum;
        self.count += other.count;
        Ok(())
    }

    fn reset(&mut self) {
        self.sum = 0.0;
        self.count = 0;
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

pub struct AvgFunction;

impl AggregateFunction for AvgFunction {
    fn name(&self) -> &str {
        "avg"
    }

    fn return_type(&self, input_types: &[DataType]) -> Result<DataType> {
        if input_types.len() != 1 {
            return Err(SiriusError::TypeError(
                "AVG requires exactly one argument".into(),
            ));
        }
        if !input_types[0].is_numeric() {
            return Err(SiriusError::TypeError(format!(
                "AVG does not support type {}",
                input_types[0]
            )));
        }
        Ok(DataType::Float64)
    }

    fn create_state(&self) -> Box<dyn AggregateState> {
        Box::new(AvgState::new())
    }

    fn update(&self, state: &mut dyn AggregateState, input: &Column) -> Result<()> {
        let state = state
            .as_any_mut()
            .downcast_mut::<AvgState>()
            .ok_or_else(|| SiriusError::TypeError("Invalid state type for AVG".into()))?;

        if input.is_empty() {
            return Ok(());
        }

        match input.data_type() {
            DataType::Int8 => {
                let data = input.raw_data();
                for i in 0..input.len() {
                    if !input.is_null(i) {
                        state.sum += data[i] as i8 as f64;
                        state.count += 1;
                    }
                }
            }
            DataType::Int16 => {
                let data = input.raw_data();
                for i in 0..input.len() {
                    if !input.is_null(i) {
                        let bytes = [data[i * 2], data[i * 2 + 1]];
                        state.sum += i16::from_ne_bytes(bytes) as f64;
                        state.count += 1;
                    }
                }
            }
            DataType::Int32 => {
                let slice = input.as_i32_slice();
                for i in 0..input.len() {
                    if !input.is_null(i) {
                        state.sum += slice[i] as f64;
                        state.count += 1;
                    }
                }
            }
            DataType::Int64 | DataType::BigInt => {
                let slice = input.as_i64_slice();
                for i in 0..input.len() {
                    if !input.is_null(i) {
                        state.sum += slice[i] as f64;
                        state.count += 1;
                    }
                }
            }
            DataType::Float32 => {
                let slice = input.as_f32_slice();
                for i in 0..input.len() {
                    if !input.is_null(i) {
                        state.sum += slice[i] as f64;
                        state.count += 1;
                    }
                }
            }
            DataType::Float64 => {
                let slice = input.as_f64_slice();
                for i in 0..input.len() {
                    if !input.is_null(i) {
                        state.sum += slice[i];
                        state.count += 1;
                    }
                }
            }
            other => {
                return Err(SiriusError::TypeError(format!(
                    "AVG does not support type {other}"
                )));
            }
        }

        Ok(())
    }

    fn finalize(&self, state: &dyn AggregateState) -> Result<AggregateValue> {
        let state = state
            .as_any()
            .downcast_ref::<AvgState>()
            .ok_or_else(|| SiriusError::TypeError("Invalid state type for AVG".into()))?;

        if state.count == 0 {
            return Ok(AggregateValue::Null);
        }

        Ok(AggregateValue::Float64(state.sum / state.count as f64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_avg_i32() {
        let func = AvgFunction;
        let mut state = func.create_state();
        let col = Column::from_i32(vec![2, 4, 6]);
        func.update(state.as_mut(), &col).unwrap();
        let result = func.finalize(state.as_ref()).unwrap();
        assert!((result.as_f64().unwrap() - 4.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_avg_f64() {
        let func = AvgFunction;
        let mut state = func.create_state();
        let col = Column::from_f64(vec![1.0, 2.0, 3.0, 4.0]);
        func.update(state.as_mut(), &col).unwrap();
        let result = func.finalize(state.as_ref()).unwrap();
        assert!((result.as_f64().unwrap() - 2.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_avg_with_nulls() {
        let func = AvgFunction;
        let mut state = func.create_state();
        let data: Vec<u8> = [10i32, 0i32, 30i32]
            .iter()
            .flat_map(|v| v.to_ne_bytes())
            .collect();
        let col = Column::from_raw_with_validity(DataType::Int32, data, vec![0b101u64], 3);
        func.update(state.as_mut(), &col).unwrap();
        let result = func.finalize(state.as_ref()).unwrap();
        assert!((result.as_f64().unwrap() - 20.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_avg_all_null() {
        let func = AvgFunction;
        let mut state = func.create_state();
        let col = Column::from_raw_with_validity(DataType::Int32, vec![0u8; 12], vec![0u64], 3);
        func.update(state.as_mut(), &col).unwrap();
        assert!(func.finalize(state.as_ref()).unwrap().is_null());
    }

    #[test]
    fn test_avg_empty() {
        let func = AvgFunction;
        let state = func.create_state();
        assert!(func.finalize(state.as_ref()).unwrap().is_null());
    }

    #[test]
    fn test_avg_merge_states() {
        let func = AvgFunction;
        let mut s1 = func.create_state();
        func.update(s1.as_mut(), &Column::from_i32(vec![10, 20])).unwrap();

        let mut s2 = func.create_state();
        func.update(s2.as_mut(), &Column::from_i32(vec![30])).unwrap();

        s1.merge(s2.as_ref()).unwrap();
        let result = func.finalize(s1.as_ref()).unwrap();
        assert!((result.as_f64().unwrap() - 20.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_avg_return_type() {
        let func = AvgFunction;
        assert_eq!(func.return_type(&[DataType::Int32]).unwrap(), DataType::Float64);
        assert_eq!(func.return_type(&[DataType::Float64]).unwrap(), DataType::Float64);
        assert!(func.return_type(&[DataType::Boolean]).is_err());
        assert!(func.return_type(&[]).is_err());
    }
}
