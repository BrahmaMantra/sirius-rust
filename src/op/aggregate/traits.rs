use crate::data::column::Column;
use crate::data::types::DataType;
use crate::error::Result;

/// 聚合状态 trait — 每个聚合函数的中间累积状态
pub trait AggregateState: Send + Sync {
    fn merge(&mut self, other: &dyn AggregateState) -> Result<()>;
    fn reset(&mut self);
    fn as_any(&self) -> &dyn std::any::Any;
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}

/// 聚合函数值 — 最终结果
#[derive(Debug, Clone)]
pub enum AggregateValue {
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    Null,
}

impl AggregateValue {
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            AggregateValue::Int64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            AggregateValue::UInt64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            AggregateValue::Float64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, AggregateValue::Null)
    }
}

/// 聚合函数 trait — SUM, COUNT 等的统一接口
pub trait AggregateFunction: Send + Sync {
    fn name(&self) -> &str;
    fn return_type(&self, input_types: &[DataType]) -> Result<DataType>;
    fn create_state(&self) -> Box<dyn AggregateState>;
    fn update(&self, state: &mut dyn AggregateState, input: &Column) -> Result<()>;
    fn finalize(&self, state: &dyn AggregateState) -> Result<AggregateValue>;
}
