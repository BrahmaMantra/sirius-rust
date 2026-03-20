//! 聚合执行器 — GPU 统一接口
//!
//! `AggregateExecutor` trait 定义了聚合操作的统一接口。
//! 构造 `SiriusContext` 时根据 GPU 可用性选择具体实现，
//! 不支持 GPU 时回退到 DuckDB 原生执行（不在这里实现 CPU fallback）。

pub mod mock_gpu;

use crate::data::column::Column;
use crate::error::Result;
use crate::op::aggregate::traits::AggregateValue;

/// 聚合执行器 trait — GPU 和 MockGPU 实现同一接口
pub trait AggregateExecutor: Send + Sync {
    fn name(&self) -> &str;

    fn sum(&self, input: &Column) -> Result<AggregateValue>;
    fn count(&self, input: &Column) -> Result<AggregateValue>;
    fn count_star(&self, input: &Column) -> Result<AggregateValue>;
    fn avg(&self, input: &Column) -> Result<AggregateValue>;
    fn min(&self, input: &Column) -> Result<AggregateValue>;
    fn max(&self, input: &Column) -> Result<AggregateValue>;
}
