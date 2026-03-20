//! GPU → CPU 回退 — 对应 sirius/src/fallback.cpp
//!
//! 当 GPU 执行失败或不可用时，使用 CPU 路径执行。

use crate::data::column::Column;
use crate::error::Result;
use crate::op::aggregate::traits::{AggregateFunction, AggregateValue};

/// CPU 聚合执行
pub fn execute_cpu_aggregate(
    func: &dyn AggregateFunction,
    input: &Column,
) -> Result<AggregateValue> {
    let mut state = func.create_state();
    func.update(state.as_mut(), input)?;
    func.finalize(state.as_ref())
}
