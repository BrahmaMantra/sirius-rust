//! GPU 执行调度 — 对应 sirius/src/gpu_executor.cpp
//!
//! 执行聚合操作，自动选择 GPU 或 CPU 路径。

use crate::data::column::Column;
use crate::error::Result;
use crate::op::aggregate::traits::{AggregateFunction, AggregateValue};
use crate::sirius_context::SiriusContext;

/// 执行一个聚合操作 — 自动选择 GPU 或 CPU 路径
pub fn execute_aggregate(
    ctx: &SiriusContext,
    func: &dyn AggregateFunction,
    input: &Column,
) -> Result<AggregateValue> {
    // GPU 路径
    if let Some(backend) = ctx.gpu_backend() {
        let result = match func.name() {
            "sum" => backend.gpu_sum(input),
            "count" => backend.gpu_count(input),
            "count_star" => backend.gpu_count_star(input),
            _ => Err(crate::error::SiriusError::NotImplemented(format!(
                "GPU {} not implemented",
                func.name()
            ))),
        };

        match result {
            Ok(val) => return Ok(val),
            Err(_) => {
                // GPU 失败，fallback 到 CPU
            }
        }
    }

    // CPU 路径（默认或 fallback）
    crate::fallback::execute_cpu_aggregate(func, input)
}

/// 分批执行聚合（用于数据超出 GPU 显存的场景，如 TPC-H SF10）
pub fn execute_aggregate_batched(
    _ctx: &SiriusContext,
    func: &dyn AggregateFunction,
    chunks: &[Column],
) -> Result<AggregateValue> {
    let mut state = func.create_state();

    for chunk in chunks {
        func.update(state.as_mut(), chunk)?;
    }

    func.finalize(state.as_ref())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cuda::backend::MockGpuBackend;
    use crate::op::aggregate::count::CountFunction;
    use crate::op::aggregate::sum::SumFunction;

    #[test]
    fn test_cpu_sum() {
        let ctx = SiriusContext::new();
        let func = SumFunction;
        let col = Column::from_i32(vec![1, 2, 3, 4, 5]);
        let result = execute_aggregate(&ctx, &func, &col).unwrap();
        assert_eq!(result.as_i64(), Some(15));
    }

    #[test]
    fn test_cpu_count() {
        let ctx = SiriusContext::new();
        let func = CountFunction;
        let col = Column::from_i32(vec![1, 2, 3]);
        let result = execute_aggregate(&ctx, &func, &col).unwrap();
        assert_eq!(result.as_i64(), Some(3));
    }

    #[test]
    fn test_gpu_simulated_sum() {
        let ctx = SiriusContext::with_backend(Box::new(MockGpuBackend::new()));
        assert!(ctx.is_gpu_available());
        let func = SumFunction;
        let col = Column::from_i32(vec![1, 2, 3, 4, 5]);
        let result = execute_aggregate(&ctx, &func, &col).unwrap();
        assert_eq!(result.as_i64(), Some(15));
    }

    #[test]
    fn test_gpu_simulated_count() {
        let ctx = SiriusContext::with_backend(Box::new(MockGpuBackend::new()));
        let func = CountFunction;
        let col = Column::from_i32(vec![1, 2, 3, 4, 5]);
        let result = execute_aggregate(&ctx, &func, &col).unwrap();
        assert_eq!(result.as_i64(), Some(5));
    }

    #[test]
    fn test_gpu_cpu_consistency() {
        let cpu_ctx = SiriusContext::new();
        let gpu_ctx = SiriusContext::with_backend(Box::new(MockGpuBackend::new()));
        let func = SumFunction;
        let data: Vec<i32> = (1..=10_000).collect();
        let col = Column::from_i32(data);
        let cpu_result = execute_aggregate(&cpu_ctx, &func, &col).unwrap();
        let gpu_result = execute_aggregate(&gpu_ctx, &func, &col).unwrap();
        assert_eq!(cpu_result.as_i64(), gpu_result.as_i64());
    }

    #[test]
    fn test_batched_sum() {
        let ctx = SiriusContext::new();
        let func = SumFunction;
        let chunks = vec![
            Column::from_i32(vec![1, 2, 3]),
            Column::from_i32(vec![4, 5, 6]),
            Column::from_i32(vec![7, 8, 9]),
        ];
        let result = execute_aggregate_batched(&ctx, &func, &chunks).unwrap();
        assert_eq!(result.as_i64(), Some(45));
    }
}
