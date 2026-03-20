//! 全局执行上下文 — 对应 sirius/src/sirius_context.cpp
//!
//! 构造时根据 GPU 可用性选择 executor 实现，
//! 不支持 GPU 时回退到 DuckDB 原生（不做 CPU fallback）。

use crate::executor::mock_gpu::MockGpuExecutor;
use crate::executor::AggregateExecutor;
use std::sync::OnceLock;

/// 全局单例
static CONTEXT: OnceLock<SiriusContext> = OnceLock::new();

pub fn global() -> &'static SiriusContext {
    CONTEXT.get_or_init(SiriusContext::new)
}

/// Sirius 全局上下文
pub struct SiriusContext {
    executor: Box<dyn AggregateExecutor>,
}

impl SiriusContext {
    pub fn new() -> Self {
        let executor = Self::detect_executor();
        Self { executor }
    }

    /// 使用指定的 executor（用于测试）
    pub fn with_executor(executor: Box<dyn AggregateExecutor>) -> Self {
        Self { executor }
    }

    fn detect_executor() -> Box<dyn AggregateExecutor> {
        #[cfg(feature = "gpu")]
        {
            if let Ok(_gpu_ctx) = crate::gpu_context::GpuContext::try_init() {
                // TODO: 创建真正的 GpuExecutor
                // return Box::new(GpuExecutor::new(gpu_ctx));
            }
        }

        Box::new(MockGpuExecutor::new())
    }

    pub fn executor(&self) -> &dyn AggregateExecutor {
        self.executor.as_ref()
    }
}

impl Default for SiriusContext {
    fn default() -> Self {
        Self::new()
    }
}
