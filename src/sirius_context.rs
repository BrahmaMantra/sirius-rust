//! 全局执行上下文 — 对应 sirius/src/sirius_context.cpp
//!
//! 管理 GPU 后端、聚合注册表等全局状态。

use crate::cuda::backend::GpuBackend;
use crate::op::aggregate::AggregateRegistry;
use std::sync::OnceLock;

/// 全局单例
static CONTEXT: OnceLock<SiriusContext> = OnceLock::new();

pub fn global() -> &'static SiriusContext {
    CONTEXT.get_or_init(SiriusContext::new)
}

/// Sirius 全局上下文
pub struct SiriusContext {
    aggregate_registry: AggregateRegistry,
    gpu_backend: Option<Box<dyn GpuBackend>>,
}

impl SiriusContext {
    pub fn new() -> Self {
        let mut ctx = Self {
            aggregate_registry: AggregateRegistry::new(),
            gpu_backend: None,
        };
        ctx.try_init_gpu();
        ctx
    }

    /// 使用指定的 GPU 后端（用于测试）
    pub fn with_backend(backend: Box<dyn GpuBackend>) -> Self {
        Self {
            aggregate_registry: AggregateRegistry::new(),
            gpu_backend: Some(backend),
        }
    }

    fn try_init_gpu(&mut self) {
        #[cfg(feature = "gpu")]
        {
            match crate::gpu_context::GpuContext::try_init() {
                Ok(_gpu_ctx) => {
                    // TODO: 创建 CudaBackend 包装真实 GPU
                    // self.gpu_backend = Some(Box::new(CudaBackend::new(gpu_ctx)));
                }
                Err(_) => {}
            }
        }
    }

    pub fn is_gpu_available(&self) -> bool {
        self.gpu_backend.is_some()
    }

    pub fn gpu_backend(&self) -> Option<&dyn GpuBackend> {
        self.gpu_backend.as_deref()
    }

    pub fn aggregate_registry(&self) -> &AggregateRegistry {
        &self.aggregate_registry
    }
}

impl Default for SiriusContext {
    fn default() -> Self {
        Self::new()
    }
}
