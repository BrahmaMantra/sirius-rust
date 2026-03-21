pub mod traits;

// Phase 1: SUM, COUNT
pub mod count;
pub mod sum;

// Phase 2 (预留)
pub mod avg;
pub mod max;
pub mod min;

// DuckDB C 回调注册
pub mod register;
pub(crate) mod sum_agg;
pub(crate) mod count_agg;
pub(crate) mod avg_agg;
pub(crate) mod min_agg;
pub(crate) mod max_agg;

use std::collections::HashMap;
use traits::AggregateFunction;

/// 聚合函数注册表
pub struct AggregateRegistry {
    functions: HashMap<String, Box<dyn AggregateFunction>>,
}

impl AggregateRegistry {
    pub fn new() -> Self {
        let mut registry = Self {
            functions: HashMap::new(),
        };
        registry.register_defaults();
        registry
    }

    fn register_defaults(&mut self) {
        self.register(Box::new(sum::SumFunction));
        self.register(Box::new(count::CountFunction));
        self.register(Box::new(count::CountStarFunction));
        self.register(Box::new(avg::AvgFunction));
        self.register(Box::new(min::MinFunction));
        self.register(Box::new(max::MaxFunction));
    }

    pub fn register(&mut self, func: Box<dyn AggregateFunction>) {
        self.functions.insert(func.name().to_string(), func);
    }

    pub fn get(&self, name: &str) -> Option<&dyn AggregateFunction> {
        self.functions.get(name).map(|f| f.as_ref())
    }
}

impl Default for AggregateRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry() {
        let registry = AggregateRegistry::new();
        assert!(registry.get("sum").is_some());
        assert!(registry.get("count").is_some());
        assert!(registry.get("count_star").is_some());
        assert!(registry.get("avg").is_some());
        assert!(registry.get("min").is_some());
        assert!(registry.get("max").is_some());
    }
}
