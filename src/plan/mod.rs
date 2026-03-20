//! 查询计划 — 对应 sirius/src/plan/
//!
//! 将 Substrait 逻辑计划转换为 GPU 物理计划。
//! Phase 2/3: gpu_plan_aggregate, gpu_plan_filter, gpu_plan_projection, ...
