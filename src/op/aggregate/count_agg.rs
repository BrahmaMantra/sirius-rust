//! sirius_count 的 DuckDB C 回调
//!
//! 关键：duckdb_aggregate_state 直接 cast 为 state 指针，不通过 internal_ptr。

use crate::ext_api;
use crate::ffi::sys;
use crate::gpu_columns;
use crate::op::aggregate::count::CountFunction;
use crate::op::aggregate::traits::{AggregateFunction, AggregateValue};

#[repr(C)]
pub(crate) struct CountState {
    count: i64,
}

pub(crate) unsafe extern "C" fn state_size(_info: sys::duckdb_function_info) -> sys::idx_t {
    std::mem::size_of::<CountState>() as sys::idx_t
}

pub(crate) unsafe extern "C" fn init(
    _info: sys::duckdb_function_info,
    state: sys::duckdb_aggregate_state,
) {
    let ptr = state as *mut CountState;
    std::ptr::write(ptr, CountState { count: 0 });
}

pub(crate) unsafe extern "C" fn update(
    info: sys::duckdb_function_info,
    input: sys::duckdb_data_chunk,
    states: *mut sys::duckdb_aggregate_state,
) {
    let row_count = ext_api::duckdb_data_chunk_get_size(input);
    if row_count == 0 {
        return;
    }

    let col = match gpu_columns::vector_to_column(input, 0) {
        Some(c) => c,
        None => return,
    };

    let ctx = crate::sirius_context::global();

    // GPU 路径
    if let Some(backend) = ctx.gpu_backend() {
        if let Ok(AggregateValue::Int64(v)) = backend.gpu_count(&col) {
            let state_ptr = *states as *mut CountState;
            (*state_ptr).count += v;
            return;
        }
    }

    // CPU 路径
    let func = CountFunction;
    let mut agg_state = func.create_state();
    if func.update(agg_state.as_mut(), &col).is_ok() {
        if let Ok(AggregateValue::Int64(v)) = func.finalize(agg_state.as_ref()) {
            let state_ptr = *states as *mut CountState;
            (*state_ptr).count += v;
        }
    } else {
        let msg = std::ffi::CString::new("sirius_count update failed").unwrap();
        ext_api::duckdb_aggregate_function_set_error(info, msg.as_ptr());
    }
}

pub(crate) unsafe extern "C" fn combine(
    _info: sys::duckdb_function_info,
    source: *mut sys::duckdb_aggregate_state,
    target: *mut sys::duckdb_aggregate_state,
    count: sys::idx_t,
) {
    for i in 0..count as usize {
        let src = *source.add(i) as *const CountState;
        let tgt = *target.add(i) as *mut CountState;
        (*tgt).count += (*src).count;
    }
}

pub(crate) unsafe extern "C" fn finalize(
    _info: sys::duckdb_function_info,
    source: *mut sys::duckdb_aggregate_state,
    result: sys::duckdb_vector,
    count: sys::idx_t,
    offset: sys::idx_t,
) {
    for i in 0..count as usize {
        let state = *source.add(i) as *const CountState;
        let idx = (offset as usize + i) as u64;
        gpu_columns::write_i64_to_vector(result, idx, (*state).count);
    }
}
