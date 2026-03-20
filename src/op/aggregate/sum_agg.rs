//! sirius_sum 的 DuckDB C 回调
//!
//! 关键：duckdb_aggregate_state 直接 cast 为 state 指针，不通过 internal_ptr。
//! 参考 DuckDB C API 测试：duckdb/test/api/capi/capi_aggregate_functions.cpp

use crate::ext_api;
use crate::ffi::sys;
use crate::gpu_columns;
use crate::op::aggregate::sum::SumFunction;
use crate::op::aggregate::traits::{AggregateFunction, AggregateValue};

#[repr(C)]
pub(crate) struct SumState {
    int_sum: i64,
    float_sum: f64,
    is_float: bool,
    has_value: bool,
}

pub(crate) unsafe extern "C" fn state_size(_info: sys::duckdb_function_info) -> sys::idx_t {
    std::mem::size_of::<SumState>() as sys::idx_t
}

pub(crate) unsafe extern "C" fn init(
    _info: sys::duckdb_function_info,
    state: sys::duckdb_aggregate_state,
) {
    let ptr = state as *mut SumState;
    std::ptr::write(ptr, SumState {
        int_sum: 0,
        float_sum: 0.0,
        is_float: false,
        has_value: false,
    });
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
        if let Ok(val) = backend.gpu_sum(&col) {
            // 所有行映射到同一个 state（无 GROUP BY 时 states[0] == states[i]）
            let state_ptr = *states as *mut SumState;
            merge_value(state_ptr, &val);
            return;
        }
    }

    // CPU 路径
    let func = SumFunction;
    let mut agg_state = func.create_state();
    if func.update(agg_state.as_mut(), &col).is_ok() {
        if let Ok(val) = func.finalize(agg_state.as_ref()) {
            let state_ptr = *states as *mut SumState;
            merge_value(state_ptr, &val);
        }
    } else {
        let msg = std::ffi::CString::new("sirius_sum update failed").unwrap();
        ext_api::duckdb_aggregate_function_set_error(info, msg.as_ptr());
    }
}

unsafe fn merge_value(state: *mut SumState, val: &AggregateValue) {
    match val {
        AggregateValue::Int64(v) => {
            (*state).int_sum = (*state).int_sum.wrapping_add(*v);
            (*state).has_value = true;
        }
        AggregateValue::Float64(v) => {
            (*state).is_float = true;
            (*state).float_sum += v;
            (*state).has_value = true;
        }
        AggregateValue::UInt64(v) => {
            (*state).int_sum = (*state).int_sum.wrapping_add(*v as i64);
            (*state).has_value = true;
        }
        AggregateValue::Null => {}
    }
}

pub(crate) unsafe extern "C" fn combine(
    _info: sys::duckdb_function_info,
    source: *mut sys::duckdb_aggregate_state,
    target: *mut sys::duckdb_aggregate_state,
    count: sys::idx_t,
) {
    for i in 0..count as usize {
        let src = *source.add(i) as *const SumState;
        let tgt = *target.add(i) as *mut SumState;
        if (*src).has_value {
            if (*src).is_float {
                (*tgt).is_float = true;
                (*tgt).float_sum += (*src).float_sum;
            } else {
                (*tgt).int_sum = (*tgt).int_sum.wrapping_add((*src).int_sum);
            }
            (*tgt).has_value = true;
        }
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
        let state = *source.add(i) as *const SumState;
        let idx = (offset as usize + i) as u64;
        if !(*state).has_value {
            gpu_columns::set_vector_null(result, idx);
        } else if (*state).is_float {
            gpu_columns::write_f64_to_vector(result, idx, (*state).float_sum);
        } else {
            gpu_columns::write_i64_to_vector(result, idx, (*state).int_sum);
        }
    }
}
