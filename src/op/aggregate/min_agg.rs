//! sirius_min 的 DuckDB C 回调

use crate::ext_api;
use crate::ffi::sys;
use crate::gpu_columns;
use crate::op::aggregate::traits::AggregateValue;

#[repr(C)]
pub(crate) struct MinState {
    int_min: i64,
    float_min: f64,
    is_float: bool,
    has_value: bool,
}

pub(crate) unsafe extern "C" fn state_size(_info: sys::duckdb_function_info) -> sys::idx_t {
    std::mem::size_of::<MinState>() as sys::idx_t
}

pub(crate) unsafe extern "C" fn init(
    _info: sys::duckdb_function_info,
    state: sys::duckdb_aggregate_state,
) {
    std::ptr::write(state as *mut MinState, MinState {
        int_min: i64::MAX,
        float_min: f64::INFINITY,
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
    match ctx.executor().min(&col) {
        Ok(val) => {
            let state_ptr = *states as *mut MinState;
            match val {
                AggregateValue::Int64(v) => {
                    if !(*state_ptr).has_value || v < (*state_ptr).int_min {
                        (*state_ptr).int_min = v;
                    }
                    (*state_ptr).has_value = true;
                }
                AggregateValue::Float64(v) => {
                    (*state_ptr).is_float = true;
                    if !(*state_ptr).has_value || v < (*state_ptr).float_min {
                        (*state_ptr).float_min = v;
                    }
                    (*state_ptr).has_value = true;
                }
                AggregateValue::Null => {}
                _ => {}
            }
        }
        Err(_) => {
            let msg = std::ffi::CString::new("sirius_min update failed").unwrap();
            ext_api::duckdb_aggregate_function_set_error(info, msg.as_ptr());
        }
    }
}

pub(crate) unsafe extern "C" fn combine(
    _info: sys::duckdb_function_info,
    source: *mut sys::duckdb_aggregate_state,
    target: *mut sys::duckdb_aggregate_state,
    count: sys::idx_t,
) {
    for i in 0..count as usize {
        let src = *source.add(i) as *const MinState;
        let tgt = *target.add(i) as *mut MinState;
        if (*src).has_value {
            if (*src).is_float {
                (*tgt).is_float = true;
                if !(*tgt).has_value || (*src).float_min < (*tgt).float_min {
                    (*tgt).float_min = (*src).float_min;
                }
            } else if !(*tgt).has_value || (*src).int_min < (*tgt).int_min {
                (*tgt).int_min = (*src).int_min;
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
        let state = *source.add(i) as *const MinState;
        let idx = (offset as usize + i) as u64;
        if !(*state).has_value {
            gpu_columns::set_vector_null(result, idx);
        } else if (*state).is_float {
            gpu_columns::write_f64_to_vector(result, idx, (*state).float_min);
        } else {
            gpu_columns::write_i64_to_vector(result, idx, (*state).int_min);
        }
    }
}
