//! sirius_avg 的 DuckDB C 回调

use crate::ext_api;
use crate::ffi::sys;
use crate::gpu_columns;

#[repr(C)]
pub(crate) struct AvgState {
    sum: f64,
    count: i64,
}

pub(crate) unsafe extern "C" fn state_size(_info: sys::duckdb_function_info) -> sys::idx_t {
    std::mem::size_of::<AvgState>() as sys::idx_t
}

pub(crate) unsafe extern "C" fn init(
    _info: sys::duckdb_function_info,
    state: sys::duckdb_aggregate_state,
) {
    std::ptr::write(state as *mut AvgState, AvgState { sum: 0.0, count: 0 });
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
    match ctx.executor().avg(&col) {
        Ok(crate::op::aggregate::traits::AggregateValue::Float64(v)) => {
            let state_ptr = *states as *mut AvgState;
            let valid = col.valid_count() as i64;
            (*state_ptr).sum += v * valid as f64;
            (*state_ptr).count += valid;
        }
        Ok(_) => {}
        Err(_) => {
            let msg = std::ffi::CString::new("sirius_avg update failed").unwrap();
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
        let src = *source.add(i) as *const AvgState;
        let tgt = *target.add(i) as *mut AvgState;
        (*tgt).sum += (*src).sum;
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
        let state = *source.add(i) as *const AvgState;
        let idx = (offset as usize + i) as u64;
        if (*state).count == 0 {
            gpu_columns::set_vector_null(result, idx);
        } else {
            gpu_columns::write_f64_to_vector(result, idx, (*state).sum / (*state).count as f64);
        }
    }
}
