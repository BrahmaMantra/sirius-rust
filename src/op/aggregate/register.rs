//! 聚合函数注册 — 将 sirius 聚合函数注册到 DuckDB

use crate::ext_api;
use crate::ffi::sys;
use super::{count_agg, sum_agg};

/// 注册 sirius_sum（多类型重载）
///
/// # Safety
/// `conn` 必须是有效的 DuckDB 连接
pub unsafe fn register_sum(conn: sys::duckdb_connection) {
    let name = std::ffi::CString::new("sirius_sum").unwrap();
    let set = ext_api::duckdb_create_aggregate_function_set(name.as_ptr());

    // 整数类型 → BIGINT
    for &ty in &[
        sys::DUCKDB_TYPE_DUCKDB_TYPE_TINYINT,
        sys::DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT,
        sys::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER,
        sys::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT,
    ] {
        add_sum_overload(set, name.as_ptr(), ty, sys::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT);
    }

    // 浮点类型 → DOUBLE
    for &ty in &[
        sys::DUCKDB_TYPE_DUCKDB_TYPE_FLOAT,
        sys::DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE,
    ] {
        add_sum_overload(set, name.as_ptr(), ty, sys::DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE);
    }

    ext_api::duckdb_register_aggregate_function_set(conn, set);
    ext_api::duckdb_destroy_aggregate_function_set(&mut { set });
}

/// 注册 sirius_count（多类型重载）
///
/// # Safety
/// `conn` 必须是有效的 DuckDB 连接
pub unsafe fn register_count(conn: sys::duckdb_connection) {
    let name = std::ffi::CString::new("sirius_count").unwrap();
    let set = ext_api::duckdb_create_aggregate_function_set(name.as_ptr());

    for &ty in &[
        sys::DUCKDB_TYPE_DUCKDB_TYPE_TINYINT,
        sys::DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT,
        sys::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER,
        sys::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT,
        sys::DUCKDB_TYPE_DUCKDB_TYPE_FLOAT,
        sys::DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE,
        sys::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR,
    ] {
        add_count_overload(set, name.as_ptr(), ty);
    }

    ext_api::duckdb_register_aggregate_function_set(conn, set);
    ext_api::duckdb_destroy_aggregate_function_set(&mut { set });
}

unsafe fn add_sum_overload(
    set: sys::duckdb_aggregate_function_set,
    name: *const std::os::raw::c_char,
    input_type: sys::DUCKDB_TYPE,
    return_type: sys::DUCKDB_TYPE,
) {
    let func = ext_api::duckdb_create_aggregate_function();
    // function set 中每个子函数也必须设置 name
    ext_api::duckdb_aggregate_function_set_name(func, name);

    let input_lt = ext_api::duckdb_create_logical_type(input_type);
    let return_lt = ext_api::duckdb_create_logical_type(return_type);

    ext_api::duckdb_aggregate_function_add_parameter(func, input_lt);
    ext_api::duckdb_aggregate_function_set_return_type(func, return_lt);
    ext_api::duckdb_aggregate_function_set_functions(
        func,
        Some(sum_agg::state_size),
        Some(sum_agg::init),
        Some(sum_agg::update),
        Some(sum_agg::combine),
        Some(sum_agg::finalize),
    );

    ext_api::duckdb_add_aggregate_function_to_set(set, func);

    ext_api::duckdb_destroy_logical_type(&mut { input_lt });
    ext_api::duckdb_destroy_logical_type(&mut { return_lt });
    ext_api::duckdb_destroy_aggregate_function(&mut { func });
}

unsafe fn add_count_overload(
    set: sys::duckdb_aggregate_function_set,
    name: *const std::os::raw::c_char,
    input_type: sys::DUCKDB_TYPE,
) {
    let func = ext_api::duckdb_create_aggregate_function();
    ext_api::duckdb_aggregate_function_set_name(func, name);

    let input_lt = ext_api::duckdb_create_logical_type(input_type);
    let return_lt = ext_api::duckdb_create_logical_type(sys::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT);

    ext_api::duckdb_aggregate_function_add_parameter(func, input_lt);
    ext_api::duckdb_aggregate_function_set_return_type(func, return_lt);
    ext_api::duckdb_aggregate_function_set_special_handling(func);
    ext_api::duckdb_aggregate_function_set_functions(
        func,
        Some(count_agg::state_size),
        Some(count_agg::init),
        Some(count_agg::update),
        Some(count_agg::combine),
        Some(count_agg::finalize),
    );

    ext_api::duckdb_add_aggregate_function_to_set(set, func);

    ext_api::duckdb_destroy_logical_type(&mut { input_lt });
    ext_api::duckdb_destroy_logical_type(&mut { return_lt });
    ext_api::duckdb_destroy_aggregate_function(&mut { func });
}
