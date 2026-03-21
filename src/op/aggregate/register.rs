//! 聚合函数注册 — 将 sirius 聚合函数注册到 DuckDB

use crate::ext_api;
use crate::ffi::sys;
use super::{avg_agg, count_agg, max_agg, min_agg, sum_agg};

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

/// 注册 sirius_count_star()（无参数，计所有行）
pub unsafe fn register_count_star(conn: sys::duckdb_connection) {
    let name = std::ffi::CString::new("sirius_count_star").unwrap();
    let set = ext_api::duckdb_create_aggregate_function_set(name.as_ptr());

    let func = ext_api::duckdb_create_aggregate_function();
    ext_api::duckdb_aggregate_function_set_name(func, name.as_ptr());
    let return_lt = ext_api::duckdb_create_logical_type(sys::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT);
    ext_api::duckdb_aggregate_function_set_return_type(func, return_lt);
    ext_api::duckdb_aggregate_function_set_special_handling(func);
    ext_api::duckdb_aggregate_function_set_functions(
        func,
        Some(count_agg::state_size),
        Some(count_agg::init),
        Some(count_agg::update_star),
        Some(count_agg::combine),
        Some(count_agg::finalize),
    );
    ext_api::duckdb_add_aggregate_function_to_set(set, func);
    ext_api::duckdb_register_aggregate_function_set(conn, set);
    ext_api::duckdb_destroy_logical_type(&mut { return_lt });
    ext_api::duckdb_destroy_aggregate_function(&mut { func });
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

/// 注册 sirius_avg（数值类型 → DOUBLE）
pub unsafe fn register_avg(conn: sys::duckdb_connection) {
    let name = std::ffi::CString::new("sirius_avg").unwrap();
    let set = ext_api::duckdb_create_aggregate_function_set(name.as_ptr());

    for &ty in &[
        sys::DUCKDB_TYPE_DUCKDB_TYPE_TINYINT,
        sys::DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT,
        sys::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER,
        sys::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT,
        sys::DUCKDB_TYPE_DUCKDB_TYPE_FLOAT,
        sys::DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE,
    ] {
        let func = ext_api::duckdb_create_aggregate_function();
        ext_api::duckdb_aggregate_function_set_name(func, name.as_ptr());
        let input_lt = ext_api::duckdb_create_logical_type(ty);
        let return_lt = ext_api::duckdb_create_logical_type(sys::DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE);
        ext_api::duckdb_aggregate_function_add_parameter(func, input_lt);
        ext_api::duckdb_aggregate_function_set_return_type(func, return_lt);
        ext_api::duckdb_aggregate_function_set_functions(
            func,
            Some(avg_agg::state_size),
            Some(avg_agg::init),
            Some(avg_agg::update),
            Some(avg_agg::combine),
            Some(avg_agg::finalize),
        );
        ext_api::duckdb_add_aggregate_function_to_set(set, func);
        ext_api::duckdb_destroy_logical_type(&mut { input_lt });
        ext_api::duckdb_destroy_logical_type(&mut { return_lt });
        ext_api::duckdb_destroy_aggregate_function(&mut { func });
    }

    ext_api::duckdb_register_aggregate_function_set(conn, set);
    ext_api::duckdb_destroy_aggregate_function_set(&mut { set });
}

/// 注册 sirius_min（返回与输入同类型）
pub unsafe fn register_min(conn: sys::duckdb_connection) {
    let name = std::ffi::CString::new("sirius_min").unwrap();
    let set = ext_api::duckdb_create_aggregate_function_set(name.as_ptr());

    for &ty in &[
        sys::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER,
        sys::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT,
        sys::DUCKDB_TYPE_DUCKDB_TYPE_FLOAT,
        sys::DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE,
    ] {
        let func = ext_api::duckdb_create_aggregate_function();
        ext_api::duckdb_aggregate_function_set_name(func, name.as_ptr());
        let input_lt = ext_api::duckdb_create_logical_type(ty);
        let return_lt = ext_api::duckdb_create_logical_type(
            if ty == sys::DUCKDB_TYPE_DUCKDB_TYPE_FLOAT || ty == sys::DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE {
                sys::DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE
            } else {
                sys::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT
            }
        );
        ext_api::duckdb_aggregate_function_add_parameter(func, input_lt);
        ext_api::duckdb_aggregate_function_set_return_type(func, return_lt);
        ext_api::duckdb_aggregate_function_set_functions(
            func,
            Some(min_agg::state_size),
            Some(min_agg::init),
            Some(min_agg::update),
            Some(min_agg::combine),
            Some(min_agg::finalize),
        );
        ext_api::duckdb_add_aggregate_function_to_set(set, func);
        ext_api::duckdb_destroy_logical_type(&mut { input_lt });
        ext_api::duckdb_destroy_logical_type(&mut { return_lt });
        ext_api::duckdb_destroy_aggregate_function(&mut { func });
    }

    ext_api::duckdb_register_aggregate_function_set(conn, set);
    ext_api::duckdb_destroy_aggregate_function_set(&mut { set });
}

/// 注册 sirius_max（返回与输入同类型）
pub unsafe fn register_max(conn: sys::duckdb_connection) {
    let name = std::ffi::CString::new("sirius_max").unwrap();
    let set = ext_api::duckdb_create_aggregate_function_set(name.as_ptr());

    for &ty in &[
        sys::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER,
        sys::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT,
        sys::DUCKDB_TYPE_DUCKDB_TYPE_FLOAT,
        sys::DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE,
    ] {
        let func = ext_api::duckdb_create_aggregate_function();
        ext_api::duckdb_aggregate_function_set_name(func, name.as_ptr());
        let input_lt = ext_api::duckdb_create_logical_type(ty);
        let return_lt = ext_api::duckdb_create_logical_type(
            if ty == sys::DUCKDB_TYPE_DUCKDB_TYPE_FLOAT || ty == sys::DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE {
                sys::DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE
            } else {
                sys::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT
            }
        );
        ext_api::duckdb_aggregate_function_add_parameter(func, input_lt);
        ext_api::duckdb_aggregate_function_set_return_type(func, return_lt);
        ext_api::duckdb_aggregate_function_set_functions(
            func,
            Some(max_agg::state_size),
            Some(max_agg::init),
            Some(max_agg::update),
            Some(max_agg::combine),
            Some(max_agg::finalize),
        );
        ext_api::duckdb_add_aggregate_function_to_set(set, func);
        ext_api::duckdb_destroy_logical_type(&mut { input_lt });
        ext_api::duckdb_destroy_logical_type(&mut { return_lt });
        ext_api::duckdb_destroy_aggregate_function(&mut { func });
    }

    ext_api::duckdb_register_aggregate_function_set(conn, set);
    ext_api::duckdb_destroy_aggregate_function_set(&mut { set });
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
