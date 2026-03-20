//! DuckDB Extension C API 函数指针表
//!
//! DuckDB 以 RTLD_LOCAL 加载扩展，扩展无法直接解析 DuckDB 符号。
//! C_STRUCT ABI 通过 `duckdb_ext_api_v1` 函数指针表传递所有 C API 函数。
//!
//! 本模块提供：
//! - 全局 API 表存储
//! - 所有 DuckDB C API 调用的 wrapper 函数
//! - `link-duckdb` feature 时直接调用 bindgen 函数（用于集成测试）

use crate::ffi::ext_sys;
use crate::ffi::sys;
use std::sync::atomic::{AtomicBool, Ordering};

/// 全局 API 函数指针表（由 C_STRUCT ABI 入口点初始化）
static mut EXT_API: std::mem::MaybeUninit<ext_sys::duckdb_ext_api_v1> =
    std::mem::MaybeUninit::uninit();
static API_INITIALIZED: AtomicBool = AtomicBool::new(false);

/// 从 DuckDB 提供的 API 表初始化全局函数指针
///
/// # Safety
/// `api` 必须是有效的 `duckdb_ext_api_v1` 指针
pub unsafe fn init_from_ext_api(api: *const ext_sys::duckdb_ext_api_v1) {
    unsafe {
        let ptr = std::ptr::addr_of_mut!(EXT_API);
        (*ptr).write(*api);
    }
    API_INITIALIZED.store(true, Ordering::Release);
}

/// 获取 API 表引用（仅在非 link-duckdb 模式下使用）
#[cfg(not(feature = "link-duckdb"))]
unsafe fn api() -> &'static ext_sys::duckdb_ext_api_v1 {
    debug_assert!(
        API_INITIALIZED.load(Ordering::Acquire),
        "DuckDB ext API not initialized"
    );
    unsafe {
        let ptr = std::ptr::addr_of!(EXT_API);
        (*ptr).assume_init_ref()
    }
}

// ============================================================
// Wrapper functions — 条件编译分发
//
// link-duckdb: 直接调用 bindgen 生成的函数（静态链接 DuckDB）
// 非 link-duckdb: 通过全局 API 表调用（loadable extension）
// ============================================================

// --- Connection ---

#[cfg(feature = "link-duckdb")]
pub unsafe fn duckdb_connect(
    database: sys::duckdb_database,
    out_connection: *mut sys::duckdb_connection,
) -> sys::duckdb_state {
    sys::duckdb_connect(database, out_connection)
}

#[cfg(not(feature = "link-duckdb"))]
pub unsafe fn duckdb_connect(
    database: sys::duckdb_database,
    out_connection: *mut sys::duckdb_connection,
) -> sys::duckdb_state {
    (api().duckdb_connect.unwrap())(database, out_connection)
}

#[cfg(feature = "link-duckdb")]
pub unsafe fn duckdb_disconnect(connection: *mut sys::duckdb_connection) {
    sys::duckdb_disconnect(connection)
}

#[cfg(not(feature = "link-duckdb"))]
pub unsafe fn duckdb_disconnect(connection: *mut sys::duckdb_connection) {
    (api().duckdb_disconnect.unwrap())(connection)
}

// --- Aggregate Function ---

#[cfg(feature = "link-duckdb")]
pub unsafe fn duckdb_create_aggregate_function() -> sys::duckdb_aggregate_function {
    sys::duckdb_create_aggregate_function()
}

#[cfg(not(feature = "link-duckdb"))]
pub unsafe fn duckdb_create_aggregate_function() -> sys::duckdb_aggregate_function {
    (api().duckdb_create_aggregate_function.unwrap())()
}

#[cfg(feature = "link-duckdb")]
pub unsafe fn duckdb_destroy_aggregate_function(
    aggregate_function: *mut sys::duckdb_aggregate_function,
) {
    sys::duckdb_destroy_aggregate_function(aggregate_function)
}

#[cfg(not(feature = "link-duckdb"))]
pub unsafe fn duckdb_destroy_aggregate_function(
    aggregate_function: *mut sys::duckdb_aggregate_function,
) {
    (api().duckdb_destroy_aggregate_function.unwrap())(aggregate_function)
}

#[cfg(feature = "link-duckdb")]
pub unsafe fn duckdb_aggregate_function_set_name(
    aggregate_function: sys::duckdb_aggregate_function,
    name: *const std::os::raw::c_char,
) {
    sys::duckdb_aggregate_function_set_name(aggregate_function, name)
}

#[cfg(not(feature = "link-duckdb"))]
pub unsafe fn duckdb_aggregate_function_set_name(
    aggregate_function: sys::duckdb_aggregate_function,
    name: *const std::os::raw::c_char,
) {
    (api().duckdb_aggregate_function_set_name.unwrap())(aggregate_function, name)
}

#[cfg(feature = "link-duckdb")]
pub unsafe fn duckdb_aggregate_function_add_parameter(
    aggregate_function: sys::duckdb_aggregate_function,
    type_: sys::duckdb_logical_type,
) {
    sys::duckdb_aggregate_function_add_parameter(aggregate_function, type_)
}

#[cfg(not(feature = "link-duckdb"))]
pub unsafe fn duckdb_aggregate_function_add_parameter(
    aggregate_function: sys::duckdb_aggregate_function,
    type_: sys::duckdb_logical_type,
) {
    (api().duckdb_aggregate_function_add_parameter.unwrap())(aggregate_function, type_)
}

#[cfg(feature = "link-duckdb")]
pub unsafe fn duckdb_aggregate_function_set_return_type(
    aggregate_function: sys::duckdb_aggregate_function,
    type_: sys::duckdb_logical_type,
) {
    sys::duckdb_aggregate_function_set_return_type(aggregate_function, type_)
}

#[cfg(not(feature = "link-duckdb"))]
pub unsafe fn duckdb_aggregate_function_set_return_type(
    aggregate_function: sys::duckdb_aggregate_function,
    type_: sys::duckdb_logical_type,
) {
    (api().duckdb_aggregate_function_set_return_type.unwrap())(aggregate_function, type_)
}

#[cfg(feature = "link-duckdb")]
pub unsafe fn duckdb_aggregate_function_set_functions(
    aggregate_function: sys::duckdb_aggregate_function,
    state_size: sys::duckdb_aggregate_state_size,
    state_init: sys::duckdb_aggregate_init_t,
    update: sys::duckdb_aggregate_update_t,
    combine: sys::duckdb_aggregate_combine_t,
    finalize: sys::duckdb_aggregate_finalize_t,
) {
    sys::duckdb_aggregate_function_set_functions(
        aggregate_function,
        state_size,
        state_init,
        update,
        combine,
        finalize,
    )
}

#[cfg(not(feature = "link-duckdb"))]
pub unsafe fn duckdb_aggregate_function_set_functions(
    aggregate_function: sys::duckdb_aggregate_function,
    state_size: sys::duckdb_aggregate_state_size,
    state_init: sys::duckdb_aggregate_init_t,
    update: sys::duckdb_aggregate_update_t,
    combine: sys::duckdb_aggregate_combine_t,
    finalize: sys::duckdb_aggregate_finalize_t,
) {
    (api().duckdb_aggregate_function_set_functions.unwrap())(
        aggregate_function,
        state_size,
        state_init,
        update,
        combine,
        finalize,
    )
}

#[cfg(feature = "link-duckdb")]
pub unsafe fn duckdb_aggregate_function_set_special_handling(
    aggregate_function: sys::duckdb_aggregate_function,
) {
    sys::duckdb_aggregate_function_set_special_handling(aggregate_function)
}

#[cfg(not(feature = "link-duckdb"))]
pub unsafe fn duckdb_aggregate_function_set_special_handling(
    aggregate_function: sys::duckdb_aggregate_function,
) {
    (api().duckdb_aggregate_function_set_special_handling.unwrap())(aggregate_function)
}

#[cfg(feature = "link-duckdb")]
pub unsafe fn duckdb_aggregate_function_set_error(
    info: sys::duckdb_function_info,
    error: *const std::os::raw::c_char,
) {
    sys::duckdb_aggregate_function_set_error(info, error)
}

#[cfg(not(feature = "link-duckdb"))]
pub unsafe fn duckdb_aggregate_function_set_error(
    info: sys::duckdb_function_info,
    error: *const std::os::raw::c_char,
) {
    (api().duckdb_aggregate_function_set_error.unwrap())(info, error)
}

// --- Aggregate Function Set ---

#[cfg(feature = "link-duckdb")]
pub unsafe fn duckdb_create_aggregate_function_set(
    name: *const std::os::raw::c_char,
) -> sys::duckdb_aggregate_function_set {
    sys::duckdb_create_aggregate_function_set(name)
}

#[cfg(not(feature = "link-duckdb"))]
pub unsafe fn duckdb_create_aggregate_function_set(
    name: *const std::os::raw::c_char,
) -> sys::duckdb_aggregate_function_set {
    (api().duckdb_create_aggregate_function_set.unwrap())(name)
}

#[cfg(feature = "link-duckdb")]
pub unsafe fn duckdb_destroy_aggregate_function_set(
    aggregate_function_set: *mut sys::duckdb_aggregate_function_set,
) {
    sys::duckdb_destroy_aggregate_function_set(aggregate_function_set)
}

#[cfg(not(feature = "link-duckdb"))]
pub unsafe fn duckdb_destroy_aggregate_function_set(
    aggregate_function_set: *mut sys::duckdb_aggregate_function_set,
) {
    (api().duckdb_destroy_aggregate_function_set.unwrap())(aggregate_function_set)
}

#[cfg(feature = "link-duckdb")]
pub unsafe fn duckdb_add_aggregate_function_to_set(
    set: sys::duckdb_aggregate_function_set,
    function: sys::duckdb_aggregate_function,
) -> sys::duckdb_state {
    sys::duckdb_add_aggregate_function_to_set(set, function)
}

#[cfg(not(feature = "link-duckdb"))]
pub unsafe fn duckdb_add_aggregate_function_to_set(
    set: sys::duckdb_aggregate_function_set,
    function: sys::duckdb_aggregate_function,
) -> sys::duckdb_state {
    (api().duckdb_add_aggregate_function_to_set.unwrap())(set, function)
}

#[cfg(feature = "link-duckdb")]
pub unsafe fn duckdb_register_aggregate_function_set(
    con: sys::duckdb_connection,
    set: sys::duckdb_aggregate_function_set,
) -> sys::duckdb_state {
    sys::duckdb_register_aggregate_function_set(con, set)
}

#[cfg(not(feature = "link-duckdb"))]
pub unsafe fn duckdb_register_aggregate_function_set(
    con: sys::duckdb_connection,
    set: sys::duckdb_aggregate_function_set,
) -> sys::duckdb_state {
    (api().duckdb_register_aggregate_function_set.unwrap())(con, set)
}

// --- Logical Type ---

#[cfg(feature = "link-duckdb")]
pub unsafe fn duckdb_create_logical_type(type_: sys::DUCKDB_TYPE) -> sys::duckdb_logical_type {
    sys::duckdb_create_logical_type(type_)
}

#[cfg(not(feature = "link-duckdb"))]
pub unsafe fn duckdb_create_logical_type(type_: sys::DUCKDB_TYPE) -> sys::duckdb_logical_type {
    (api().duckdb_create_logical_type.unwrap())(type_)
}

#[cfg(feature = "link-duckdb")]
pub unsafe fn duckdb_destroy_logical_type(type_: *mut sys::duckdb_logical_type) {
    sys::duckdb_destroy_logical_type(type_)
}

#[cfg(not(feature = "link-duckdb"))]
pub unsafe fn duckdb_destroy_logical_type(type_: *mut sys::duckdb_logical_type) {
    (api().duckdb_destroy_logical_type.unwrap())(type_)
}

#[cfg(feature = "link-duckdb")]
pub unsafe fn duckdb_get_type_id(type_: sys::duckdb_logical_type) -> sys::duckdb_type {
    sys::duckdb_get_type_id(type_)
}

#[cfg(not(feature = "link-duckdb"))]
pub unsafe fn duckdb_get_type_id(type_: sys::duckdb_logical_type) -> sys::duckdb_type {
    (api().duckdb_get_type_id.unwrap())(type_)
}

// --- DataChunk / Vector ---

#[cfg(feature = "link-duckdb")]
pub unsafe fn duckdb_data_chunk_get_size(chunk: sys::duckdb_data_chunk) -> sys::idx_t {
    sys::duckdb_data_chunk_get_size(chunk)
}

#[cfg(not(feature = "link-duckdb"))]
pub unsafe fn duckdb_data_chunk_get_size(chunk: sys::duckdb_data_chunk) -> sys::idx_t {
    (api().duckdb_data_chunk_get_size.unwrap())(chunk)
}

#[cfg(feature = "link-duckdb")]
pub unsafe fn duckdb_data_chunk_get_vector(
    chunk: sys::duckdb_data_chunk,
    col_idx: sys::idx_t,
) -> sys::duckdb_vector {
    sys::duckdb_data_chunk_get_vector(chunk, col_idx)
}

#[cfg(not(feature = "link-duckdb"))]
pub unsafe fn duckdb_data_chunk_get_vector(
    chunk: sys::duckdb_data_chunk,
    col_idx: sys::idx_t,
) -> sys::duckdb_vector {
    (api().duckdb_data_chunk_get_vector.unwrap())(chunk, col_idx)
}

#[cfg(feature = "link-duckdb")]
pub unsafe fn duckdb_vector_get_column_type(
    vector: sys::duckdb_vector,
) -> sys::duckdb_logical_type {
    sys::duckdb_vector_get_column_type(vector)
}

#[cfg(not(feature = "link-duckdb"))]
pub unsafe fn duckdb_vector_get_column_type(
    vector: sys::duckdb_vector,
) -> sys::duckdb_logical_type {
    (api().duckdb_vector_get_column_type.unwrap())(vector)
}

#[cfg(feature = "link-duckdb")]
pub unsafe fn duckdb_vector_get_data(
    vector: sys::duckdb_vector,
) -> *mut std::os::raw::c_void {
    sys::duckdb_vector_get_data(vector)
}

#[cfg(not(feature = "link-duckdb"))]
pub unsafe fn duckdb_vector_get_data(
    vector: sys::duckdb_vector,
) -> *mut std::os::raw::c_void {
    (api().duckdb_vector_get_data.unwrap())(vector)
}

#[cfg(feature = "link-duckdb")]
pub unsafe fn duckdb_vector_get_validity(vector: sys::duckdb_vector) -> *mut u64 {
    sys::duckdb_vector_get_validity(vector)
}

#[cfg(not(feature = "link-duckdb"))]
pub unsafe fn duckdb_vector_get_validity(vector: sys::duckdb_vector) -> *mut u64 {
    (api().duckdb_vector_get_validity.unwrap())(vector)
}

#[cfg(feature = "link-duckdb")]
pub unsafe fn duckdb_vector_ensure_validity_writable(vector: sys::duckdb_vector) {
    sys::duckdb_vector_ensure_validity_writable(vector)
}

#[cfg(not(feature = "link-duckdb"))]
pub unsafe fn duckdb_vector_ensure_validity_writable(vector: sys::duckdb_vector) {
    (api().duckdb_vector_ensure_validity_writable.unwrap())(vector)
}

#[cfg(feature = "link-duckdb")]
pub unsafe fn duckdb_validity_set_row_validity(
    validity: *mut u64,
    row_idx: sys::idx_t,
    valid: bool,
) {
    sys::duckdb_validity_set_row_validity(validity, row_idx, valid)
}

#[cfg(not(feature = "link-duckdb"))]
pub unsafe fn duckdb_validity_set_row_validity(
    validity: *mut u64,
    row_idx: sys::idx_t,
    valid: bool,
) {
    (api().duckdb_validity_set_row_validity.unwrap())(validity, row_idx, valid)
}
