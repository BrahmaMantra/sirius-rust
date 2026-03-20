//! DuckDB 扩展入口 — 对应 sirius/src/sirius_extension.cpp
//!
//! 支持两种 ABI：
//! - `sirius_rust_init(db)`: 传统入口（集成测试用，需 link-duckdb）
//! - `sirius_rust_init_c_api(info, access)`: C_STRUCT ABI（标准 DuckDB CLI 加载）

use crate::ext_api;
use crate::ffi::ext_sys;
use crate::ffi::sys;
use crate::op::aggregate::register;
use std::os::raw::c_char;

/// C_STRUCT ABI 入口点 — 标准 DuckDB CLI 通过此函数加载扩展
///
/// DuckDB 查找 `{name}_init_c_api` 符号，传入 info + access。
/// 扩展通过 `access->get_api(info, version)` 获取函数指针表。
///
/// # Safety
/// 由 DuckDB 在加载扩展时调用
#[no_mangle]
pub unsafe extern "C" fn sirius_rust_init_c_api(
    info: sys::duckdb_extension_info,
    access: *mut ext_sys::duckdb_extension_access,
) -> bool {
    let access_ref = &*access;

    // 获取 DuckDB C API 函数指针表
    let version = c"v1.2.0".as_ptr();
    let get_api = access_ref.get_api.expect("get_api is null");
    let api_ptr = get_api(info, version);
    if api_ptr.is_null() {
        let set_error = access_ref.set_error.expect("set_error is null");
        set_error(info, c"Failed to get DuckDB C API v1.2.0".as_ptr());
        return false;
    }

    // 初始化全局函数指针表
    let api = api_ptr as *const ext_sys::duckdb_ext_api_v1;
    ext_api::init_from_ext_api(api);

    // 获取数据库连接
    let get_database = access_ref.get_database.expect("get_database is null");
    let db_ptr = get_database(info);
    let mut conn: sys::duckdb_connection = std::ptr::null_mut();
    if ext_api::duckdb_connect(*db_ptr, &mut conn) != sys::duckdb_state_DuckDBSuccess {
        let set_error = access_ref.set_error.expect("set_error is null");
        set_error(info, c"Failed to connect to database".as_ptr());
        return false;
    }

    // 初始化全局上下文（触发 GPU 检测）
    let _ = crate::sirius_context::global();

    // 注册聚合函数
    register::register_sum(conn);
    register::register_count(conn);

    ext_api::duckdb_disconnect(&mut conn);
    true
}

/// 传统入口点 — 集成测试使用（需要 link-duckdb feature）
///
/// # Safety
/// 由 DuckDB 在加载扩展时调用
#[no_mangle]
pub unsafe extern "C" fn sirius_rust_init(db: sys::duckdb_database) {
    let mut conn: sys::duckdb_connection = std::ptr::null_mut();
    if ext_api::duckdb_connect(db, &mut conn) != sys::duckdb_state_DuckDBSuccess {
        return;
    }

    // 初始化全局上下文（触发 GPU 检测）
    let _ = crate::sirius_context::global();

    register::register_sum(conn);
    register::register_count(conn);

    ext_api::duckdb_disconnect(&mut conn);
}

/// DuckDB 扩展版本
///
/// # Safety
/// 由 DuckDB 调用获取扩展版本
#[no_mangle]
pub unsafe extern "C" fn sirius_rust_version() -> *const c_char {
    c"v0.1.0".as_ptr()
}
