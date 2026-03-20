//! 集成测试 — 在真实 DuckDB 中运行 sirius_sum / sirius_count

use sirius_rust::ffi::sys;
use std::ffi::CString;

/// 辅助函数：执行 SQL 并返回第一行第一列的 i64 值
unsafe fn query_i64(conn: sys::duckdb_connection, sql: &str) -> i64 {
    let c_sql = CString::new(sql).unwrap();
    let mut result: sys::duckdb_result = std::mem::zeroed();

    let rc = sys::duckdb_query(conn, c_sql.as_ptr(), &mut result);
    if rc != sys::duckdb_state_DuckDBSuccess {
        let err = sys::duckdb_result_error(&mut result);
        let err_msg = if !err.is_null() {
            std::ffi::CStr::from_ptr(err).to_string_lossy().into_owned()
        } else {
            "unknown error".to_string()
        };
        sys::duckdb_destroy_result(&mut result);
        panic!("SQL failed: {sql}\nError: {err_msg}");
    }

    let val = sys::duckdb_value_int64(&mut result, 0, 0);
    sys::duckdb_destroy_result(&mut result);
    val
}

/// 辅助函数：执行 SQL 并返回第一行第一列的 f64 值
unsafe fn query_f64(conn: sys::duckdb_connection, sql: &str) -> f64 {
    let c_sql = CString::new(sql).unwrap();
    let mut result: sys::duckdb_result = std::mem::zeroed();

    let rc = sys::duckdb_query(conn, c_sql.as_ptr(), &mut result);
    if rc != sys::duckdb_state_DuckDBSuccess {
        let err = sys::duckdb_result_error(&mut result);
        let err_msg = if !err.is_null() {
            std::ffi::CStr::from_ptr(err).to_string_lossy().into_owned()
        } else {
            "unknown error".to_string()
        };
        sys::duckdb_destroy_result(&mut result);
        panic!("SQL failed: {sql}\nError: {err_msg}");
    }

    let val = sys::duckdb_value_double(&mut result, 0, 0);
    sys::duckdb_destroy_result(&mut result);
    val
}

/// 辅助函数：执行 SQL（不需要返回值）
unsafe fn exec(conn: sys::duckdb_connection, sql: &str) {
    let c_sql = CString::new(sql).unwrap();
    let mut result: sys::duckdb_result = std::mem::zeroed();

    let rc = sys::duckdb_query(conn, c_sql.as_ptr(), &mut result);
    if rc != sys::duckdb_state_DuckDBSuccess {
        let err = sys::duckdb_result_error(&mut result);
        let err_msg = if !err.is_null() {
            std::ffi::CStr::from_ptr(err).to_string_lossy().into_owned()
        } else {
            "unknown error".to_string()
        };
        sys::duckdb_destroy_result(&mut result);
        panic!("SQL failed: {sql}\nError: {err_msg}");
    }
    sys::duckdb_destroy_result(&mut result);
}

/// 打开内存数据库 + 注册扩展，返回 (db, conn)
unsafe fn setup() -> (sys::duckdb_database, sys::duckdb_connection) {
    let mut db: sys::duckdb_database = std::ptr::null_mut();
    let rc = sys::duckdb_open(std::ptr::null(), &mut db);
    assert_eq!(rc, sys::duckdb_state_DuckDBSuccess, "Failed to open db");

    // 注册 sirius 扩展
    sirius_rust::sirius_rust_init(db);

    let mut conn: sys::duckdb_connection = std::ptr::null_mut();
    let rc = sys::duckdb_connect(db, &mut conn);
    assert_eq!(rc, sys::duckdb_state_DuckDBSuccess, "Failed to connect");

    (db, conn)
}

unsafe fn teardown(mut db: sys::duckdb_database, mut conn: sys::duckdb_connection) {
    sys::duckdb_disconnect(&mut conn);
    sys::duckdb_close(&mut db);
}

#[test]
fn test_sirius_sum_integer() {
    unsafe {
        let (db, conn) = setup();

        exec(conn, "CREATE TABLE t AS SELECT i::INTEGER AS val FROM range(1, 101) t(i)");

        // sirius_sum(1..100) = 5050
        let result = query_i64(conn, "SELECT sirius_sum(val) FROM t");
        assert_eq!(result, 5050);

        teardown(db, conn);
    }
}

#[test]
fn test_sirius_sum_bigint() {
    unsafe {
        let (db, conn) = setup();

        exec(conn, "CREATE TABLE t AS SELECT i::BIGINT AS val FROM range(1, 10001) t(i)");

        let result = query_i64(conn, "SELECT sirius_sum(val) FROM t");
        assert_eq!(result, 50_005_000);

        teardown(db, conn);
    }
}

#[test]
fn test_sirius_sum_double() {
    unsafe {
        let (db, conn) = setup();

        exec(conn, "CREATE TABLE t (val DOUBLE)");
        exec(conn, "INSERT INTO t VALUES (1.5), (2.5), (3.0), (4.0)");

        let result = query_f64(conn, "SELECT sirius_sum(val) FROM t");
        assert!((result - 11.0).abs() < 1e-10, "Expected 11.0, got {result}");

        teardown(db, conn);
    }
}

#[test]
fn test_sirius_count_integer() {
    unsafe {
        let (db, conn) = setup();

        exec(conn, "CREATE TABLE t AS SELECT i::INTEGER AS val FROM range(100) t(i)");

        let result = query_i64(conn, "SELECT sirius_count(val) FROM t");
        assert_eq!(result, 100);

        teardown(db, conn);
    }
}

#[test]
fn test_sirius_count_with_nulls() {
    unsafe {
        let (db, conn) = setup();

        exec(conn, "CREATE TABLE t (val INTEGER)");
        exec(conn, "INSERT INTO t VALUES (1), (NULL), (3), (NULL), (5)");

        let result = query_i64(conn, "SELECT sirius_count(val) FROM t");
        assert_eq!(result, 3); // 只计非 NULL

        teardown(db, conn);
    }
}

#[test]
fn test_sirius_sum_vs_native_sum() {
    unsafe {
        let (db, conn) = setup();

        exec(conn, "CREATE TABLE t AS SELECT i::INTEGER AS val FROM range(1, 10001) t(i)");

        let sirius = query_i64(conn, "SELECT sirius_sum(val) FROM t");

        // 原生 sum() 需要 core_functions 扩展，如果不可用则跳过对比
        let c_sql = CString::new("SELECT sum(val) FROM t").unwrap();
        let mut result: sys::duckdb_result = std::mem::zeroed();
        let rc = sys::duckdb_query(conn, c_sql.as_ptr(), &mut result);
        if rc == sys::duckdb_state_DuckDBSuccess {
            let native = sys::duckdb_value_int64(&mut result, 0, 0);
            sys::duckdb_destroy_result(&mut result);
            assert_eq!(sirius, native, "sirius_sum should match native sum");
        } else {
            sys::duckdb_destroy_result(&mut result);
            // 验证 sirius_sum 结果正确即可
            assert_eq!(sirius, 50_005_000);
        }

        teardown(db, conn);
    }
}

#[test]
fn test_sirius_count_vs_native_count() {
    unsafe {
        let (db, conn) = setup();

        exec(conn, "CREATE TABLE t (val INTEGER)");
        exec(conn, "INSERT INTO t VALUES (1), (NULL), (3), (NULL), (5)");

        let sirius = query_i64(conn, "SELECT sirius_count(val) FROM t");

        let c_sql = CString::new("SELECT count(val) FROM t").unwrap();
        let mut result: sys::duckdb_result = std::mem::zeroed();
        let rc = sys::duckdb_query(conn, c_sql.as_ptr(), &mut result);
        if rc == sys::duckdb_state_DuckDBSuccess {
            let native = sys::duckdb_value_int64(&mut result, 0, 0);
            sys::duckdb_destroy_result(&mut result);
            assert_eq!(sirius, native, "sirius_count should match native count");
        } else {
            sys::duckdb_destroy_result(&mut result);
            assert_eq!(sirius, 3);
        }

        teardown(db, conn);
    }
}
