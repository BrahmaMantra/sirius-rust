/// DuckDB C API FFI 绑定，由 bindgen 从 duckdb/src/include/duckdb.h 生成
#[allow(non_upper_case_globals)]
#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
#[allow(dead_code)]
#[allow(clippy::all)]
pub mod sys {
    include!(concat!(env!("OUT_DIR"), "/duckdb_bindings.rs"));
}

/// DuckDB Extension C API 绑定 (duckdb_ext_api_v1)
/// 由 bindgen 从 duckdb/src/include/duckdb_extension.h 生成
#[allow(non_upper_case_globals)]
#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
#[allow(dead_code)]
#[allow(clippy::all)]
pub mod ext_sys {
    include!(concat!(env!("OUT_DIR"), "/duckdb_ext_bindings.rs"));
}
