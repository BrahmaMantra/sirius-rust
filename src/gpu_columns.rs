//! DuckDB ↔ sirius 数据转换 — 对应 sirius/src/gpu_columns.cpp
//!
//! 将 DuckDB 的 DataChunk/Vector 转为 sirius Column，
//! 以及将聚合结果写回 DuckDB Vector。

use crate::data::column::Column;
use crate::data::types::DataType;
use crate::ext_api;
use crate::ffi::sys;

/// DuckDB DUCKDB_TYPE → sirius DataType
pub fn from_duckdb_type(ty: sys::DUCKDB_TYPE) -> Option<DataType> {
    match ty {
        sys::DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN => Some(DataType::Boolean),
        sys::DUCKDB_TYPE_DUCKDB_TYPE_TINYINT => Some(DataType::Int8),
        sys::DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT => Some(DataType::Int16),
        sys::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER => Some(DataType::Int32),
        sys::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT => Some(DataType::Int64),
        sys::DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT => Some(DataType::UInt8),
        sys::DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT => Some(DataType::UInt16),
        sys::DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER => Some(DataType::UInt32),
        sys::DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT => Some(DataType::UInt64),
        sys::DUCKDB_TYPE_DUCKDB_TYPE_FLOAT => Some(DataType::Float32),
        sys::DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE => Some(DataType::Float64),
        _ => None,
    }
}

/// sirius DataType → DuckDB DUCKDB_TYPE
pub fn to_duckdb_type(dt: DataType) -> sys::DUCKDB_TYPE {
    match dt {
        DataType::Boolean => sys::DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN,
        DataType::Int8 => sys::DUCKDB_TYPE_DUCKDB_TYPE_TINYINT,
        DataType::Int16 => sys::DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT,
        DataType::Int32 => sys::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER,
        DataType::Int64 | DataType::BigInt => sys::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT,
        DataType::UInt8 => sys::DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT,
        DataType::UInt16 => sys::DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT,
        DataType::UInt32 => sys::DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER,
        DataType::UInt64 => sys::DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT,
        DataType::Float32 => sys::DUCKDB_TYPE_DUCKDB_TYPE_FLOAT,
        DataType::Float64 => sys::DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE,
    }
}

/// 从 DuckDB DataChunk 的第 col_idx 列提取数据到 sirius Column
///
/// # Safety
/// `chunk` 必须是有效的 DuckDB DataChunk 指针
pub unsafe fn vector_to_column(chunk: sys::duckdb_data_chunk, col_idx: u64) -> Option<Column> {
    let size = ext_api::duckdb_data_chunk_get_size(chunk) as usize;
    if size == 0 {
        return None;
    }

    let vector = ext_api::duckdb_data_chunk_get_vector(chunk, col_idx);
    let logical_type = ext_api::duckdb_vector_get_column_type(vector);
    let type_id = ext_api::duckdb_get_type_id(logical_type);
    ext_api::duckdb_destroy_logical_type(&mut { logical_type });

    let data_type = from_duckdb_type(type_id)?;
    let byte_size = data_type.byte_size();

    let data_ptr = ext_api::duckdb_vector_get_data(vector) as *const u8;
    let data = std::slice::from_raw_parts(data_ptr, size * byte_size).to_vec();

    let validity_ptr = ext_api::duckdb_vector_get_validity(vector);
    let validity = if validity_ptr.is_null() {
        let words = size.div_ceil(64);
        vec![u64::MAX; words]
    } else {
        let words = size.div_ceil(64);
        std::slice::from_raw_parts(validity_ptr, words).to_vec()
    };

    Some(Column::from_raw_with_validity(data_type, data, validity, size))
}

/// 将 i64 结果写入 DuckDB vector 指定行
///
/// # Safety
/// `result` 和 idx 必须有效
pub unsafe fn write_i64_to_vector(result: sys::duckdb_vector, idx: u64, value: i64) {
    let data_ptr = ext_api::duckdb_vector_get_data(result) as *mut i64;
    *data_ptr.add(idx as usize) = value;
}

/// 将 f64 结果写入 DuckDB vector 指定行
///
/// # Safety
/// `result` 和 idx 必须有效
pub unsafe fn write_f64_to_vector(result: sys::duckdb_vector, idx: u64, value: f64) {
    let data_ptr = ext_api::duckdb_vector_get_data(result) as *mut f64;
    *data_ptr.add(idx as usize) = value;
}

/// 将 vector 指定行设为 NULL
///
/// # Safety
/// `result` 和 idx 必须有效
pub unsafe fn set_vector_null(result: sys::duckdb_vector, idx: u64) {
    ext_api::duckdb_vector_ensure_validity_writable(result);
    let validity = ext_api::duckdb_vector_get_validity(result);
    ext_api::duckdb_validity_set_row_validity(validity, idx, false);
}
