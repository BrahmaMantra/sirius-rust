use std::fmt;

/// Sirius 内部数据类型，与 DuckDB LogicalType 对应
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    /// 用于 COUNT 等返回整数的聚合
    BigInt,
}

impl DataType {
    /// 该类型单个值占用的字节数
    pub fn byte_size(&self) -> usize {
        match self {
            DataType::Boolean => 1,
            DataType::Int8 | DataType::UInt8 => 1,
            DataType::Int16 | DataType::UInt16 => 2,
            DataType::Int32 | DataType::UInt32 | DataType::Float32 => 4,
            DataType::Int64 | DataType::UInt64 | DataType::Float64 | DataType::BigInt => 8,
        }
    }

    /// 是否为整数类型
    pub fn is_integer(&self) -> bool {
        matches!(
            self,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::BigInt
        )
    }

    /// 是否为浮点类型
    pub fn is_float(&self) -> bool {
        matches!(self, DataType::Float32 | DataType::Float64)
    }

    /// 是否为数值类型（可用于 SUM）
    pub fn is_numeric(&self) -> bool {
        self.is_integer() || self.is_float()
    }

    /// SUM 操作的提升类型：整数提升到 Int64，浮点提升到 Float64
    pub fn sum_result_type(&self) -> Option<DataType> {
        match self {
            dt if dt.is_integer() => Some(DataType::BigInt),
            DataType::Float32 | DataType::Float64 => Some(DataType::Float64),
            _ => None,
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Boolean => write!(f, "BOOLEAN"),
            DataType::Int8 => write!(f, "TINYINT"),
            DataType::Int16 => write!(f, "SMALLINT"),
            DataType::Int32 => write!(f, "INTEGER"),
            DataType::Int64 => write!(f, "BIGINT"),
            DataType::UInt8 => write!(f, "UTINYINT"),
            DataType::UInt16 => write!(f, "USMALLINT"),
            DataType::UInt32 => write!(f, "UINTEGER"),
            DataType::UInt64 => write!(f, "UBIGINT"),
            DataType::Float32 => write!(f, "FLOAT"),
            DataType::Float64 => write!(f, "DOUBLE"),
            DataType::BigInt => write!(f, "BIGINT"),
        }
    }
}
