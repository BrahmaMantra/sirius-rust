use crate::data::types::DataType;

/// 列式数据存储，核心数据结构
///
/// 数据以类型擦除的字节数组存储，配合 validity bitmap 标记 NULL。
/// 设计上与 DuckDB Vector 和 GPU 内存传输对齐。
#[derive(Debug, Clone)]
pub struct Column {
    /// 数据类型
    data_type: DataType,
    /// 原始字节数据（按 data_type.byte_size() 对齐）
    data: Vec<u8>,
    /// NULL 位图：bit=1 表示有效，bit=0 表示 NULL
    validity: Vec<u64>,
    /// 行数
    len: usize,
}

impl Column {
    /// 创建空列
    pub fn new(data_type: DataType) -> Self {
        Self {
            data_type,
            data: Vec::new(),
            validity: Vec::new(),
            len: 0,
        }
    }

    /// 从已有数据创建列（无 NULL）
    pub fn from_raw(data_type: DataType, data: Vec<u8>, len: usize) -> Self {
        let validity_words = len.div_ceil(64);
        let validity = vec![u64::MAX; validity_words]; // 全部有效
        Self {
            data_type,
            data,
            validity,
            len,
        }
    }

    /// 从已有数据和 validity 创建列
    pub fn from_raw_with_validity(
        data_type: DataType,
        data: Vec<u8>,
        validity: Vec<u64>,
        len: usize,
    ) -> Self {
        Self {
            data_type,
            data,
            validity,
            len,
        }
    }

    pub fn data_type(&self) -> DataType {
        self.data_type
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// 获取原始字节数据的引用
    pub fn raw_data(&self) -> &[u8] {
        &self.data
    }

    /// 获取 validity bitmap 的引用
    pub fn validity(&self) -> &[u64] {
        &self.validity
    }

    /// 检查第 idx 行是否为 NULL
    pub fn is_null(&self, idx: usize) -> bool {
        if idx >= self.len {
            return true;
        }
        let word = idx / 64;
        let bit = idx % 64;
        if word >= self.validity.len() {
            return true;
        }
        (self.validity[word] >> bit) & 1 == 0
    }

    /// 获取第 idx 行的类型化数据切片
    pub fn value_bytes(&self, idx: usize) -> &[u8] {
        let size = self.data_type.byte_size();
        let start = idx * size;
        &self.data[start..start + size]
    }

    /// 将列数据解释为 i32 切片
    pub fn as_i32_slice(&self) -> &[i32] {
        debug_assert_eq!(self.data_type.byte_size(), 4);
        unsafe {
            std::slice::from_raw_parts(self.data.as_ptr() as *const i32, self.len)
        }
    }

    /// 将列数据解释为 i64 切片
    pub fn as_i64_slice(&self) -> &[i64] {
        debug_assert_eq!(self.data_type.byte_size(), 8);
        unsafe {
            std::slice::from_raw_parts(self.data.as_ptr() as *const i64, self.len)
        }
    }

    /// 将列数据解释为 f32 切片
    pub fn as_f32_slice(&self) -> &[f32] {
        debug_assert_eq!(self.data_type.byte_size(), 4);
        unsafe {
            std::slice::from_raw_parts(self.data.as_ptr() as *const f32, self.len)
        }
    }

    /// 将列数据解释为 f64 切片
    pub fn as_f64_slice(&self) -> &[f64] {
        debug_assert_eq!(self.data_type.byte_size(), 8);
        unsafe {
            std::slice::from_raw_parts(self.data.as_ptr() as *const f64, self.len)
        }
    }

    /// 非 NULL 值的数量
    pub fn valid_count(&self) -> usize {
        let mut count = 0usize;
        for i in 0..self.len {
            if !self.is_null(i) {
                count += 1;
            }
        }
        count
    }
}

/// 从具体类型的 Vec 创建 Column 的便利方法
impl Column {
    pub fn from_i32(values: Vec<i32>) -> Self {
        let len = values.len();
        let data = unsafe {
            let ptr = values.as_ptr() as *const u8;
            std::slice::from_raw_parts(ptr, len * 4).to_vec()
        };
        Self::from_raw(DataType::Int32, data, len)
    }

    pub fn from_i64(values: Vec<i64>) -> Self {
        let len = values.len();
        let data = unsafe {
            let ptr = values.as_ptr() as *const u8;
            std::slice::from_raw_parts(ptr, len * 8).to_vec()
        };
        Self::from_raw(DataType::Int64, data, len)
    }

    pub fn from_f32(values: Vec<f32>) -> Self {
        let len = values.len();
        let data = unsafe {
            let ptr = values.as_ptr() as *const u8;
            std::slice::from_raw_parts(ptr, len * 4).to_vec()
        };
        Self::from_raw(DataType::Float32, data, len)
    }

    pub fn from_f64(values: Vec<f64>) -> Self {
        let len = values.len();
        let data = unsafe {
            let ptr = values.as_ptr() as *const u8;
            std::slice::from_raw_parts(ptr, len * 8).to_vec()
        };
        Self::from_raw(DataType::Float64, data, len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_i32() {
        let col = Column::from_i32(vec![1, 2, 3, 4, 5]);
        assert_eq!(col.len(), 5);
        assert_eq!(col.data_type(), DataType::Int32);
        assert_eq!(col.as_i32_slice(), &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_from_f64() {
        let col = Column::from_f64(vec![1.5, 2.5, 3.5]);
        assert_eq!(col.len(), 3);
        assert_eq!(col.as_f64_slice(), &[1.5, 2.5, 3.5]);
    }

    #[test]
    fn test_null_handling() {
        let data = vec![0u8; 4 * 3]; // 3 个 i32
        let validity = vec![0b101u64]; // 第 0 和第 2 个有效，第 1 个 NULL
        let col = Column::from_raw_with_validity(DataType::Int32, data, validity, 3);
        assert!(!col.is_null(0));
        assert!(col.is_null(1));
        assert!(!col.is_null(2));
        assert_eq!(col.valid_count(), 2);
    }

    #[test]
    fn test_empty_column() {
        let col = Column::new(DataType::Int64);
        assert!(col.is_empty());
        assert_eq!(col.len(), 0);
    }
}
