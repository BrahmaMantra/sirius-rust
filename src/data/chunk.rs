use crate::data::column::Column;

/// DataChunk — 一批列式数据，与 DuckDB DataChunk 概念对应
///
/// 包含多列，每列行数相同。是算子之间传递数据的基本单位。
#[derive(Debug, Clone)]
pub struct DataChunk {
    columns: Vec<Column>,
}

impl DataChunk {
    pub fn new(columns: Vec<Column>) -> Self {
        debug_assert!(
            columns.is_empty() || columns.windows(2).all(|w| w[0].len() == w[1].len()),
            "All columns must have the same length"
        );
        Self { columns }
    }

    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
        }
    }

    /// 行数
    pub fn len(&self) -> usize {
        self.columns.first().map_or(0, |c| c.len())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 列数
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn column(&self, idx: usize) -> &Column {
        &self.columns[idx]
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_chunk() {
        let col1 = Column::from_i32(vec![1, 2, 3]);
        let col2 = Column::from_f64(vec![1.1, 2.2, 3.3]);
        let chunk = DataChunk::new(vec![col1, col2]);
        assert_eq!(chunk.len(), 3);
        assert_eq!(chunk.column_count(), 2);
    }

    #[test]
    fn test_empty_chunk() {
        let chunk = DataChunk::empty();
        assert!(chunk.is_empty());
        assert_eq!(chunk.column_count(), 0);
    }
}
