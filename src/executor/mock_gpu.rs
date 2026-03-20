//! 模拟 GPU 聚合执行器 — 用 CPU 模拟 GPU block 归约行为
//!
//! 用于无 GPU 环境的测试，验证 block reduction 算法的正确性。

use crate::data::column::Column;
use crate::data::types::DataType;
use crate::error::{Result, SiriusError};
use crate::op::aggregate::traits::AggregateValue;

use super::AggregateExecutor;

pub struct MockGpuExecutor {
    block_size: usize,
    vram_bytes: usize,
}

impl Default for MockGpuExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl MockGpuExecutor {
    pub fn new() -> Self {
        Self {
            block_size: 256,
            vram_bytes: 4 * 1024 * 1024 * 1024, // 4GB
        }
    }

    pub fn with_vram(vram_bytes: usize) -> Self {
        Self {
            block_size: 256,
            vram_bytes,
        }
    }

    pub fn with_block_size(block_size: usize) -> Self {
        Self {
            block_size,
            vram_bytes: 4 * 1024 * 1024 * 1024,
        }
    }

    pub fn compute_batch_size(&self, element_size: usize, total_elements: usize) -> usize {
        let usable = self.vram_bytes * 3 / 4;
        let max_elements = usable / element_size;
        max_elements.min(total_elements)
    }

    fn block_reduce_sum_i64(&self, values: &[i64], validity: &[u64]) -> i64 {
        let mut total = 0i64;
        let elements_per_block = self.block_size * 2;

        for block_start in (0..values.len()).step_by(elements_per_block) {
            let block_end = (block_start + elements_per_block).min(values.len());
            let mut shared = vec![0i64; self.block_size];

            for tid in 0..self.block_size {
                let i = block_start + tid;
                let j = i + self.block_size;
                let mut sum = 0i64;
                if i < block_end && !is_null(validity, i) {
                    sum += values[i];
                }
                if j < block_end && !is_null(validity, j) {
                    sum += values[j];
                }
                shared[tid] = sum;
            }

            let mut s = self.block_size / 2;
            while s > 0 {
                for tid in 0..s {
                    shared[tid] += shared[tid + s];
                }
                s /= 2;
            }
            total += shared[0];
        }
        total
    }

    fn block_reduce_sum_f64(&self, values: &[f64], validity: &[u64]) -> f64 {
        let mut total = 0f64;
        let elements_per_block = self.block_size * 2;

        for block_start in (0..values.len()).step_by(elements_per_block) {
            let block_end = (block_start + elements_per_block).min(values.len());
            let mut shared = vec![0f64; self.block_size];

            for tid in 0..self.block_size {
                let i = block_start + tid;
                let j = i + self.block_size;
                let mut sum = 0f64;
                if i < block_end && !is_null(validity, i) {
                    sum += values[i];
                }
                if j < block_end && !is_null(validity, j) {
                    sum += values[j];
                }
                shared[tid] = sum;
            }

            let mut s = self.block_size / 2;
            while s > 0 {
                for tid in 0..s {
                    shared[tid] += shared[tid + s];
                }
                s /= 2;
            }
            total += shared[0];
        }
        total
    }

    fn block_reduce_min_i64(&self, values: &[i64], validity: &[u64]) -> Option<i64> {
        let mut global_min: Option<i64> = None;
        let elements_per_block = self.block_size * 2;

        for block_start in (0..values.len()).step_by(elements_per_block) {
            let block_end = (block_start + elements_per_block).min(values.len());
            let mut shared = vec![i64::MAX; self.block_size];

            for tid in 0..self.block_size {
                let i = block_start + tid;
                let j = i + self.block_size;
                let mut val = i64::MAX;
                if i < block_end && !is_null(validity, i) {
                    val = values[i];
                }
                if j < block_end && !is_null(validity, j) && values[j] < val {
                    val = values[j];
                }
                shared[tid] = val;
            }

            let mut s = self.block_size / 2;
            while s > 0 {
                for tid in 0..s {
                    if shared[tid + s] < shared[tid] {
                        shared[tid] = shared[tid + s];
                    }
                }
                s /= 2;
            }

            if shared[0] != i64::MAX {
                global_min = Some(match global_min {
                    Some(cur) if cur < shared[0] => cur,
                    _ => shared[0],
                });
            }
        }
        global_min
    }

    fn block_reduce_min_f64(&self, values: &[f64], validity: &[u64]) -> Option<f64> {
        let mut global_min: Option<f64> = None;
        let elements_per_block = self.block_size * 2;

        for block_start in (0..values.len()).step_by(elements_per_block) {
            let block_end = (block_start + elements_per_block).min(values.len());
            let mut shared = vec![f64::INFINITY; self.block_size];

            for tid in 0..self.block_size {
                let i = block_start + tid;
                let j = i + self.block_size;
                let mut val = f64::INFINITY;
                if i < block_end && !is_null(validity, i) {
                    val = values[i];
                }
                if j < block_end && !is_null(validity, j) && values[j] < val {
                    val = values[j];
                }
                shared[tid] = val;
            }

            let mut s = self.block_size / 2;
            while s > 0 {
                for tid in 0..s {
                    if shared[tid + s] < shared[tid] {
                        shared[tid] = shared[tid + s];
                    }
                }
                s /= 2;
            }

            if shared[0] != f64::INFINITY {
                global_min = Some(match global_min {
                    Some(cur) if cur < shared[0] => cur,
                    _ => shared[0],
                });
            }
        }
        global_min
    }

    fn block_reduce_max_i64(&self, values: &[i64], validity: &[u64]) -> Option<i64> {
        let mut global_max: Option<i64> = None;
        let elements_per_block = self.block_size * 2;

        for block_start in (0..values.len()).step_by(elements_per_block) {
            let block_end = (block_start + elements_per_block).min(values.len());
            let mut shared = vec![i64::MIN; self.block_size];

            for tid in 0..self.block_size {
                let i = block_start + tid;
                let j = i + self.block_size;
                let mut val = i64::MIN;
                if i < block_end && !is_null(validity, i) {
                    val = values[i];
                }
                if j < block_end && !is_null(validity, j) && values[j] > val {
                    val = values[j];
                }
                shared[tid] = val;
            }

            let mut s = self.block_size / 2;
            while s > 0 {
                for tid in 0..s {
                    if shared[tid + s] > shared[tid] {
                        shared[tid] = shared[tid + s];
                    }
                }
                s /= 2;
            }

            if shared[0] != i64::MIN {
                global_max = Some(match global_max {
                    Some(cur) if cur > shared[0] => cur,
                    _ => shared[0],
                });
            }
        }
        global_max
    }

    fn block_reduce_max_f64(&self, values: &[f64], validity: &[u64]) -> Option<f64> {
        let mut global_max: Option<f64> = None;
        let elements_per_block = self.block_size * 2;

        for block_start in (0..values.len()).step_by(elements_per_block) {
            let block_end = (block_start + elements_per_block).min(values.len());
            let mut shared = vec![f64::NEG_INFINITY; self.block_size];

            for tid in 0..self.block_size {
                let i = block_start + tid;
                let j = i + self.block_size;
                let mut val = f64::NEG_INFINITY;
                if i < block_end && !is_null(validity, i) {
                    val = values[i];
                }
                if j < block_end && !is_null(validity, j) && values[j] > val {
                    val = values[j];
                }
                shared[tid] = val;
            }

            let mut s = self.block_size / 2;
            while s > 0 {
                for tid in 0..s {
                    if shared[tid + s] > shared[tid] {
                        shared[tid] = shared[tid + s];
                    }
                }
                s /= 2;
            }

            if shared[0] != f64::NEG_INFINITY {
                global_max = Some(match global_max {
                    Some(cur) if cur > shared[0] => cur,
                    _ => shared[0],
                });
            }
        }
        global_max
    }

    fn block_reduce_count(&self, len: usize, validity: &[u64]) -> u64 {
        let mut total = 0u64;
        let elements_per_block = self.block_size * 2;

        for block_start in (0..len).step_by(elements_per_block) {
            let block_end = (block_start + elements_per_block).min(len);
            let mut shared = vec![0u64; self.block_size];

            for tid in 0..self.block_size {
                let i = block_start + tid;
                let j = i + self.block_size;
                let mut count = 0u64;
                if i < block_end && !is_null(validity, i) {
                    count += 1;
                }
                if j < block_end && !is_null(validity, j) {
                    count += 1;
                }
                shared[tid] = count;
            }

            let mut s = self.block_size / 2;
            while s > 0 {
                for tid in 0..s {
                    shared[tid] += shared[tid + s];
                }
                s /= 2;
            }
            total += shared[0];
        }
        total
    }
}

impl AggregateExecutor for MockGpuExecutor {
    fn name(&self) -> &str {
        "MockGPU"
    }

    fn sum(&self, input: &Column) -> Result<AggregateValue> {
        if input.is_empty() {
            return Ok(AggregateValue::Null);
        }

        let validity = input.validity();
        let has_any_valid = (0..input.len()).any(|i| !is_null(validity, i));
        if !has_any_valid {
            return Ok(AggregateValue::Null);
        }

        match input.data_type() {
            DataType::Int32 => {
                let values: Vec<i64> = input.as_i32_slice().iter().map(|&v| v as i64).collect();
                Ok(AggregateValue::Int64(
                    self.block_reduce_sum_i64(&values, validity),
                ))
            }
            DataType::Int64 | DataType::BigInt => Ok(AggregateValue::Int64(
                self.block_reduce_sum_i64(input.as_i64_slice(), validity),
            )),
            DataType::Float32 => {
                let values: Vec<f64> = input.as_f32_slice().iter().map(|&v| v as f64).collect();
                Ok(AggregateValue::Float64(
                    self.block_reduce_sum_f64(&values, validity),
                ))
            }
            DataType::Float64 => Ok(AggregateValue::Float64(
                self.block_reduce_sum_f64(input.as_f64_slice(), validity),
            )),
            other => Err(SiriusError::TypeError(format!(
                "GPU SUM does not support type {other}"
            ))),
        }
    }

    fn count(&self, input: &Column) -> Result<AggregateValue> {
        if input.is_empty() {
            return Ok(AggregateValue::Int64(0));
        }
        Ok(AggregateValue::Int64(
            self.block_reduce_count(input.len(), input.validity()) as i64,
        ))
    }

    fn count_star(&self, input: &Column) -> Result<AggregateValue> {
        Ok(AggregateValue::Int64(input.len() as i64))
    }

    fn avg(&self, input: &Column) -> Result<AggregateValue> {
        if input.is_empty() {
            return Ok(AggregateValue::Null);
        }

        let validity = input.validity();
        let count = self.block_reduce_count(input.len(), validity);
        if count == 0 {
            return Ok(AggregateValue::Null);
        }

        let sum = match input.data_type() {
            DataType::Int32 => {
                let values: Vec<i64> = input.as_i32_slice().iter().map(|&v| v as i64).collect();
                self.block_reduce_sum_i64(&values, validity) as f64
            }
            DataType::Int64 | DataType::BigInt => {
                self.block_reduce_sum_i64(input.as_i64_slice(), validity) as f64
            }
            DataType::Float32 => {
                let values: Vec<f64> = input.as_f32_slice().iter().map(|&v| v as f64).collect();
                self.block_reduce_sum_f64(&values, validity)
            }
            DataType::Float64 => {
                self.block_reduce_sum_f64(input.as_f64_slice(), validity)
            }
            other => {
                return Err(SiriusError::TypeError(format!(
                    "GPU AVG does not support type {other}"
                )));
            }
        };

        Ok(AggregateValue::Float64(sum / count as f64))
    }

    fn min(&self, input: &Column) -> Result<AggregateValue> {
        if input.is_empty() {
            return Ok(AggregateValue::Null);
        }

        let validity = input.validity();

        match input.data_type() {
            DataType::Int32 => {
                let values: Vec<i64> = input.as_i32_slice().iter().map(|&v| v as i64).collect();
                match self.block_reduce_min_i64(&values, validity) {
                    Some(v) => Ok(AggregateValue::Int64(v)),
                    None => Ok(AggregateValue::Null),
                }
            }
            DataType::Int64 | DataType::BigInt => {
                match self.block_reduce_min_i64(input.as_i64_slice(), validity) {
                    Some(v) => Ok(AggregateValue::Int64(v)),
                    None => Ok(AggregateValue::Null),
                }
            }
            DataType::Float32 => {
                let values: Vec<f64> = input.as_f32_slice().iter().map(|&v| v as f64).collect();
                match self.block_reduce_min_f64(&values, validity) {
                    Some(v) => Ok(AggregateValue::Float64(v)),
                    None => Ok(AggregateValue::Null),
                }
            }
            DataType::Float64 => {
                match self.block_reduce_min_f64(input.as_f64_slice(), validity) {
                    Some(v) => Ok(AggregateValue::Float64(v)),
                    None => Ok(AggregateValue::Null),
                }
            }
            other => Err(SiriusError::TypeError(format!(
                "GPU MIN does not support type {other}"
            ))),
        }
    }

    fn max(&self, input: &Column) -> Result<AggregateValue> {
        if input.is_empty() {
            return Ok(AggregateValue::Null);
        }

        let validity = input.validity();

        match input.data_type() {
            DataType::Int32 => {
                let values: Vec<i64> = input.as_i32_slice().iter().map(|&v| v as i64).collect();
                match self.block_reduce_max_i64(&values, validity) {
                    Some(v) => Ok(AggregateValue::Int64(v)),
                    None => Ok(AggregateValue::Null),
                }
            }
            DataType::Int64 | DataType::BigInt => {
                match self.block_reduce_max_i64(input.as_i64_slice(), validity) {
                    Some(v) => Ok(AggregateValue::Int64(v)),
                    None => Ok(AggregateValue::Null),
                }
            }
            DataType::Float32 => {
                let values: Vec<f64> = input.as_f32_slice().iter().map(|&v| v as f64).collect();
                match self.block_reduce_max_f64(&values, validity) {
                    Some(v) => Ok(AggregateValue::Float64(v)),
                    None => Ok(AggregateValue::Null),
                }
            }
            DataType::Float64 => {
                match self.block_reduce_max_f64(input.as_f64_slice(), validity) {
                    Some(v) => Ok(AggregateValue::Float64(v)),
                    None => Ok(AggregateValue::Null),
                }
            }
            other => Err(SiriusError::TypeError(format!(
                "GPU MAX does not support type {other}"
            ))),
        }
    }
}

fn is_null(validity: &[u64], idx: usize) -> bool {
    let word = idx / 64;
    let bit = idx % 64;
    if word >= validity.len() {
        return true;
    }
    (validity[word] >> bit) & 1 == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::column::Column;
    use crate::data::types::DataType;

    #[test]
    fn test_mock_gpu_sum_i32() {
        let exec = MockGpuExecutor::new();
        let col = Column::from_i32(vec![1, 2, 3, 4, 5]);
        assert_eq!(exec.sum(&col).unwrap().as_i64(), Some(15));
    }

    #[test]
    fn test_mock_gpu_sum_large() {
        let exec = MockGpuExecutor::new();
        let data: Vec<i32> = (1..=10_000).collect();
        let expected: i64 = (1..=10_000i64).sum();
        let col = Column::from_i32(data);
        assert_eq!(exec.sum(&col).unwrap().as_i64(), Some(expected));
    }

    #[test]
    fn test_mock_gpu_sum_with_nulls() {
        let exec = MockGpuExecutor::new();
        let data: Vec<u8> = [10i32, 0i32, 30i32]
            .iter()
            .flat_map(|v| v.to_ne_bytes())
            .collect();
        let col = Column::from_raw_with_validity(DataType::Int32, data, vec![0b101], 3);
        assert_eq!(exec.sum(&col).unwrap().as_i64(), Some(40));
    }

    #[test]
    fn test_mock_gpu_sum_all_null() {
        let exec = MockGpuExecutor::new();
        let data = vec![0u8; 4 * 3];
        let col = Column::from_raw_with_validity(DataType::Int32, data, vec![0u64], 3);
        assert!(exec.sum(&col).unwrap().is_null());
    }

    #[test]
    fn test_mock_gpu_sum_f64() {
        let exec = MockGpuExecutor::new();
        let col = Column::from_f64(vec![1.5, 2.5, 3.0]);
        assert!((exec.sum(&col).unwrap().as_f64().unwrap() - 7.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_mock_gpu_count() {
        let exec = MockGpuExecutor::new();
        let col = Column::from_i32(vec![1, 2, 3, 4, 5]);
        assert_eq!(exec.count(&col).unwrap().as_i64(), Some(5));
    }

    #[test]
    fn test_mock_gpu_count_with_nulls() {
        let exec = MockGpuExecutor::new();
        let data = vec![0u8; 4 * 5];
        let col = Column::from_raw_with_validity(DataType::Int32, data, vec![0b10101u64], 5);
        assert_eq!(exec.count(&col).unwrap().as_i64(), Some(3));
    }

    #[test]
    fn test_mock_gpu_count_star() {
        let exec = MockGpuExecutor::new();
        let data = vec![0u8; 4 * 5];
        let col = Column::from_raw_with_validity(DataType::Int32, data, vec![0b10101u64], 5);
        assert_eq!(exec.count_star(&col).unwrap().as_i64(), Some(5));
    }

    #[test]
    fn test_mock_gpu_avg_i32() {
        let exec = MockGpuExecutor::new();
        let col = Column::from_i32(vec![2, 4, 6]);
        assert!((exec.avg(&col).unwrap().as_f64().unwrap() - 4.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_mock_gpu_avg_with_nulls() {
        let exec = MockGpuExecutor::new();
        let data: Vec<u8> = [10i32, 0i32, 20i32]
            .iter()
            .flat_map(|v| v.to_ne_bytes())
            .collect();
        let col = Column::from_raw_with_validity(DataType::Int32, data, vec![0b101], 3);
        assert!((exec.avg(&col).unwrap().as_f64().unwrap() - 15.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_mock_gpu_avg_all_null() {
        let exec = MockGpuExecutor::new();
        let col = Column::from_raw_with_validity(DataType::Int32, vec![0u8; 12], vec![0u64], 3);
        assert!(exec.avg(&col).unwrap().is_null());
    }

    #[test]
    fn test_mock_gpu_min_i32() {
        let exec = MockGpuExecutor::new();
        let col = Column::from_i32(vec![5, 1, 8, 3, 9]);
        assert_eq!(exec.min(&col).unwrap().as_i64(), Some(1));
    }

    #[test]
    fn test_mock_gpu_min_with_nulls() {
        let exec = MockGpuExecutor::new();
        let data: Vec<u8> = [100i32, 0i32, 5i32]
            .iter()
            .flat_map(|v| v.to_ne_bytes())
            .collect();
        let col = Column::from_raw_with_validity(DataType::Int32, data, vec![0b101], 3);
        assert_eq!(exec.min(&col).unwrap().as_i64(), Some(5));
    }

    #[test]
    fn test_mock_gpu_min_all_null() {
        let exec = MockGpuExecutor::new();
        let col = Column::from_raw_with_validity(DataType::Int32, vec![0u8; 12], vec![0u64], 3);
        assert!(exec.min(&col).unwrap().is_null());
    }

    #[test]
    fn test_mock_gpu_max_i32() {
        let exec = MockGpuExecutor::new();
        let col = Column::from_i32(vec![5, 1, 8, 3, 9]);
        assert_eq!(exec.max(&col).unwrap().as_i64(), Some(9));
    }

    #[test]
    fn test_mock_gpu_max_with_nulls() {
        let exec = MockGpuExecutor::new();
        let data: Vec<u8> = [5i32, 0i32, 100i32]
            .iter()
            .flat_map(|v| v.to_ne_bytes())
            .collect();
        let col = Column::from_raw_with_validity(DataType::Int32, data, vec![0b101], 3);
        assert_eq!(exec.max(&col).unwrap().as_i64(), Some(100));
    }

    #[test]
    fn test_mock_gpu_max_all_null() {
        let exec = MockGpuExecutor::new();
        let col = Column::from_raw_with_validity(DataType::Int32, vec![0u8; 12], vec![0u64], 3);
        assert!(exec.max(&col).unwrap().is_null());
    }

    #[test]
    fn test_mock_gpu_min_f64() {
        let exec = MockGpuExecutor::new();
        let col = Column::from_f64(vec![3.5, 1.2, 7.8]);
        assert!((exec.min(&col).unwrap().as_f64().unwrap() - 1.2).abs() < f64::EPSILON);
    }

    #[test]
    fn test_mock_gpu_max_f64() {
        let exec = MockGpuExecutor::new();
        let col = Column::from_f64(vec![3.5, 1.2, 7.8]);
        assert!((exec.max(&col).unwrap().as_f64().unwrap() - 7.8).abs() < f64::EPSILON);
    }

    #[test]
    fn test_batch_size_fits() {
        let exec = MockGpuExecutor::new();
        assert_eq!(exec.compute_batch_size(8, 1_000_000), 1_000_000);
    }

    #[test]
    fn test_batch_size_exceeds() {
        let exec = MockGpuExecutor::with_vram(1024);
        let batch = exec.compute_batch_size(8, 1_000_000);
        assert!(batch < 1_000_000);
        assert_eq!(batch, 1024 * 3 / 4 / 8);
    }

    #[test]
    fn test_mock_gpu_sum_across_block_boundary() {
        let exec = MockGpuExecutor::with_block_size(4);
        let data: Vec<i32> = (1..=100).collect();
        let expected: i64 = (1..=100i64).sum();
        let col = Column::from_i32(data);
        assert_eq!(exec.sum(&col).unwrap().as_i64(), Some(expected));
    }
}
