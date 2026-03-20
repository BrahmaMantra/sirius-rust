//! GPU 后端 trait + MockGpuBackend — 对应 sirius/src/cuda/ 层的 Rust 抽象

use crate::data::column::Column;
use crate::error::Result;
use crate::op::aggregate::traits::AggregateValue;

/// GPU 后端 trait — 统一真实 CUDA 和模拟后端的接口
pub trait GpuBackend: Send + Sync {
    fn name(&self) -> &str;
    fn is_real_gpu(&self) -> bool;
    fn gpu_sum(&self, input: &Column) -> Result<AggregateValue>;
    fn gpu_count(&self, input: &Column) -> Result<AggregateValue>;
    fn gpu_count_star(&self, input: &Column) -> Result<AggregateValue>;
    fn available_memory(&self) -> Result<(usize, usize)>;

    fn compute_batch_size(&self, element_size: usize, total_elements: usize) -> Result<usize> {
        let (free, _total) = self.available_memory()?;
        let usable = free * 3 / 4;
        let max_elements = usable / element_size;
        if max_elements >= total_elements {
            Ok(total_elements)
        } else {
            Ok(max_elements)
        }
    }
}

/// 模拟 GPU 后端 — 用 CPU 模拟 GPU 的 block 归约行为
pub struct MockGpuBackend {
    block_size: usize,
    vram_bytes: usize,
}

impl Default for MockGpuBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl MockGpuBackend {
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

impl GpuBackend for MockGpuBackend {
    fn name(&self) -> &str {
        "MockGPU"
    }

    fn is_real_gpu(&self) -> bool {
        false
    }

    fn gpu_sum(&self, input: &Column) -> Result<AggregateValue> {
        use crate::data::types::DataType;

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
                Ok(AggregateValue::Int64(self.block_reduce_sum_i64(&values, validity)))
            }
            DataType::Int64 | DataType::BigInt => {
                Ok(AggregateValue::Int64(self.block_reduce_sum_i64(input.as_i64_slice(), validity)))
            }
            DataType::Float32 => {
                let values: Vec<f64> = input.as_f32_slice().iter().map(|&v| v as f64).collect();
                Ok(AggregateValue::Float64(self.block_reduce_sum_f64(&values, validity)))
            }
            DataType::Float64 => {
                Ok(AggregateValue::Float64(self.block_reduce_sum_f64(input.as_f64_slice(), validity)))
            }
            other => Err(crate::error::SiriusError::TypeError(format!(
                "GPU SUM does not support type {other}"
            ))),
        }
    }

    fn gpu_count(&self, input: &Column) -> Result<AggregateValue> {
        if input.is_empty() {
            return Ok(AggregateValue::Int64(0));
        }
        Ok(AggregateValue::Int64(self.block_reduce_count(input.len(), input.validity()) as i64))
    }

    fn gpu_count_star(&self, input: &Column) -> Result<AggregateValue> {
        Ok(AggregateValue::Int64(input.len() as i64))
    }

    fn available_memory(&self) -> Result<(usize, usize)> {
        Ok((self.vram_bytes, self.vram_bytes))
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
    fn test_simulated_sum_i32() {
        let backend = MockGpuBackend::new();
        let col = Column::from_i32(vec![1, 2, 3, 4, 5]);
        let result = backend.gpu_sum(&col).unwrap();
        assert_eq!(result.as_i64(), Some(15));
    }

    #[test]
    fn test_simulated_sum_large() {
        let backend = MockGpuBackend::new();
        let data: Vec<i32> = (1..=10_000).collect();
        let expected: i64 = (1..=10_000i64).sum();
        let col = Column::from_i32(data);
        let result = backend.gpu_sum(&col).unwrap();
        assert_eq!(result.as_i64(), Some(expected));
    }

    #[test]
    fn test_simulated_sum_with_nulls() {
        let backend = MockGpuBackend::new();
        let data: Vec<u8> = [10i32, 0i32, 30i32]
            .iter()
            .flat_map(|v| v.to_ne_bytes())
            .collect();
        let col = Column::from_raw_with_validity(DataType::Int32, data, vec![0b101], 3);
        let result = backend.gpu_sum(&col).unwrap();
        assert_eq!(result.as_i64(), Some(40));
    }

    #[test]
    fn test_simulated_sum_all_null() {
        let backend = MockGpuBackend::new();
        let data = vec![0u8; 4 * 3];
        let col = Column::from_raw_with_validity(DataType::Int32, data, vec![0u64], 3);
        let result = backend.gpu_sum(&col).unwrap();
        assert!(result.is_null());
    }

    #[test]
    fn test_simulated_sum_f64() {
        let backend = MockGpuBackend::new();
        let col = Column::from_f64(vec![1.5, 2.5, 3.0]);
        let result = backend.gpu_sum(&col).unwrap();
        assert!((result.as_f64().unwrap() - 7.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_simulated_count() {
        let backend = MockGpuBackend::new();
        let col = Column::from_i32(vec![1, 2, 3, 4, 5]);
        let result = backend.gpu_count(&col).unwrap();
        assert_eq!(result.as_i64(), Some(5));
    }

    #[test]
    fn test_simulated_count_with_nulls() {
        let backend = MockGpuBackend::new();
        let data = vec![0u8; 4 * 5];
        let col = Column::from_raw_with_validity(DataType::Int32, data, vec![0b10101u64], 5);
        let result = backend.gpu_count(&col).unwrap();
        assert_eq!(result.as_i64(), Some(3));
    }

    #[test]
    fn test_simulated_count_star() {
        let backend = MockGpuBackend::new();
        let data = vec![0u8; 4 * 5];
        let col = Column::from_raw_with_validity(DataType::Int32, data, vec![0b10101u64], 5);
        let result = backend.gpu_count_star(&col).unwrap();
        assert_eq!(result.as_i64(), Some(5));
    }

    #[test]
    fn test_batch_size_fits_in_vram() {
        let backend = MockGpuBackend::new();
        let batch = backend.compute_batch_size(8, 1_000_000).unwrap();
        assert_eq!(batch, 1_000_000);
    }

    #[test]
    fn test_batch_size_exceeds_vram() {
        let backend = MockGpuBackend::with_vram(1024);
        let batch = backend.compute_batch_size(8, 1_000_000).unwrap();
        assert!(batch < 1_000_000);
        assert_eq!(batch, 1024 * 3 / 4 / 8);
    }

    #[test]
    fn test_simulated_sum_across_block_boundary() {
        let backend = MockGpuBackend::with_block_size(4);
        let data: Vec<i32> = (1..=100).collect();
        let expected: i64 = (1..=100i64).sum();
        let col = Column::from_i32(data);
        let result = backend.gpu_sum(&col).unwrap();
        assert_eq!(result.as_i64(), Some(expected));
    }
}
