// SUM 归约 CUDA kernel

extern "C" __global__ void sum_reduce_i64(
    const long long* data,
    const unsigned long long* validity,
    long long* output,
    unsigned int n
) {
    extern __shared__ long long sdata_i64[];

    unsigned int tid = threadIdx.x;
    unsigned int i = blockIdx.x * blockDim.x * 2 + threadIdx.x;
    unsigned int j = i + blockDim.x;

    long long sum = 0;
    if (i < n) {
        unsigned int word = i / 64;
        unsigned int bit = i % 64;
        if ((validity[word] >> bit) & 1) {
            sum += data[i];
        }
    }
    if (j < n) {
        unsigned int word = j / 64;
        unsigned int bit = j % 64;
        if ((validity[word] >> bit) & 1) {
            sum += data[j];
        }
    }
    sdata_i64[tid] = sum;
    __syncthreads();

    for (unsigned int s = blockDim.x / 2; s > 0; s >>= 1) {
        if (tid < s) {
            sdata_i64[tid] += sdata_i64[tid + s];
        }
        __syncthreads();
    }

    if (tid == 0) {
        output[blockIdx.x] = sdata_i64[0];
    }
}

extern "C" __global__ void sum_reduce_f64(
    const double* data,
    const unsigned long long* validity,
    double* output,
    unsigned int n
) {
    extern __shared__ double sdata_f64[];

    unsigned int tid = threadIdx.x;
    unsigned int i = blockIdx.x * blockDim.x * 2 + threadIdx.x;
    unsigned int j = i + blockDim.x;

    double sum = 0.0;
    if (i < n) {
        unsigned int word = i / 64;
        unsigned int bit = i % 64;
        if ((validity[word] >> bit) & 1) {
            sum += data[i];
        }
    }
    if (j < n) {
        unsigned int word = j / 64;
        unsigned int bit = j % 64;
        if ((validity[word] >> bit) & 1) {
            sum += data[j];
        }
    }
    sdata_f64[tid] = sum;
    __syncthreads();

    for (unsigned int s = blockDim.x / 2; s > 0; s >>= 1) {
        if (tid < s) {
            sdata_f64[tid] += sdata_f64[tid + s];
        }
        __syncthreads();
    }

    if (tid == 0) {
        output[blockIdx.x] = sdata_f64[0];
    }
}
