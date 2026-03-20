// COUNT 归约 CUDA kernel

extern "C" __global__ void count_reduce(
    const unsigned long long* validity,
    unsigned long long* output,
    unsigned int n
) {
    extern __shared__ unsigned long long sdata_count[];

    unsigned int tid = threadIdx.x;
    unsigned int i = blockIdx.x * blockDim.x * 2 + threadIdx.x;
    unsigned int j = i + blockDim.x;

    unsigned long long count = 0;
    if (i < n) {
        unsigned int word = i / 64;
        unsigned int bit = i % 64;
        if ((validity[word] >> bit) & 1) {
            count++;
        }
    }
    if (j < n) {
        unsigned int word = j / 64;
        unsigned int bit = j % 64;
        if ((validity[word] >> bit) & 1) {
            count++;
        }
    }
    sdata_count[tid] = count;
    __syncthreads();

    for (unsigned int s = blockDim.x / 2; s > 0; s >>= 1) {
        if (tid < s) {
            sdata_count[tid] += sdata_count[tid + s];
        }
        __syncthreads();
    }

    if (tid == 0) {
        output[blockIdx.x] = sdata_count[0];
    }
}
