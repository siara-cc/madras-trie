#include "common_mt_dv1.hpp"

#define __fq1 __device__
#define __gq1 __device__
#include "../../include/madras/dv1/reader/static_trie.hpp"

#include <cuda_runtime.h>
#include <time.h>
#include <vector>
#include <stdio.h>
#include <stdlib.h>

// -----------------------------
// Device-compatible wrapper
// -----------------------------
class madras_cuda_wrapper {
public:
  madras::dv1::static_trie st;
  key_ctx *lines;
  uint8_t *file_buf_lines;
  size_t file_size;
  size_t line_count;
  uint8_t *query_status;

  __device__ madras_cuda_wrapper() {}

  __device__ void init(uint8_t *_file_buf_lines,
                       size_t _file_size,
                       uint8_t *_file_buf_mdx,
                       size_t _mdx_file_size,
                       key_ctx *_lines,
                       size_t _line_count,
                       uint8_t *_q_status) {
    lines = _lines;
    line_count = _line_count;
    query_status = _q_status;
    file_buf_lines = _file_buf_lines;
    file_size = _file_size;

    st.load_static_trie(_file_buf_mdx);
    memset(query_status, 0, line_count);
  }
};

// -----------------------------
// Init kernel
// -----------------------------
__global__ void init_madras_cuda_wrapper(
    madras_cuda_wrapper *d_cw,
    uint8_t *_file_buf_lines,
    size_t _file_size,
    uint8_t *_file_buf_mdx,
    size_t _mdx_file_size,
    key_ctx *_lines,
    size_t _line_count,
    uint8_t *_q_status) {

  if (blockIdx.x == 0 && threadIdx.x == 0) {
    d_cw->init(_file_buf_lines, _file_size,
               _file_buf_mdx, _mdx_file_size,
               _lines, _line_count,
               _q_status);
  }
}

// -----------------------------
// Lookup kernel (GRID-STRIDE)
// -----------------------------
__global__ void lookup_kernel(madras_cuda_wrapper *d_cw) {
  uint32_t tid = blockIdx.x * blockDim.x + threadIdx.x;
  uint32_t stride = blockDim.x * gridDim.x;

  madras::dv1::input_ctx in_ctx;

  for (uint32_t idx = tid; idx < d_cw->line_count; idx += stride) {
    key_ctx *ctx = &d_cw->lines[idx];

    in_ctx.key     = d_cw->file_buf_lines + ctx->key_loc;
    in_ctx.key_len = ctx->key_len;

    bool is_success = d_cw->st.lookup(in_ctx);
    ctx->leaf_id    = d_cw->st.leaf_rank1(in_ctx.node_id);

    uint8_t key_buf[256];
    size_t out_key_len;

    d_cw->st.reverse_lookup(ctx->leaf_id, &out_key_len, key_buf);

    if (out_key_len == in_ctx.key_len &&
        madras::dv1::cmn::memcmp(in_ctx.key, key_buf, out_key_len) == 0) {
      d_cw->query_status[idx] = is_success;
    }
  }
}

// -----------------------------
// Timing helper
// -----------------------------
static inline timespec now() {
  timespec t;
  clock_gettime(CLOCK_MONOTONIC, &t);
  return t;
}

// -----------------------------
// Main
// -----------------------------
int main(int argc, const char *argv[]) {

  if (argc < 2) {
    printf("Usage: cuda_bench <file_name>\n");
    return 1;
  }

  cudaSetDeviceFlags(cudaDeviceScheduleBlockingSync);
  cudaSetDevice(0);

  // -----------------------------
  // Load input
  // -----------------------------
  std::vector<key_ctx> lines;
  size_t file_size;
  uint8_t *file_buf = load_lines(argv[1], lines, file_size);

  size_t mdx_file_size;
  uint8_t *mdx_file_buf = load_mdx_file(argv[1], mdx_file_size);

  // -----------------------------
  // Device allocations
  // -----------------------------

  // GPU resident
  uint8_t *d_file_buf_lines;
  cudaMalloc(&d_file_buf_lines, file_size + 1);
  cudaMemcpy(d_file_buf_lines, file_buf,
             file_size + 1, cudaMemcpyHostToDevice);

  // Zero copy
  // uint8_t *h_file_buf_lines;
  // uint8_t *d_file_buf_lines;
  // cudaHostAlloc(&h_file_buf_lines, file_size + 1, cudaHostAllocMapped);
  // memcpy(h_file_buf_lines, file_buf, file_size + 1);
  // cudaHostGetDevicePointer(&d_file_buf_lines, h_file_buf_lines, 0);

  // GPU resident
  uint8_t *d_file_buf_mdx;
  cudaMalloc(&d_file_buf_mdx, mdx_file_size + 1);
  cudaMemcpy(d_file_buf_mdx, mdx_file_buf,
             mdx_file_size + 1, cudaMemcpyHostToDevice);

  // Zero copy
  // uint8_t *h_file_buf_mdx;
  // uint8_t *d_file_buf_mdx;
  // cudaHostAlloc(&h_file_buf_mdx, mdx_file_size + 1, cudaHostAllocMapped);
  // memcpy(h_file_buf_mdx, mdx_file_buf, mdx_file_size + 1);
  // cudaHostGetDevicePointer(&d_file_buf_mdx, h_file_buf_mdx, 0);

  key_ctx *d_lines;
  cudaMalloc(&d_lines, sizeof(key_ctx) * lines.size());
  cudaMemcpy(d_lines, lines.data(),
             sizeof(key_ctx) * lines.size(),
             cudaMemcpyHostToDevice);

  uint8_t *d_query_status;
  cudaMalloc(&d_query_status, lines.size());

  madras_cuda_wrapper *d_cw;
  cudaMalloc(&d_cw, sizeof(madras_cuda_wrapper));

  // -----------------------------
  // Init wrapper
  // -----------------------------
  init_madras_cuda_wrapper<<<1,1>>>(
      d_cw,
      d_file_buf_lines, file_size + 1,
      d_file_buf_mdx, mdx_file_size + 1,
      d_lines, lines.size(),
      d_query_status);

  cudaDeviceSynchronize();

  // -----------------------------
  // Launch lookup (SINGLE LAUNCH)
  // -----------------------------
  const int threads_per_block = 256;   // T4 sweet spot
  const int blocks = 160;              // 40 SMs × 4

  timespec t0 = now();

  lookup_kernel<<<blocks, threads_per_block>>>(d_cw);
  cudaDeviceSynchronize();

  timespec t1 = now();

  double elapsed =
      (t1.tv_sec - t0.tv_sec) +
      (t1.tv_nsec - t0.tv_nsec) * 1e-9;

  printf("Time taken for retrieve: %.6f\n", elapsed);

  // -----------------------------
  // Copy back & verify
  // -----------------------------
  uint8_t *query_status = new uint8_t[lines.size()];
  cudaMemcpy(query_status, d_query_status,
             lines.size(), cudaMemcpyDeviceToHost);

  size_t success_count = 0;
  for (size_t i = 0; i < lines.size(); i++) {
    if (query_status[i])
      success_count++;
  }

  printf("Success count: %lu, Total: %lu\n",
         success_count, lines.size());

  // -----------------------------
  // Cleanup
  // -----------------------------
  cudaFree(d_cw);
  cudaFree(d_lines);
  cudaFree(d_file_buf_lines);
  cudaFree(d_file_buf_mdx);
  cudaFree(d_query_status);

  // Zero copy
  // cudaFreeHost(h_file_buf_lines);
  // cudaFreeHost(h_file_buf_mdx);

  delete[] file_buf;
  delete[] mdx_file_buf;
  delete[] query_status;

  return 0;
}
