#include "common_mt_dv1.hpp"

#define __fq1 __device__
#define __gq1 __device__
#include "../src/madras_dv1.hpp"

#include <cuda_runtime.h>

// Device-compatible madras_cuda_wrapper class
class madras_cuda_wrapper {
  public:
    madras_dv1::static_trie *st;
    key_ctx *lines;
    uint8_t *file_buf_lines;
    size_t file_size;
    size_t line_count;
    uint8_t *query_status;
    __device__ madras_cuda_wrapper() {}
    __device__ void init(uint8_t *_file_buf_lines, size_t _file_size, 
                         uint8_t *_file_buf_mdx, size_t _mdx_file_size,
                         key_ctx *_lines, size_t _line_count, uint8_t *_q_status) {
      lines = _lines;
      line_count = _line_count;
      query_status = _q_status;
      file_buf_lines = _file_buf_lines;
      file_size = _file_size;
      st = new madras_dv1::static_trie();
      st->load_static_trie(_file_buf_mdx);
      memset(query_status, '\0', line_count);
    }
    __device__ madras_dv1::static_trie *get_trie_inst() const {
      return st;
    }
};

// Kernel for initializing madras_cuda_wrapper on GPU
__global__ void init_madras_cuda_wrapper(madras_cuda_wrapper *d_nl, uint8_t *_file_buf_lines, size_t _file_size,
                    uint8_t *_file_buf_mdx, size_t _mdx_file_size,
                    key_ctx *_lines, size_t _line_count, uint8_t *_q_status) {
    if (threadIdx.x == 0 && blockIdx.x == 0) {
      d_nl->init(_file_buf_lines, _file_size, _file_buf_mdx, _mdx_file_size, _lines, _line_count, _q_status);
    }
}

// Kernel for invoking the lookup on the GPU
__global__ void lookup_1x1_kernel(madras_cuda_wrapper *d_cw, uint32_t num_queries) {
  madras_dv1::input_ctx in_ctx;
  uint8_t key_buf[256];
  size_t out_key_len;
  for (uint32_t idx = 0; idx < num_queries; idx++) {
    key_ctx *ctx = &d_cw->lines[idx];
    in_ctx.key = d_cw->file_buf_lines + ctx->key_loc;
    in_ctx.key_len = ctx->key_len;
    bool is_success = d_cw->get_trie_inst()->lookup(in_ctx);
    ctx->leaf_id = d_cw->get_trie_inst()->leaf_rank1(in_ctx.node_id);
    d_cw->get_trie_inst()->reverse_lookup(ctx->leaf_id, &out_key_len, key_buf);
    if (out_key_len == in_ctx.key_len && madras_dv1::cmn::memcmp(in_ctx.key, key_buf, out_key_len) == 0)
      d_cw->query_status[idx] = is_success;
    // printf("Is success: %d\n", is_success);
    // printf("Node id: %u\n", in_ctx.node_id);
  }
}

// Kernel for invoking the lookup on the GPU
__global__ void lookup_kernel(madras_cuda_wrapper *d_cw, uint32_t start_idx, uint32_t num_queries) {
  uint32_t cuda_idx = blockIdx.x * blockDim.x + threadIdx.x;
  if (cuda_idx < num_queries) {
    uint32_t idx = start_idx + cuda_idx;
    madras_dv1::input_ctx in_ctx;
    key_ctx *ctx = &d_cw->lines[idx];
    in_ctx.key = d_cw->file_buf_lines + ctx->key_loc;
    in_ctx.key_len = ctx->key_len;
    bool is_success = d_cw->get_trie_inst()->lookup(in_ctx);
    ctx->leaf_id = d_cw->get_trie_inst()->leaf_rank1(in_ctx.node_id);
    uint8_t key_buf[256];
    size_t out_key_len;
    d_cw->get_trie_inst()->reverse_lookup(ctx->leaf_id, &out_key_len, key_buf);
    if (out_key_len == in_ctx.key_len && madras_dv1::cmn::memcmp(in_ctx.key, key_buf, out_key_len) == 0)
      d_cw->query_status[idx] = is_success;
    // printf("Is success: %d\n", is_success);
    // printf("Node id: %u\n", in_ctx.node_id);
  }
}

void checkCudaError(cudaError_t result, const char* msg) {
    if (result != cudaSuccess) {
        printf("%s Error: %s\n", msg, cudaGetErrorString(result));
        exit(EXIT_FAILURE);
    }
}

int main(int argc, const char *argv[]) {

  if (argc < 4) {
    printf("Usage: cuda_bench <file_name> <num_threads> <num_blocks>\n");
    return 1;
  }

  cudaSetDevice(0);
  cudaSetDeviceFlags(cudaDeviceScheduleYield);
  cudaSetDeviceFlags(cudaDeviceScheduleBlockingSync);

  std::vector<key_ctx> lines;
  size_t file_size;
  uint8_t *file_buf = load_lines(argv[1], lines, file_size);

  uint8_t *d_file_buf;
  cudaMalloc(&d_file_buf, file_size + 1);
  cudaMemcpy(d_file_buf, file_buf, file_size + 1, cudaMemcpyHostToDevice);

  size_t mdx_file_size;
  uint8_t *mdx_file_buf = load_mdx_file(argv[1], mdx_file_size);

  uint8_t *d_file_buf_lines;
  cudaMalloc(&d_file_buf_lines, file_size + 1);
  cudaMemcpy(d_file_buf_lines, file_buf, file_size + 1, cudaMemcpyHostToDevice);

  uint8_t *d_file_buf_mdx;
  cudaMallocManaged(&d_file_buf_mdx, mdx_file_size + 1);
  cudaMemcpy(d_file_buf_mdx, mdx_file_buf, mdx_file_size + 1, cudaMemcpyHostToDevice);
  // uint8_t *h_file_buf_mdx; // Host pointer
  // uint8_t *d_file_buf_mdx; // Device pointer
  // cudaHostAlloc((void**)&h_file_buf_mdx, mdx_file_size + 1, cudaHostAllocMapped);
  // memcpy(h_file_buf_mdx, mdx_file_buf, mdx_file_size + 1);
  // cudaHostGetDevicePointer(&d_file_buf_mdx, h_file_buf_mdx, 0);

  key_ctx *d_lines;
  cudaMalloc(&d_lines, sizeof(key_ctx) * lines.size());
  cudaMemcpy(d_lines, lines.data(), sizeof(key_ctx) * lines.size(), cudaMemcpyHostToDevice);

  uint8_t *d_query_status;
  cudaMalloc(&d_query_status, lines.size());

  madras_cuda_wrapper *d_cw;
  cudaMalloc(&d_cw, sizeof(madras_cuda_wrapper));

  //cudaDeviceSetLimit(cudaLimitStackSize, 4096);

  // Initialize the `madras_cuda_wrapper` object with GPU data
  init_madras_cuda_wrapper<<<1, 1>>>(d_cw, d_file_buf_lines, file_size + 1,
        d_file_buf_mdx, mdx_file_size + 1, d_lines, lines.size(), d_query_status);
  cudaDeviceSynchronize(); // Ensure initialization completes before lookup_kernel

  struct timespec t;
  clock_gettime(CLOCK_REALTIME, &t);

  // Launch the kernel to perform lookups
  size_t threads_per_block = atoi(argv[2]);
  size_t blocks = atoi(argv[3]);
  size_t capacity = blocks * threads_per_block;
  size_t iter_count = lines.size() / capacity;
  if ((lines.size() % capacity) > 0)
    iter_count++;
  for (size_t i = 0; i < iter_count; i++) {
    size_t query_count = capacity;
    if (i == (iter_count - 1) && (lines.size() % capacity) > 0)
      query_count = (lines.size() % capacity);
    lookup_kernel<<<blocks, threads_per_block>>>(d_cw, i * capacity, query_count);
    cudaDeviceSynchronize();
  }

  // lookup_1x1_kernel<<<1, 1>>>(d_cw, lines.size());
  // cudaDeviceSynchronize();

  //cudaDeviceSynchronize();

  t = print_time_taken(t, "Time taken for retrieve: ");

  // Copy results back to host
  uint8_t *query_status = new uint8_t[lines.size()];
  cudaMemcpy(query_status, d_query_status, lines.size(), cudaMemcpyDeviceToHost);
  // cudaMemcpy(lines.data(), d_lines, sizeof(key_ctx) * lines.size(), cudaMemcpyDeviceToHost);

  size_t success_count = 0;
  for (size_t i = 0; i < lines.size(); i++) {
    if (query_status[i] == 1)
      success_count++;
  }
  printf("Success count: %lu, Total: %lu\n", success_count, lines.size());

  // Cleanup GPU memory
  cudaFree(d_cw);
  cudaFree(d_lines);
  cudaFree(d_file_buf);
  cudaFree(d_file_buf_mdx);
  cudaFree(d_query_status);

  delete [] file_buf;
  delete [] mdx_file_buf;
  delete [] query_status;

  return 0;

}
