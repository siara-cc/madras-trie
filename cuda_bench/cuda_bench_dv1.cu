#include <stdio.h>
#include <stdint.h>
#include <sys/stat.h>
#include <time.h>

#define __fq1 __device__
#define __gq1 __device__
#include "../src/madras_tt_dv1.hpp"

#include <cuda_runtime.h>

typedef struct {
  uint32_t key_loc;
  uint32_t key_len;
  union {
    uint32_t leaf_id;
    uint32_t leaf_seq;
  };
} key_ctx;

double time_taken_in_secs(clock_t t) {
  t = clock() - t;
  return ((double)t)/CLOCKS_PER_SEC;
}

clock_t print_time_taken(clock_t t, const char *msg) {
  double time_taken = time_taken_in_secs(t); // in seconds
  printf("%s %lf\n", msg, time_taken);
  return clock();
}

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
__global__ void lookup_kernel(madras_cuda_wrapper *d_cw, size_t start_idx, size_t num_queries) {
  int idx = blockIdx.x * blockDim.x + threadIdx.x;
  if (idx < num_queries) {
    madras_dv1::input_ctx in_ctx;
    in_ctx.key = d_cw->file_buf_lines + d_cw->lines[start_idx + idx].key_loc;
    in_ctx.key_len = d_cw->lines[start_idx + idx].key_len;
    bool is_success = d_cw->get_trie_inst()->lookup(in_ctx);
    d_cw->query_status[start_idx + idx] = is_success;
    // printf("Is success: %d\n", is_success);
    // printf("Node id: %u\n", in_ctx.node_id);
  }
}

int main(int argc, const char *argv[]) {

  clock_t t = clock();

  struct stat file_stat;
  memset(&file_stat, '\0', sizeof(file_stat));
  stat(argv[1], &file_stat);
  size_t file_size = file_stat.st_size;
  printf("File_name: %s, size: %lu\n", argv[1], file_size);

  FILE *fp = fopen(argv[1], "rb");
  if (fp == NULL) {
    perror("Could not open file; ");
    return 1;
  }
  uint8_t *file_buf = new uint8_t[file_size + 1];
  size_t res = fread(file_buf, 1, file_size, fp);
  if (res != file_size) {
    perror("Error reading file: ");
    free(file_buf);
    return 1;
  }
  fclose(fp);

  std::vector<key_ctx> lines;
  size_t line_count = 0;
  bool is_sorted = true;
  const uint8_t *prev_line = (const uint8_t *) "";
  size_t prev_line_len = 0;
  size_t line_len = 0;
  uint8_t *line = gen::extract_line(file_buf, line_len, file_size);
  do {
    if (gen::compare(line, line_len, prev_line, prev_line_len) != 0) {
      uint8_t *key = line;
      int key_len = line_len;
      if (gen::compare(key, key_len, prev_line, gen::min(prev_line_len, key_len)) < 0)
        is_sorted = false;
      lines.push_back((key_ctx) {(uint32_t) (line - file_buf), (uint32_t) line_len, 0});
      prev_line = line;
      prev_line_len = line_len;
      line_count++;
      if ((line_count % 100000) == 0) {
        printf(".");
        fflush(stdout);
      }
    }
    line = gen::extract_line(line, line_len, file_size - (line - file_buf) - line_len);
  } while (line != NULL);
  printf("\n");
  printf("Sorted? : %d\n", is_sorted);

  uint8_t *d_file_buf;
  cudaMalloc(&d_file_buf, file_stat.st_size + 1);
  cudaMemcpy(d_file_buf, file_buf, file_stat.st_size + 1, cudaMemcpyHostToDevice);

  char mdx_file_name[strlen(argv[1]) + 5];
  strcpy(mdx_file_name, argv[1]);
  strcat(mdx_file_name, ".mdx");
  memset(&file_stat, '\0', sizeof(file_stat));
  stat(mdx_file_name, &file_stat);
  size_t mdx_file_size = file_stat.st_size;
  printf("MDX File_name: %s, size: %lu\n", mdx_file_name, mdx_file_size);
  fp = fopen(mdx_file_name, "rb");
  if (fp == NULL) {
    perror("Could not open mdx file; ");
    free(file_buf);
    return 1;
  }
  uint8_t *mdx_file_buf = new uint8_t[mdx_file_size + 1];
  res = fread(mdx_file_buf, 1, mdx_file_size, fp);
  if (res != mdx_file_size) {
    perror("Error reading mdx file: ");
    free(file_buf);
    free(mdx_file_buf);
    return 1;
  }
  fclose(fp);

  uint8_t *d_file_buf_lines;
  cudaMalloc(&d_file_buf_lines, file_size + 1);
  cudaMemcpy(d_file_buf_lines, file_buf, file_size + 1, cudaMemcpyHostToDevice);

  uint8_t *d_file_buf_mdx;
  cudaMalloc(&d_file_buf_mdx, mdx_file_size + 1);
  cudaMemcpy(d_file_buf_mdx, mdx_file_buf, mdx_file_size + 1, cudaMemcpyHostToDevice);

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

  // Launch the kernel to perform lookups
  size_t threads_per_block = 1024;
  size_t blocks = 6;
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

  // lookup_kernel<<<2, 10>>>(d_cw, num_queries, d_query_status);

  // Copy results back to host
  uint8_t *query_status = new uint8_t[lines.size()];
  cudaMemcpy(query_status, d_query_status, lines.size(), cudaMemcpyDeviceToHost);

  t = print_time_taken(t, "Time taken for retrieve: ");

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
