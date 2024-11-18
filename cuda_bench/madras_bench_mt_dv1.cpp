#include <future>
#include <functional>

#include "common_mt_dv1.hpp"

void process_range(size_t start, size_t end, uint8_t *file_buf_lines, key_ctx *lines, uint8_t *query_status, madras_dv1::static_trie *trie) {
  madras_dv1::input_ctx in_ctx;
  uint8_t key_buf[256];
  size_t out_key_len;
  for (size_t i = start; i < end; ++i) {
    key_ctx *ctx = &lines[i];
    in_ctx.key = file_buf_lines + ctx->key_loc;
    in_ctx.key_len = ctx->key_len;
    bool is_success = trie->lookup(in_ctx);
    ctx->leaf_id = trie->leaf_rank1(in_ctx.node_id);
    trie->reverse_lookup(ctx->leaf_id, &out_key_len, key_buf);
    if (out_key_len == in_ctx.key_len && memcmp(in_ctx.key, key_buf, out_key_len) == 0)
      query_status[i] = is_success;
  }
}

void distribute_lookup(uint8_t *file_buf_lines, key_ctx *lines, size_t total_lines, uint8_t *query_status, madras_dv1::static_trie *trie, size_t num_threads) {
  size_t lines_per_thread = (total_lines + num_threads - 1) / num_threads;
  std::vector<std::future<void>> futures;
  for (size_t t = 0; t < num_threads; ++t) {
    size_t start = t * lines_per_thread;
    size_t end = std::min(start + lines_per_thread, total_lines);
    if (start < end) {
      futures.push_back(std::async(std::launch::async, process_range, start, end, file_buf_lines, lines, query_status, trie));
    }
  }
  for (auto& future : futures) {
      future.get();
  }
}

int main(int argc, const char *argv[]) {

  if (argc < 3) {
    printf("Usage: mt_bench <file_name> <num_threads>\n");
    return 1;
  }

  std::vector<key_ctx> lines;

  uint8_t *file_buf = load_lines(argv[1], lines);
  if (file_buf == nullptr)
    return 1;

  uint8_t *mdx_file_buf = load_mdx_file(argv[1]);
  if (mdx_file_buf == nullptr) {
    delete [] file_buf;
    return 1;
  }

  clock_t t = clock();

  uint8_t *query_status = new uint8_t[lines.size()](); // all '\0's

  madras_dv1::static_trie *trie = new madras_dv1::static_trie();
  trie->load_static_trie(mdx_file_buf);

  int num_threads = atoi(argv[2]);
  if (num_threads == 1)
    process_range(0, lines.size(), file_buf, lines.data(), query_status, trie);
  else
    distribute_lookup(file_buf, lines.data(), lines.size(), query_status, trie, num_threads);

  t = print_time_taken(t, "Time taken for retrieve: ");

  size_t success_count = 0;
  for (size_t i = 0; i < lines.size(); i++) {
    if (query_status[i] == 1)
      success_count++;
  }
  printf("Success count: %lu, Total: %lu\n", success_count, lines.size());

  delete [] file_buf;
  delete [] mdx_file_buf;
  delete [] query_status;

  return 0;

}
