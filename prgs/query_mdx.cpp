#include <cstring>
#include <stdio.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include "../src/madras_dv1.hpp"

double get_wall_time_seconds() {
  struct timespec t;
  clock_gettime(CLOCK_REALTIME, &t);
  return t.tv_sec + t.tv_nsec / 1e9;
}

double time_taken_in_secs(clock_t t) {
  t = clock() - t;
  return ((double)t)/CLOCKS_PER_SEC;
}

clock_t print_time_taken(clock_t t, const char *msg) {
  double time_taken = time_taken_in_secs(t); // in seconds
  printf("%s %f (%lu, %lu)\n", msg, time_taken, clock() - t, CLOCKS_PER_SEC);
  return clock();
}

#include <cstdio>
#include <cstdlib>
#include <cstring>

#if defined(_WIN32) || defined(_WIN64)
#include <windows.h>
#include <psapi.h>

void print_resource_stats() {
    printf("Running on Windows...\n");

    IO_COUNTERS ioCounters;
    PROCESS_MEMORY_COUNTERS memCounters;

    if (GetProcessIoCounters(GetCurrentProcess(), &ioCounters)) {
        printf("Read operations: %llu\n", ioCounters.ReadOperationCount);
        printf("Write operations: %llu\n", ioCounters.WriteOperationCount);
        printf("Bytes read: %llu\n", ioCounters.ReadTransferCount);
        printf("Bytes written: %llu\n", ioCounters.WriteTransferCount);
    } else {
        printf("Failed to get IO counters.\n");
    }

    if (GetProcessMemoryInfo(GetCurrentProcess(), &memCounters, sizeof(memCounters))) {
        printf("Peak Working Set Size (bytes): %lu\n", memCounters.PeakWorkingSetSize);
    } else {
        printf("Failed to get memory info.\n");
    }
}

#elif defined(__APPLE__)
#include <unistd.h>
#include <sys/resource.h>
#include <libproc.h>

void print_resource_stats() {
    printf("Running on macOS...\n");

    pid_t pid = getpid();

    // Print basic block I/O stats via system
    char cmd[32];
    snprintf(cmd, 32, "ps -p %d -o oublk -o inblk", pid);
    system(cmd);

    // Detailed disk IO and memory usage
    rusage_info_current rusage;
    if (proc_pid_rusage(pid, RUSAGE_INFO_CURRENT, (void **)&rusage) == 0) {
        printf("Bytes read: %llu\n", rusage.ri_diskio_bytesread);
        printf("Bytes written: %llu\n", rusage.ri_diskio_byteswritten);
        printf("Physical footprint (bytes): %llu\n", rusage.ri_phys_footprint);
    } else {
        printf("Failed to get detailed rusage info.\n");
    }

    struct rusage ru;
    if (getrusage(RUSAGE_SELF, &ru) == 0) {
        printf("Max RSS (kilobytes): %ld\n", ru.ru_maxrss);
    }
}

#else // Linux
#include <unistd.h>
#include <sys/resource.h>
#include <fcntl.h>
#include <errno.h>
#include <stdint.h>
#include <linux/perf_event.h>
#include <sys/syscall.h>

// perf_event_open syscall wrapper
int perf_event_open(struct perf_event_attr *hw_event, pid_t pid,
                    int cpu, int group_fd, unsigned long flags) {
    return syscall(__NR_perf_event_open, hw_event, pid, cpu, group_fd, flags);
}

uint64_t read_perf_counter(uint32_t type, uint64_t config) {
    struct perf_event_attr pe{};
    pe.type = type;
    pe.size = sizeof(struct perf_event_attr);
    pe.config = config;
    pe.disabled = 0;
    pe.exclude_kernel = 0;
    pe.exclude_hv = 0;

    int fd = perf_event_open(&pe, 0, -1, -1, 0);
    if (fd == -1) {
        perror("perf_event_open");
        return 0;
    }

    uint64_t count = 0;
    if (read(fd, &count, sizeof(uint64_t)) == -1) {
        perror("read perf_event");
        close(fd);
        return 0;
    }

    close(fd);
    return count;
}

void print_resource_stats() {
    printf("Running on Linux...\n");

    pid_t pid = getpid();
    char cmd[100];
    sprintf(cmd, "ps -p %d -o oublk -o inblk", pid);
    system(cmd);

    sprintf(cmd, "cat /proc/%d/io", pid);
    system(cmd);

    struct rusage ru;
    if (getrusage(RUSAGE_SELF, &ru) == 0) {
        printf("Max RSS (kilobytes): %ld\n", ru.ru_maxrss);
        printf("In blocks: %ld\n", ru.ru_inblock);
        printf("Out blocks: %ld\n", ru.ru_oublock);
    }

    uint64_t instructions = read_perf_counter(PERF_TYPE_HARDWARE, PERF_COUNT_HW_INSTRUCTIONS);
    uint64_t cycles       = read_perf_counter(PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES);

    printf("Instructions retired: %lu\n", instructions);
    printf("CPU cycles elapsed: %lu\n", cycles);
}
#endif

int main(int argc, char *argv[]) {

  if (argc < 3) {
    printf("Usage: query_mdx <mdx_file> <column_index>");
    return 0;
  }

  clock_t t = clock();
  double start = get_wall_time_seconds();

  madras_dv1::static_trie_map dict_reader;
  dict_reader.set_print_enabled(true);
  // dict_reader.load(argv[1]);
  dict_reader.map_file_to_mem(argv[1]);

  t = print_time_taken(t, "Time taken for load: ");
  double end = get_wall_time_seconds();
  printf("Wall-clock time elapsed: %.6f seconds\n", end - start);
  start = get_wall_time_seconds();

  int column_idx = atoi(argv[2]);

  int row_count = dict_reader.get_key_count();
  if (row_count == 0)
    row_count = dict_reader.get_node_count();
  // else {
  //   printf("This utility only for mdx with no primary trie\n");
  //   return 1;
  // }
  char data_type = dict_reader.get_column_type(column_idx);
  char enc_type = dict_reader.get_column_encoding(column_idx);
  const char *col_name = dict_reader.get_column_name(column_idx);
  uint32_t node_count = dict_reader.get_node_count();
  size_t col_size = dict_reader.get_column_size(column_idx);
  printf("Row count: %d, Node count: %u, Col size: %lu\n", row_count, node_count, col_size);
  printf("Data type: %c, Enc_type: %c, Name: %s\n", data_type, enc_type, col_name);
  size_t val_len;
  madras_dv1::value_retriever_base *val_retriever = dict_reader.get_value_retriever(column_idx);
  madras_dv1::val_ctx vctx;
  if (data_type == MST_TEXT || data_type == MST_BIN) {
    vctx.init(dict_reader.get_max_val_len(column_idx) + 1, true, false);
    int64_t sum = 0;
    bool has_next = true;
    val_retriever->fill_val_ctx(0, vctx);
    do {
      has_next = val_retriever->next_val(vctx);
      if (*vctx.val_len != -1)
        sum += *vctx.val_len;
    } while (has_next);
    printf("Sum: %lld\n", sum);
  } else
  if (data_type == MST_INT) {
    int64_t sum = 0;
    if (enc_type == MSE_VINTGB) {
      madras_dv1::block_retriever_base *block_retriever = dict_reader.get_block_retriever<'S'>(column_idx);
      block_retriever->block_operation(0, node_count, &sum);
    } else {
      vctx.init(32, true, true);
      bool has_next = true;
      val_retriever->fill_val_ctx(0, vctx);
      do {
        has_next = val_retriever->next_val(vctx);
        if (*vctx.val_len != -1)
          sum += *((int64_t *) vctx.val);
      } while (has_next);
    }
    printf("Sum: %lld\n", sum);
  } else {
    double sum = 0;
    if (enc_type == MSE_VINTGB) {
      madras_dv1::block_retriever_base *block_retriever = dict_reader.get_block_retriever<'S'>(column_idx);
      block_retriever->block_operation(0, node_count, &sum);
    } else {
      vctx.init(32, true, true);
      bool has_next = true;
      val_retriever->fill_val_ctx(0, vctx);
      size_t count = 0;
      do {
        count++;
        has_next = val_retriever->next_val(vctx);
        if (*vctx.val_len != -1)
          sum += *((double *) vctx.val);
      } while (has_next);
    }
    printf("Sum: %lf\n", sum);
  }
  printf("vctx.node_id: %u\n", vctx.node_id);
  printf("eps: %lf, ", row_count / time_taken_in_secs(t) / 1000);
  t = print_time_taken(t, "Time taken for sum: ");
  end = get_wall_time_seconds();
  printf("Wall-clock time elapsed: %.6f seconds\n", end - start);

  print_resource_stats();

  return 1;
}
