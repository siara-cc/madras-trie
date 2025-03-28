#include <iostream>
#include <cstring>
#include <time.h>
#include <stdio.h>
#include <string.h>
#include <limits.h>
#include <sqlite3.h>
#include <inttypes.h>
#include <sys/stat.h>

#include "../../ds_common/src/gen.hpp"

#include "madras_dv1.hpp"
#include "madras_builder_dv1.hpp"

const static char *dt_formats[] = {"%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%d", "%m/%d/%Y %I:%M %p", "%d/%m/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"};
const static size_t dt_format_lens[] = {8, 8, 8, 19, 19, 19, 19, 19, 19};

double time_taken_in_secs(struct timespec t) {
  struct timespec t_end;
  clock_gettime(CLOCK_REALTIME, &t_end);
  return (t_end.tv_sec - t.tv_sec) + (t_end.tv_nsec - t.tv_nsec) / 1e9;
}

struct timespec print_time_taken(struct timespec t, const char *msg) {
  double time_taken = time_taken_in_secs(t); // in seconds
  printf("%s %lf\n", msg, time_taken);
  clock_gettime(CLOCK_REALTIME, &t);
  return t;
}

static double round_dbl(const double input, char data_type) {
  double p10 = gen::pow10(data_type - MST_DEC0);
  int64_t i64 = static_cast<int64_t>(input * p10);
  double ret = i64;
  ret /= p10;
  return ret;
}

void reduce_sql_value(uint64_t *values, double *values_dbl, size_t *value_lens, sqlite3_stmt *stmt, size_t exp_col_idx, char exp_col_type, char encoding_type, size_t ins_seq_id, size_t sql_col_idx) {
  int64_t s64;
  double dbl;
  if (sqlite3_column_type(stmt, sql_col_idx) == SQLITE_NULL) {
    if (exp_col_type == MST_TEXT || exp_col_type == MST_BIN)
      values[exp_col_idx] = 0;
    else
      values[exp_col_idx] = INT64_MIN;
    value_lens[exp_col_idx] = 0;
  } else if (exp_col_type == MST_TEXT) {
    values[exp_col_idx] = (uint64_t) sqlite3_column_text(stmt, sql_col_idx);
    value_lens[exp_col_idx] = sqlite3_column_bytes(stmt, sql_col_idx);
  } else if (exp_col_type == MST_BIN) {
    values[exp_col_idx] = (uint64_t) sqlite3_column_blob(stmt, sql_col_idx);
    value_lens[exp_col_idx] = sqlite3_column_bytes(stmt, sql_col_idx);
  } else if (exp_col_type >= MST_DATE_US && exp_col_type <= MST_DATETIME_ISOT_MS) {
    struct tm tm = {0};
    const uint8_t *dt_txt_db = sqlite3_column_text(stmt, sql_col_idx);
    char dt_txt[dt_format_lens[exp_col_type - MST_DATE_US] + 1];
    strncpy(dt_txt, (const char *) dt_txt_db, dt_format_lens[exp_col_type - MST_DATE_US]);
    dt_txt[dt_format_lens[exp_col_type - MST_DATE_US]] = 0;
    // printf("%s, %s\n", dt_txt, dt_formats[exp_col_type - MST_DATE_US]);
    char *result = strptime((const char *) dt_txt, dt_formats[exp_col_type - MST_DATE_US], &tm);
    if (result == nullptr || *result != '\0') {
      printf(" e%lu/%lu", ins_seq_id, sql_col_idx);
      values[exp_col_idx] = INT64_MIN;
    } else {
      uint64_t dt_val = (uint64_t) mktime(&tm);
      if (exp_col_type >= MST_DATE_US && exp_col_type <= MST_DATE_ISO)
        dt_val /= 86400;
      if (exp_col_type == MST_DATETIME_ISO_MS || exp_col_type == MST_DATETIME_ISOT_MS) {
        dt_val *= 1000;
        char *dot_pos = (char *) memchr(dt_txt_db, '.', strnlen((const char *) dt_txt_db, 24));
        if (dot_pos != nullptr)
          dt_val += atoi(dot_pos + 1);
      }
      // printf("dt_val: %llu\n", dt_val);
      values[exp_col_idx] = dt_val;
    }
    value_lens[exp_col_idx] = 8;
  } else if (exp_col_type == MST_INT) {
    s64 = sqlite3_column_int64(stmt, sql_col_idx);
    memcpy(values + exp_col_idx, &s64, 8);
    value_lens[exp_col_idx] = 8;
  } else if (exp_col_type >= MST_DEC0 && exp_col_type <= MST_DEC9) {
    values_dbl[exp_col_idx] = sqlite3_column_double(stmt, sql_col_idx);
    value_lens[exp_col_idx] = 8;
  }
}

int main(int argc, char* argv[]) {

  sqlite3 *db;
  sqlite3_stmt *stmt;
  sqlite3_stmt *stmt_col_names;
  int rc;
  if (argc < 6) {
    std::cout << std::endl;
    // std::cout << "Usage: export_sqlite <db_file> <table_or_select> <storage_types> <encoding_types> <row_count=0> <offset=0> <key_column_list_0> <key_column_list_1> ... <key_column_list_n>" << std::endl;
    std::cout << "Usage: export_sqlite <db_file> <table_or_select> <key_col_idx> <storage_types> <encoding_types> [row_count]" << std::endl;
    std::cout << std::endl;
    std::cout << "  <db_file>         - Sqlite database file name [with path]" << std::endl;
    std::cout << std::endl;
    std::cout << "  <table_or_select> - Name of table to export or select statement" << std::endl;
    std::cout << std::endl;
    std::cout << "  <key_col_idx>     - Index of key column (starting with 1)." << std::endl;
    std::cout << "                      If 0, a sequence number will be generated as key" << std::endl;
    std::cout << std::endl;
    std::cout << "  <storage_types>   - String having storage type letter for each column." << std::endl;
    std::cout << "                      Following types are supported:" << std::endl;
    std::cout << "                      t : text" << std::endl;
    std::cout << "                      * : binary data" << std::endl;
    std::cout << "                      0 : signed compressed 60-bit integer" << std::endl;
    std::cout << "                      1 to 9 : signed compressed float having as many mantissa digits" << std::endl;
    std::cout << "                      - : ignore column" << std::endl;
    std::cout << "                      ^ : key column (to match key_col_idx above)" << std::endl;
    std::cout << "                          In this version only 1 key column can be specified" << std::endl;
    std::cout << "  <encoding_types>  - String having encoding type letter for each column." << std::endl;
    std::cout << "                      Following types are supported:" << std::endl;
    std::cout << "                      u : Make unique values (remove duplicates before storing)" << std::endl;
    std::cout << "                      d : Make unique values and apply delta coding (only for numeric columns)" << std::endl;
    std::cout << "  [row_count]       - No. of rows to export. If not given, all rows are exported." << std::endl;
    std::cout << std::endl;
    return 1;
  }

  struct timespec t;
  clock_gettime(CLOCK_REALTIME, &t);

  rc = sqlite3_open(argv[1], &db);
  if (rc) {
    std::cerr << "Can't open database: " << sqlite3_errmsg(db) << std::endl;
    return 1;
  }

  const char *table_name = "vtab";
  const char *arg_sel_or_tbl = argv[2];
  bool dont_verify = false;
  if (*arg_sel_or_tbl == '-') {
    dont_verify = true;
    arg_sel_or_tbl++;
  }
  std::string sql;
  if (sqlite3_strnicmp(argv[2], "select ", 7) == 0) {
    sql = arg_sel_or_tbl;
  } else {
    sql = "SELECT * FROM ";
    sql += arg_sel_or_tbl;
    table_name = arg_sel_or_tbl;
  }

  rc = sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt_col_names, nullptr);
  if (rc != SQLITE_OK) {
    std::cerr << "SQL error: " << sqlite3_errmsg(db) << std::endl;
    sqlite3_close(db);
    return 1;
  }
  sqlite3_step(stmt_col_names);

  int sql_column_count = sqlite3_column_count(stmt_col_names);
  int column_count = sql_column_count;
  const char *storage_types = argv[4];
  if (column_count > strlen(storage_types)) {
    std::cerr << "Storage types not specified for all columns" << std::endl;
    sqlite3_finalize(stmt_col_names);
    sqlite3_close(db);
    return 1;
  }
  if (column_count < strlen(storage_types))
    column_count = strlen(storage_types);
  const char *encoding_types = argv[5];
  int row_count = INT_MAX;
  if (argc > 6)
    row_count = atoi(argv[6]);
  if (row_count == 0)
    row_count = INT_MAX;

  const char *sk_col_positions = "";
  if (argc > 7)
    sk_col_positions = argv[7];
    
  const char *col_positions_str = argv[3];
  std::vector<uint16_t> col_positions;
  if (col_positions_str[0] != '0' && col_positions_str[0] != '\0') {
    size_t col_positions_len = strlen(col_positions_str);
    const char *cur_col = col_positions_str;
    for (size_t i = 0; i < col_positions_len; i++) {
      if (col_positions_str[i] == '+') {
        col_positions.push_back(atoi(cur_col) - 1);
        cur_col = col_positions_str + i + 1;
      }
    }
    col_positions.push_back(atoi(cur_col) - 1);
  }
  size_t pk_col_count = col_positions.size();
  for (int i = 0; i < column_count; i++) {
    bool is_pk_col = false;
    for (size_t k = 0; k < pk_col_count; k++) {
      if (col_positions[k] == i) {
        is_pk_col = true;
        break;
      }
    }
    if (storage_types[i] == '-') {
      if (is_pk_col) {
        std::cerr << "Storage type cannot be '-' for primary column\n" << std::endl;
        sqlite3_finalize(stmt_col_names);
        sqlite3_close(db);
        return 1;
      }
      continue;
    }
    if (encoding_types[i] != 'u') {
      if (is_pk_col) {
        std::cerr << "Encoding type should be u for primary columns\n" << std::endl;
        sqlite3_finalize(stmt_col_names);
        sqlite3_close(db);
        return 1;
      }
    }
    if (!is_pk_col)
      col_positions.push_back(i);
  }

  std::string column_names = table_name;
  std::string col_types;
  std::string col_encodings;
  size_t exp_col_count = 0;
  for (size_t i = 0; i < col_positions.size(); i++) {
    uint16_t col_pos = col_positions[i];
    if (col_pos < sql_column_count) {
      const char* column_name = sqlite3_column_name(stmt_col_names, col_pos);
      column_names.append(",");
      column_names.append(column_name);
    }
    col_types.append(1, storage_types[col_pos]);
    col_encodings.append(1, encoding_types[col_pos]);
    exp_col_count++;
  }
  printf("Col Count: %lu, PK Col Count: %lu, Table/Key/Column names: %s, types: %s, encodings: %s\n",
    exp_col_count, pk_col_count, column_names.c_str(), col_types.c_str(), col_encodings.c_str());

  if (exp_col_count == 0) {
    std::cerr << "At least 1 column to be specified for export" << std::endl;
    sqlite3_finalize(stmt_col_names);
    sqlite3_close(db);
    return 1;
  }

  rc = sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr);
  if (rc != SQLITE_OK) {
    std::cerr << "SQL error: " << sqlite3_errmsg(db) << std::endl;
    sqlite3_finalize(stmt_col_names);
    sqlite3_close(db);
    return 1;
  }

  std::string out_file = argv[1];
  out_file += ".mdx";
  madras_dv1::bldr_options bldr_opts = madras_dv1::dflt_opts;
  bldr_opts.inner_tries = true;
  bldr_opts.sort_nodes_on_freq = false;
  madras_dv1::builder mb(out_file.c_str(), column_names.c_str(), exp_col_count, 
      col_types.c_str(), col_encodings.c_str(), 0, pk_col_count,
      &bldr_opts, sk_col_positions);
  mb.set_print_enabled();
  mb.open_file();

  size_t exp_col_idx = 0;
  size_t ins_seq_id = 0;
  uint64_t values[exp_col_count];
  double *values_dbl = (double *) values;
  size_t value_lens[exp_col_count];
  while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
    exp_col_idx = 0;
    for (size_t i = 0; i < col_positions.size(); i++) {
      size_t sql_col_idx = col_positions[i];
      char exp_col_type = storage_types[sql_col_idx];
      char encoding_type = encoding_types[sql_col_idx];
      reduce_sql_value(values, values_dbl, value_lens, stmt, exp_col_idx, exp_col_type, encoding_type, ins_seq_id, sql_col_idx);
      exp_col_idx++;
    }
    if (mb.insert(values, value_lens)) {
      std::cerr << "Unexpected: Record found: " << ins_seq_id << ". Check Primary key definition." << std::endl;
      sqlite3_finalize(stmt_col_names);
      sqlite3_close(db);
      return 1;
    }
    if (ins_seq_id >= row_count)
      break;
    ins_seq_id++;
    if ((ins_seq_id % 100000) == 0) {
      std::cout << ".";
      std::flush(std::cout);
    }
  }

  std::cout << std::endl;

  mb.build_and_write_all();

  t = print_time_taken(t, "Time taken for build: ");

  madras_dv1::static_trie_map stm;
  stm.load(out_file.c_str());
  printf("Tbl name: %s\n", stm.get_table_name());
  printf("Col types: %s\n", stm.get_column_types());
  printf("Col encodings: %s\n", stm.get_column_encodings());
  uint16_t stm_col_count = stm.get_column_count();
  printf("Col Count: %u, Cols:", stm_col_count);
  for (int i = 0; i < stm_col_count; i++)
    printf(", %s (%u)", stm.get_column_name(i), stm.get_max_val_len(i));
  printf("\n");

  if (dont_verify) {
    sqlite3_finalize(stmt_col_names);
    sqlite3_finalize(stmt);
    sqlite3_close(db);
    return 1;
  }

  sqlite3_reset(stmt);
  ins_seq_id = 0;
  madras_dv1::input_ctx in_ctx;
  uint8_t *key = new uint8_t[stm.get_max_key_len()];
  uint32_t ptr_count[column_count];
  int64_t int_sums[column_count];
  double dbl_sums[column_count];
  uint8_t key_val[stm.get_max_key_len() + 1];
  memset(ptr_count, '\xFF', sizeof(uint32_t) * column_count);
  memset(int_sums, '\0', sizeof(int64_t) * column_count);
  memset(dbl_sums, '\0', sizeof(double) * column_count);
  while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
    if (ins_seq_id > row_count)
      break;
    uint32_t node_id = ins_seq_id;
    if (pk_col_count > 0) {
      gen::byte_vec key_rec;
      for (size_t i = 0; i < pk_col_count; i++) {
        size_t sql_col_idx = col_positions[i];
        char exp_col_type = storage_types[sql_col_idx];
        char encoding_type = encoding_types[sql_col_idx];
        reduce_sql_value(values, values_dbl, value_lens, stmt, i, exp_col_type, encoding_type, ins_seq_id, sql_col_idx);
        mb.append_rec_value(exp_col_type, encoding_type, (void *) &values[i], (const uint8_t *) values[i], value_lens[i],
            key_rec, i < (pk_col_count - 1) ? APPEND_REC_KEY_MIDDLE : APPEND_REC_KEY_LAST);
      }
      in_ctx.key = key_rec.data();
      in_ctx.key_len = key_rec.size();
      bool is_found = stm.lookup(in_ctx);
      if (!is_found) {
        std::cout << "Key not found: nid:" << in_ctx.node_id << ", seq:" << ins_seq_id << ", len:" << key_rec.size() << std::endl;
        ins_seq_id++;
        continue;
      }
      node_id = in_ctx.node_id;
    }
    size_t col_val_idx = pk_col_count;
    for (size_t i = pk_col_count; i < sql_column_count; i++) {
      size_t sql_col_idx = col_positions[i];
      char exp_col_type = storage_types[sql_col_idx];
      char encoding_type = encoding_types[sql_col_idx];
      if (sqlite3_column_type(stmt, sql_col_idx) == SQLITE_NULL) {
        uint8_t val_buf[stm.get_max_val_len()];
        size_t val_len = 8;
        const uint8_t *ret_buf = stm.get_col_val(node_id, col_val_idx, &val_len, val_buf); // , &ptr_count[col_val_idx]);
        size_t null_value_len;
        uint8_t *null_value = stm.get_null_value(null_value_len);
        if (val_len != null_value_len || memcmp(val_buf, null_value, null_value_len) != 0) {
          printf("Val not null: nid: %u, seq: %lu, col: %lu, A:%lu,[%.*s]/%lu\n", node_id, ins_seq_id, col_val_idx, val_len, (int) val_len, val_buf, null_value_len);
          printf("%d, %d\n", val_buf[0], val_buf[1]);
        }
      } else
      if (exp_col_type == MST_TEXT || exp_col_type == MST_BIN) {
        const uint8_t *sql_val = (const uint8_t *) sqlite3_column_blob(stmt, sql_col_idx);
        size_t sql_val_len = sqlite3_column_bytes(stmt, sql_col_idx);
        size_t val_len = stm.get_max_val_len(col_val_idx) + 1;
        uint8_t val_buf[val_len];
        const uint8_t *ret_buf = stm.get_col_val(node_id, col_val_idx, &val_len, val_buf); // , &ptr_count[col_val_idx]);
        if (sql_val == nullptr) {
          size_t empty_value_len;
          uint8_t *empty_value = stm.get_empty_value(empty_value_len);
          if (val_len != empty_value_len || memcmp(val_buf, empty_value, empty_value_len) != 0) {
            printf("Val not empty: nid: %u, seq: %lu, col: %lu, A:%lu,[%.*s]/%lu\n", node_id, ins_seq_id, col_val_idx, val_len, (int) val_len, val_buf, empty_value_len);
            printf("%d, %d\n", val_buf[0], val_buf[1]);
          }
        } else if (val_len != sql_val_len) {
          printf("Val len mismatch: nid: %u, seq: %lu, col: %lu, E:%lu/A:%lu\n", node_id, ins_seq_id, col_val_idx, sql_val_len, val_len);
          printf("Expected: [%s]\n", (sql_val == nullptr ? "nullptr" : (const char *) sql_val));
          printf("Found: [%.*s]\n", (int) val_len, ret_buf);
        } else {
          if (memcmp(sql_val, ret_buf, val_len) != 0) {
            printf("Val mismatch: nid: %u, seq: %lu, col: %lu, E:%lu/A:%lu\n", node_id, ins_seq_id, col_val_idx, sql_val_len, val_len);
            printf("Expected: [%s]\n", (sql_val == nullptr ? "nullptr" : (const char *) sql_val));
            printf("Found: [%.*s]\n", (int) val_len, ret_buf);
          }
        }
      } else if (exp_col_type >= MST_DATE_US && exp_col_type <= MST_DATETIME_ISOT_MS) {
        const uint8_t *sql_val = (const uint8_t *) sqlite3_column_blob(stmt, sql_col_idx);
        size_t sql_val_len = sqlite3_column_bytes(stmt, sql_col_idx);
        uint8_t val_buf[16];
        size_t val_len = 8;
        const uint8_t *ret_buf = stm.get_col_val(node_id, col_val_idx, &val_len, val_buf); // , &ptr_count[col_val_idx]);
        time_t original_epoch = *((int64_t *) ret_buf);
        // printf("orig epoch: %llu\n", original_epoch);
        if (exp_col_type >= MST_DATE_US && exp_col_type <= MST_DATE_ISO)
          original_epoch *= 86400;
        if (exp_col_type == MST_DATETIME_ISO_MS || exp_col_type == MST_DATETIME_ISOT_MS)
          original_epoch /= 1000;
        char dt_txt[50];
        strftime(dt_txt, sizeof(dt_txt), dt_formats[exp_col_type - MST_DATE_US], localtime(&original_epoch));
        val_len = strlen(dt_txt);
        if (exp_col_type == MST_DATETIME_ISO_MS || exp_col_type == MST_DATETIME_ISOT_MS) {
          original_epoch = *((int64_t *) ret_buf);
          dt_txt[val_len++] = '.';
          dt_txt[val_len++] = '0' + ((original_epoch / 100) % 10);
          dt_txt[val_len++] = '0' + ((original_epoch / 10) % 10);
          dt_txt[val_len++] = '0' + (original_epoch % 10);
          dt_txt[val_len] = 0;
        }
        if (val_len != sql_val_len) {
          printf("Val len mismatch: nid: %u, seq: %lu, col: %lu, E:%lu/A:%lu\n", node_id, ins_seq_id, col_val_idx, sql_val_len, val_len);
          printf("Expected: [%s]\n", (sql_val == nullptr ? "nullptr" : (const char *) sql_val));
          printf("Found: [%.*s]\n", (int) val_len, dt_txt);
        } else {
          if (memcmp(sql_val, dt_txt, val_len) != 0) {
            printf("Val mismatch: nid: %u, seq: %lu, col: %lu, E:%lu/A:%lu\n", node_id, ins_seq_id, col_val_idx, sql_val_len, val_len);
            printf("Expected: [%s]\n", (sql_val == nullptr ? "nullptr" : (const char *) sql_val));
            printf("Found: [%.*s]\n", (int) val_len, dt_txt);
          }
        }
      } else if (exp_col_type == MST_INT) {
        int64_t sql_val = sqlite3_column_int64(stmt, sql_col_idx);
        uint8_t val_buf[16];
        size_t val_len = 8;
        const uint8_t *ret_buf = stm.get_col_val(node_id, col_val_idx, &val_len, val_buf); // , &ptr_count[col_val_idx]);
        int64_t i64 = *((int64_t *) val_buf);
        if (i64 != sql_val)
          std::cerr << "Int not matching: nid:" << node_id << ", seq:" << ins_seq_id << ", col:" << col_val_idx << " - e" << sql_val << ":a" << i64 << std::endl;
        int_sums[col_val_idx] += i64;
      } else if (exp_col_type >= MST_DEC0 && exp_col_type <= MST_DEC9) {
        double sql_val = round_dbl(sqlite3_column_double(stmt, i), exp_col_type);
        uint8_t val_buf[16];
        size_t val_len;
        const uint8_t *ret_buf = stm.get_col_val(node_id, col_val_idx, &val_len, val_buf); // , &ptr_count[col_val_idx]);
        double dbl_val = *((double *) val_buf);
        if (dbl_val != sql_val)
          std::cerr << "Dbl not matching: nid:" << node_id << ", seq:" << ins_seq_id << ", col:" << col_val_idx << " - e" << sql_val << ":a" << dbl_val << std::endl;
        dbl_sums[col_val_idx] += dbl_val;
      }
      col_val_idx++;
    }
    ins_seq_id++;
  }
  printf("Totals:");
  for (int i = 0; i < stm_col_count; i++) {
    printf(" %s:", stm.get_column_name(i));
    if (int_sums[i] != 0)
      printf(" %" PRId64, int_sums[i]);
    if (dbl_sums[i] != 0)
      printf(" %f", dbl_sums[i]);
  }
  printf("\n");
  // encoding_types = stm.get_column_encodings();
  // for (int i = 0; i < stm_col_count; i++) {
  //   char encoding_type = encoding_types[i];
  //   if (encoding_type == 'T') {
  //     madras_dv1::static_trie_map stm_ct = stm.get_col_trie_map(i);
  //     printf("%s\n", stm_ct.get_table_name());
  //   }
  // }
  delete [] key;

  sqlite3_finalize(stmt_col_names);
  sqlite3_finalize(stmt);
  sqlite3_close(db);

  t = print_time_taken(t, "Time taken for verification: ");

  return 0;

}
