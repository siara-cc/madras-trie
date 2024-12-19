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

const static char *dt_formats[] = {"%Y-%m-%dT%H:%M:%S", "%d-%m-%Y", "%d-%m-%Y %H:%M:%S", "%m-%d-%Y", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%m-%d-%Y %H:%M:%S"};
const static size_t dt_format_lens[] = {19, 8, 19, 8, 8, 19, 19, 19, 19};

double time_taken_in_secs(clock_t t) {
  t = clock() - t;
  return ((double)t)/CLOCKS_PER_SEC;
}

clock_t print_time_taken(clock_t t, const char *msg) {
  double time_taken = time_taken_in_secs(t); // in seconds
  std::cout << msg << time_taken << std::endl;
  return clock();
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

  time_t t = clock();

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

  int column_count = sqlite3_column_count(stmt_col_names);
  const char *storage_types = argv[4];
  if (column_count > strlen(storage_types)) {
    std::cerr << "Storage types not specified for all columns" << std::endl;
    sqlite3_finalize(stmt_col_names);
    sqlite3_close(db);
    return 1;
  }
  const char *encoding_types = argv[5];

  int key_col_sql_idx = atoi(argv[3]);
  std::string column_names = table_name;

  int row_count = INT_MAX;
  if (argc > 6)
    row_count = atoi(argv[6]);

  std::string col_types;
  std::string col_encodings;
  int exp_col_count = 0;
  int key_col_idx = key_col_sql_idx - 1;
  for (int i = 0; i < column_count; i++) {
    if (storage_types[i] == '-') {
      if ((key_col_sql_idx - 1) > i)
        key_col_idx--;
      continue;
    }
    const char* column_name = sqlite3_column_name(stmt_col_names, i);
    column_names.append(",");
    column_names.append(column_name);
    col_types.append(1, storage_types[i]);
    col_encodings.append(1, encoding_types[i]);
    exp_col_count++;
  }
  printf("Count: %d, Table/Key/Column names: %s, types: %s, encodings: %s\n",
    exp_col_count, column_names.c_str(), col_types.c_str(), col_encodings.c_str());

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
  madras_dv1::builder mb(out_file.c_str(), column_names.c_str(), exp_col_count, 
      col_types.c_str(), col_encodings.c_str(), 0, key_col_sql_idx == 0,
      bldr_opts);
  mb.set_print_enabled();
  mb.open_file();

  size_t exp_col_idx = 0;
  size_t ins_seq_id = 0;
  uint64_t values[exp_col_count];
  double *values_dbl = (double *) values;
  size_t value_lens[exp_col_count];
  while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
    exp_col_idx = 0;
    for (size_t i = 0; i < column_count; i++) {
      if (storage_types[i] == '-')
        continue;
      size_t sql_col_idx = i;
      char exp_col_type = storage_types[i];
      int64_t s64;
      double dbl;
      if (sqlite3_column_type(stmt, sql_col_idx) == SQLITE_NULL) {
        values[exp_col_idx] = INT64_MAX;
        value_lens[exp_col_idx] = 0;
      } else if (exp_col_type == DCT_TEXT || exp_col_type == DCT_WORDS) {
        values[exp_col_idx] = (uint64_t) sqlite3_column_text(stmt, sql_col_idx);
        value_lens[exp_col_idx] = sqlite3_column_bytes(stmt, sql_col_idx);
      } else if (exp_col_type == DCT_BIN) {
        values[exp_col_idx] = (uint64_t) sqlite3_column_blob(stmt, sql_col_idx);
        value_lens[exp_col_idx] = sqlite3_column_bytes(stmt, sql_col_idx);
      } else if (exp_col_type >= DCT_DATETIME_ISOT && exp_col_type <= DCT_DATETIME_US) {
        struct tm tm = {0};
        const uint8_t *dt_txt_db = sqlite3_column_text(stmt, sql_col_idx);
        char dt_txt[dt_format_lens[exp_col_type - DCT_DATETIME_ISOT] + 1];
        strncpy(dt_txt, (const char *) dt_txt_db, dt_format_lens[exp_col_type - DCT_DATETIME_ISOT]);
        dt_txt[dt_format_lens[exp_col_type - DCT_DATETIME_ISOT]] = 0;
        // printf("%s, %s\n", dt_txt, dt_formats[exp_col_type - DCT_DATETIME_ISOT]);
        char *result = strptime((const char *) dt_txt, dt_formats[exp_col_type - DCT_DATETIME_ISOT], &tm);
        if (result == nullptr || *result != '\0') {
          printf(" e%lu/%lu", ins_seq_id, i);
          values[exp_col_idx] = INT64_MAX;
        } else {
          uint64_t dt_val = (uint64_t) mktime(&tm);
          if (exp_col_type >= DCT_DATE_EUR && exp_col_type <= DCT_DATE_ISO)
            dt_val /= 86400;
          if (exp_col_type == DCT_DATETIME_MS || exp_col_type == DCT_DATETIME_ISO || exp_col_type == DCT_DATETIME_ISOT) {
            dt_val *= 1000;
            char *dot_pos = (char *) memchr(dt_txt_db, '.', strnlen((const char *) dt_txt_db, 24));
            if (dot_pos != nullptr)
              dt_val += atoi(dot_pos + 1);
          }
          values[exp_col_idx] = dt_val;
        }
        value_lens[exp_col_idx] = 8;
      } else if (exp_col_type == DCT_S64_INT || exp_col_type == DCT_U64_INT) {
        s64 = sqlite3_column_int64(stmt, sql_col_idx);
        memcpy(values + exp_col_idx, &s64, 8);
        value_lens[exp_col_idx] = 8;
      } else if ((exp_col_type >= DCT_S64_DEC1 && exp_col_type <= DCT_S64_DEC9) ||
                (exp_col_type >= DCT_U64_DEC1 && exp_col_type <= DCT_U64_DEC9) ||
                (exp_col_type >= DCT_U15_DEC1 && exp_col_type <= DCT_U15_DEC2)) {
        values_dbl[exp_col_idx] = sqlite3_column_double(stmt, sql_col_idx);
        value_lens[exp_col_idx] = 8;
      }
      exp_col_idx++;
    }
    mb.insert(values, value_lens, key_col_idx);
    if (ins_seq_id >= row_count)
      break;
    ins_seq_id++;
    if ((ins_seq_id % 100000) == 0) {
      std::cout << ".";
      std::flush(std::cout);
    }
  }

  std::cout << std::endl;

  sqlite3_reset(stmt);

  // Verification
  if (false) {
    exp_col_idx = 0;
    ins_seq_id = 0;
    size_t pos = 2;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
      exp_col_idx = 0;
        uint32_t pos = mb.rec_pos_vec[ins_seq_id + 1];
        size_t vlen;
        gen::read_vint32((*mb.all_vals)[pos], &vlen);
        pos += vlen;
      for (size_t i = 0; i < column_count; i++) {
        if (storage_types[i] == '-')
          continue;
          uint8_t *data_pos = (*mb.all_vals)[pos];
          size_t len_len = 0;
          uint32_t data_len = 0;
        size_t sql_col_idx = i;
        char exp_col_type = storage_types[i];
        int64_t s64, i64;
        double dbl;
        if (sqlite3_column_type(stmt, sql_col_idx) == SQLITE_NULL) {
            data_len = gen::read_vint32(data_pos, &len_len);
            data_pos += len_len;
            if (data_len != madras_dv1::NULL_VALUE_LEN || memcmp(data_pos, madras_dv1::NULL_VALUE, madras_dv1::NULL_VALUE_LEN) != 0)
              printf("Mismatch: Not null: seq: %lu, col: %lu\n", ins_seq_id, exp_col_idx);
            pos += len_len;
            pos += data_len;
        } else if (exp_col_type == DCT_TEXT || exp_col_type == DCT_WORDS) {
            uint8_t *sql_bytes = (uint8_t *) sqlite3_column_blob(stmt, sql_col_idx);
            size_t sql_val_len = sqlite3_column_bytes(stmt, sql_col_idx);
            data_len = gen::read_vint32(data_pos, &len_len);
            data_pos += len_len;
            if (sql_val_len == 0) {
              if (data_len != madras_dv1::EMPTY_VALUE_LEN || memcmp(data_pos, madras_dv1::EMPTY_VALUE, madras_dv1::EMPTY_VALUE_LEN) != 0)
                printf("Mismatch: Not empty: seq: %lu, col: %lu\n", ins_seq_id, exp_col_idx);
            } else {
              if (data_len != sql_val_len || memcmp(data_pos, sql_bytes, sql_val_len) != 0)
                printf("Mismatch: text/bin seq: %lu, col: %lu, e:%lu, a:%u\n", ins_seq_id, exp_col_idx, sql_val_len, data_len);
            }
            pos += len_len;
            pos += data_len;
        } else if (exp_col_type == DCT_S64_INT) {
            s64 = sqlite3_column_int64(stmt, sql_col_idx);
            i64 = gen::read_svint60(data_pos);
            if (s64 != i64)
              printf("Mismatch: int seq: %lu, col: %lu\n", ins_seq_id, exp_col_idx);
            data_len = gen::read_svint60_len(data_pos);
            pos += data_len;
        } else if (exp_col_type >= DCT_S64_DEC1 && exp_col_type <= DCT_S64_DEC9) {
            dbl = sqlite3_column_double(stmt, sql_col_idx);
            s64 = static_cast<int64_t>(dbl * gen::pow10(exp_col_type - DCT_S64_DEC1 + 1));
            i64 = gen::read_svint60(data_pos);
            if (s64 != i64)
              printf("Mismatch: int seq: %lu, col: %lu\n", ins_seq_id, exp_col_idx);
            data_len = gen::read_svint60_len(data_pos);
            pos += data_len;
        }
        exp_col_idx++;
      }
      if (ins_seq_id >= row_count)
        break;
      ins_seq_id++;
      if ((ins_seq_id % 100000) == 0) {
        std::cout << ".";
        std::flush(std::cout);
      }
    }
    sqlite3_reset(stmt);
  }

  mb.write_all(key_col_idx);

  t = print_time_taken(t, "Time taken for build: ");

  madras_dv1::static_trie_map stm;
  stm.load(out_file.c_str());
  printf("Tbl name: %s\n", stm.get_table_name());
  printf("Col types: %s\n", stm.get_column_types());
  printf("Col encodings: %s\n", stm.get_column_encodings());
  uint16_t stm_col_count = stm.get_column_count();
  printf("Col Count: %u, Cols:", stm_col_count);
  for (int i = 0; i < stm_col_count; i++)
    printf(" %s", stm.get_column_name(i));
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
    int col_val_idx = 0;
    if (key_col_sql_idx > 0) {
      const uint8_t *sql_key = (const uint8_t *) sqlite3_column_blob(stmt, key_col_sql_idx - 1);
      int sql_key_len = sqlite3_column_bytes(stmt, key_col_sql_idx - 1);
      in_ctx.key = (const uint8_t *) (sql_key == nullptr ? madras_dv1::NULL_VALUE : sql_key);
      in_ctx.key_len = sql_key == nullptr ? strlen((const char *) madras_dv1::NULL_VALUE) : sql_key_len;
      char key_col_type = storage_types[key_col_sql_idx - 1];
      if (key_col_type >= DCT_S64_INT && key_col_type <= DCT_S64_DEC9) {
        in_ctx.key = key_val;
        if (sql_key == nullptr) {
          key_val[0] = 0;
          in_ctx.key_len = 1;
        } else {
          int64_t i64;
          if (key_col_type == DCT_S64_INT)
            i64 = sqlite3_column_int(stmt, key_col_sql_idx - 1);
          else {
            double dbl = sqlite3_column_double(stmt, key_col_sql_idx - 1);
            i64 = static_cast<int64_t>(dbl * gen::pow10(key_col_type - DCT_S64_DEC1 + 1));
          }
          in_ctx.key_len = gen::get_svint60_len(i64);
          gen::copy_svint60(i64, key_val, in_ctx.key_len);
        }
      }
      bool is_found = stm.lookup(in_ctx);
      if (!is_found) {
        std::cout << "Key not found: nid:" << in_ctx.node_id << ", seq:" << ins_seq_id << ", len:" << sql_key_len << std::endl;
        ins_seq_id++;
        continue;
      }
      node_id = in_ctx.node_id;
    }
    for (int i = 0; i < column_count; i++) {
      char exp_col_type = storage_types[i];
      if (exp_col_type == '-')
        continue;
      if (i == (key_col_sql_idx - 1)) {
        col_val_idx++;
        continue;
      }
      if (sqlite3_column_type(stmt, i) == SQLITE_NULL) {
        uint8_t val_buf[stm.get_max_val_len()];
        size_t val_len = 8;
        bool is_success = stm.get_col_val(node_id, col_val_idx, &val_len, val_buf); // , &ptr_count[col_val_idx]);
        if (is_success) {
          if (val_len != 0 && memcmp(val_buf, madras_dv1::NULL_VALUE, madras_dv1::NULL_VALUE_LEN) != 0) {
            val_buf[val_len] = 0;
            std::cout << "Val not null: nid:" << node_id << ", seq:" << ins_seq_id << " col:" << col_val_idx << ": A:" << val_len << "/" << val_buf << std::endl;
            printf("%d, %d\n", val_buf[0], val_buf[1]);
          }
        }
      } else
      if (exp_col_type == DCT_TEXT || exp_col_type == DCT_BIN || exp_col_type == DCT_WORDS) {
        const uint8_t *sql_val = (const uint8_t *) sqlite3_column_blob(stmt, i);
        size_t sql_val_len = sqlite3_column_bytes(stmt, i);
        size_t val_len = stm.get_max_val_len(col_val_idx) + 1;
        uint8_t val_buf[val_len];
        bool is_success = stm.get_col_val(node_id, col_val_idx, &val_len, val_buf); // , &ptr_count[col_val_idx]);
        if (is_success) {
          if (val_len > 0)
            val_buf[val_len] = '\0';
          if (sql_val_len == 0) {
            sql_val = madras_dv1::EMPTY_VALUE;
            sql_val_len = 1;
          }
          if (val_len != sql_val_len) {
            std::cout << "Val len mismatch: nid:" << node_id << ", seq:" << ins_seq_id << ", col:" << col_val_idx << " - e" << sql_val_len << ": a" << val_len << std::endl;
            std::cout << "Expected: " << (sql_val == nullptr ? "nullptr" : (const char *) sql_val) << std::endl;
            std::cout << "Found: " << val_buf << std::endl;
          } else {
            if (memcmp(sql_val, val_buf, val_len) != 0) {
              std::cout << "Val not matching: " << node_id << ", seq:" << ins_seq_id << ", " << col_val_idx << " - e" << sql_val_len << ": a" << val_len << std::endl;
              std::cout << "Expected: " << (sql_val == nullptr ? "nullptr" : (const char *) sql_val) << std::endl;
              std::cout << "Found: " << val_buf << std::endl;
            }
          }
        }
      } else if (exp_col_type >= DCT_DATETIME_ISOT && exp_col_type <= DCT_DATETIME_US) {
        const uint8_t *sql_val = (const uint8_t *) sqlite3_column_blob(stmt, i);
        size_t sql_val_len = sqlite3_column_bytes(stmt, i);
        uint8_t val[16];
        size_t val_len = 8;
        bool is_success = stm.get_col_val(node_id, col_val_idx, &val_len, val); // , &ptr_count[col_val_idx]);
        if (is_success) {
          time_t original_epoch = *((int64_t *) val);
          if (exp_col_type >= DCT_DATE_EUR && exp_col_type <= DCT_DATE_ISO)
            original_epoch *= 86400;
          if (exp_col_type == DCT_DATETIME_MS || exp_col_type == DCT_DATETIME_ISO || exp_col_type == DCT_DATETIME_ISOT)
            original_epoch /= 1000;
          char dt_txt[50];
          strftime(dt_txt, sizeof(dt_txt), dt_formats[exp_col_type - DCT_DATETIME_ISOT], localtime(&original_epoch));
          val_len = strlen(dt_txt);
          if (exp_col_type == DCT_DATETIME_MS || exp_col_type == DCT_DATETIME_ISO || exp_col_type == DCT_DATETIME_ISOT) {
            original_epoch = *((int64_t *) val);
            dt_txt[val_len++] = '.';
            dt_txt[val_len++] = '0' + ((original_epoch / 100) % 10);
            dt_txt[val_len++] = '0' + ((original_epoch / 10) % 10);
            dt_txt[val_len++] = '0' + (original_epoch % 10);
            dt_txt[val_len] = 0;
          }
          if (val_len != sql_val_len) {
            std::cout << "Val len mismatch: nid:" << node_id << ", seq:" << ins_seq_id << ", col:" << col_val_idx << " - e" << sql_val_len << ": a" << val_len << std::endl;
            std::cout << "Expected: " << (sql_val == nullptr ? "nullptr" : (const char *) sql_val) << std::endl;
            std::cout << "Found: " << dt_txt << std::endl;
          } else {
            if (memcmp(sql_val, dt_txt, val_len) != 0) {
              std::cout << "Val not matching: " << node_id << ", seq:" << ins_seq_id << ", " << col_val_idx << " - e" << sql_val_len << ": a" << val_len << std::endl;
              std::cout << "Expected: " << (sql_val == nullptr ? "nullptr" : (const char *) sql_val) << std::endl;
              std::cout << "Found: " << dt_txt << std::endl;
            }
          }
        } else
          std::cerr << "Date/time not found: nid:" << node_id << ", seq:" << ins_seq_id << ", E:" << sql_val << std::endl;
      } else if (exp_col_type == DCT_S64_INT || exp_col_type == DCT_U64_INT) {
        int64_t sql_val = sqlite3_column_int64(stmt, i);
        uint8_t val[16];
        size_t val_len = 8;
        bool is_success = stm.get_col_val(node_id, col_val_idx, &val_len, val); // , &ptr_count[col_val_idx]);
        if (is_success) {
          int64_t i64 = *((int64_t *) val);
          if (i64 != sql_val)
            std::cerr << "Int not matching: nid:" << node_id << ", seq:" << ins_seq_id << ", col:" << col_val_idx << " - e" << sql_val << ":a" << i64 << std::endl;
          int_sums[col_val_idx] += i64;
        } else
          std::cerr << "Int not found: nid:" << node_id << ", seq:" << ins_seq_id << ", E:" << sql_val << std::endl;
      } else if ((exp_col_type >= DCT_S64_DEC1 && exp_col_type <= DCT_S64_DEC9) ||
                 (exp_col_type >= DCT_U64_DEC1 && exp_col_type <= DCT_U64_DEC9) ||
                 (exp_col_type >= DCT_U15_DEC1 && exp_col_type <= DCT_U15_DEC2)) {
        double sql_val = madras_dv1::bldr_util::round(sqlite3_column_double(stmt, i), exp_col_type);
        uint8_t val[16];
        size_t val_len;
        bool is_success = stm.get_col_val(node_id, col_val_idx, &val_len, val); // , &ptr_count[col_val_idx]);
        if (is_success) {
          double dbl_val = *((double *) val);
          if (dbl_val != sql_val)
            std::cerr << "Dbl not matching: nid:" << node_id << ", seq:" << ins_seq_id << ", col:" << col_val_idx << " - e" << sql_val << ":a" << dbl_val << std::endl;
          dbl_sums[col_val_idx] += dbl_val;
        } else
          std::cerr << "Dbl not found: nid:" << node_id << ", seq:" << ins_seq_id << ", col:" << col_val_idx << " = e" << sql_val << std::endl;
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
  delete [] key;

  sqlite3_finalize(stmt_col_names);
  sqlite3_finalize(stmt);
  sqlite3_close(db);

  t = print_time_taken(t, "Time taken for verification: ");

  return 0;

}
