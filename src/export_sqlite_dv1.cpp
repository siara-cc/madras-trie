#include <iostream>
#include <string>
#include <sqlite3.h>
#include <sys/stat.h>

#include "../../ds_common/src/gen.hpp"

#include "madras_dv1.hpp"
#include "madras_builder_dv1.hpp"

double time_taken_in_secs(clock_t t) {
  t = clock() - t;
  return ((double)t)/CLOCKS_PER_SEC;
}

clock_t print_time_taken(clock_t t, const char *msg) {
  double time_taken = time_taken_in_secs(t); // in seconds
  std::cout << msg << time_taken << std::endl;
  return clock();
}

void export_key_and_column0(madras_dv1::builder& bldr, sqlite3_stmt *stmt, int sql_col_idx,
    int exp_col_idx, int exp_col_type, int key_col_idx, char key_data_type, int row_count) {
  int rc;
  int64_t ins_seq_id = 0;
  uint8_t key_buf[10];
  while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
    uint8_t *key;
    int key_len;
    uint8_t converted_val[10];
    switch (key_data_type) {
      case LPDT_TEXT:
        key = (uint8_t *) sqlite3_column_text(stmt, key_col_idx - 1);
        key_len = sqlite3_column_bytes(stmt, key_col_idx - 1);
        break;
      case LPDT_BIN:
        key = (uint8_t *) sqlite3_column_blob(stmt, key_col_idx - 1);
        key_len = sqlite3_column_bytes(stmt, key_col_idx - 1);
        break;
      default: {
        int64_t ki64;
        double kd64;
        if (key_data_type == LPDT_S64_INT || key_data_type == LPDT_U64_INT) {
          ki64 = sqlite3_column_int64(stmt, key_col_idx - 1);
          key = leopard::cmn::convert(&ki64, 8, converted_val, key_len, key_data_type);
        } else {
          kd64 = sqlite3_column_double(stmt, key_col_idx - 1);
          key = leopard::cmn::convert(&kd64, 8, converted_val, key_len, key_data_type);
        }
      }
    }
    const void *val = NULL;
    int val_len = 8;
    int64_t s64;
    double dbl;
    if (sqlite3_column_type(stmt, sql_col_idx) == SQLITE_NULL) {
      val = NULL;
      val_len = 0;
    } else if (exp_col_type == LPDT_TEXT) {
      val = sqlite3_column_text(stmt, sql_col_idx);
      val_len = sqlite3_column_bytes(stmt, sql_col_idx);
    } else if (exp_col_type == LPDT_BIN) {
      val = sqlite3_column_blob(stmt, sql_col_idx);
      val_len = sqlite3_column_bytes(stmt, sql_col_idx);
    } else if (exp_col_type == LPDT_S64_INT || exp_col_type == LPDT_U64_INT) {
      s64 = sqlite3_column_int64(stmt, sql_col_idx);
      val = &s64;
    } else if ((exp_col_type >= LPDT_S64_DEC1 && exp_col_type <= LPDT_S64_DEC9) ||
               (exp_col_type >= LPDT_U64_DEC1 && exp_col_type <= LPDT_U64_DEC9) ||
               (exp_col_type >= LPDT_U15_DEC1 && exp_col_type <= LPDT_U15_DEC2)) {
      dbl = sqlite3_column_double(stmt, sql_col_idx);
      val = &dbl;
    }
    bldr.insert(key, key_len, val, val_len);
    if (ins_seq_id >= row_count)
      break;
    ins_seq_id++;
  }
}

void export_column(madras_dv1::builder& bldr, sqlite3_stmt *stmt,
    int sql_col_idx, int exp_col_idx, int exp_col_type, int row_count) {
  int rc;
  const char *null_val = "";
  int64_t ins_seq_id = 0;
  int null_count = 0;
  int zero_count = 0;
  while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
    const void *val;
    int val_len = 8;
    int64_t s64;
    double dbl;
    if (sqlite3_column_type(stmt, sql_col_idx) == SQLITE_NULL) {
      null_count++;
      val = NULL;
      val_len = 0;
    } else if (exp_col_type == LPDT_TEXT || exp_col_type == LPDT_WORDS) {
      val = sqlite3_column_text(stmt, sql_col_idx);
      val_len = sqlite3_column_bytes(stmt, sql_col_idx);
    } else if (exp_col_type == LPDT_BIN) {
      val = sqlite3_column_blob(stmt, sql_col_idx);
      val_len = sqlite3_column_bytes(stmt, sql_col_idx);
    } else if (exp_col_type == LPDT_S64_INT || exp_col_type == LPDT_U64_INT) {
      s64 = sqlite3_column_int64(stmt, sql_col_idx);
      if (s64 == 0)
        zero_count++;
      val = &s64;
    } else if ((exp_col_type >= LPDT_S64_DEC1 && exp_col_type <= LPDT_S64_DEC9) ||
               (exp_col_type >= LPDT_U64_DEC1 && exp_col_type <= LPDT_U64_DEC9) ||
               (exp_col_type >= LPDT_U15_DEC1 && exp_col_type <= LPDT_U15_DEC2)) {
      dbl = sqlite3_column_double(stmt, sql_col_idx);
      if (dbl == 0)
        zero_count++;
      val = &dbl;
    }
    bool is_success = bldr.insert_col_val(val, val_len);
    if (!is_success) {
      std::cerr << "Error inserting into builder" << std::endl;
      return;
    }
    if (ins_seq_id >= row_count)
      break;
    ins_seq_id++;
  }
  printf("NULL count: %d, 0 count: %d\n", null_count, zero_count);
}

int main(int argc, char* argv[]) {

  sqlite3 *db;
  sqlite3_stmt *stmt;
  sqlite3_stmt *stmt_col_names;
  int rc;
  if (argc < 6) {
    std::cout << std::endl;
    std::cout << "Usage: export_sqlite <db_file> <table_or_select> <key_col_idx> <storage_types> [row_count]" << std::endl;
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

  rc = sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt_col_names, NULL);
  if (rc != SQLITE_OK) {
    std::cerr << "SQL error: " << sqlite3_errmsg(db) << std::endl;
    sqlite3_close(db);
    return 1;
  }
  sqlite3_step(stmt_col_names);

  int column_count = sqlite3_column_count(stmt_col_names);
  const char *storage_types = argv[4];
  if (column_count < strlen(storage_types)) {
    std::cerr << "Storage types not specified for all columns" << std::endl;
    sqlite3_finalize(stmt_col_names);
    sqlite3_close(db);
    return 1;
  }
  const char *encoding_types = argv[5];

  int key_col_idx = atoi(argv[3]);
  std::string column_names = table_name;
  if (key_col_idx != 0) {
    column_names.append(",");
    column_names.append(sqlite3_column_name(stmt_col_names, key_col_idx - 1));
  }

  int row_count = INT_MAX;
  if (argc > 6)
    row_count = atoi(argv[6]);

  std::string col_types;
  std::string col_encodings;
  int exp_col_count = 0;
  if (key_col_idx != 0) {
    col_types.append(1, storage_types[key_col_idx - 1]);
    col_encodings.append(1, encoding_types[key_col_idx - 1]);
    exp_col_count++;
  }
  for (int i = 0; i < column_count; i++) {
    if (storage_types[i] == '-' || i == (key_col_idx - 1))
      continue;
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

  rc = sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, NULL);
  if (rc != SQLITE_OK) {
    std::cerr << "SQL error: " << sqlite3_errmsg(db) << std::endl;
    sqlite3_finalize(stmt_col_names);
    sqlite3_close(db);
    return 1;
  }

  std::string out_file = argv[1];
  out_file += ".mdx";
  madras_dv1::builder mb(out_file.c_str(), column_names.c_str(), exp_col_count, 
      col_types.c_str(), col_encodings.c_str(), true, key_col_idx == 0);
  mb.set_print_enabled();
  mb.open_file();

  int exp_col_idx = 0;
  for (int i = 0; i < column_count; i++) {
    if (storage_types[i] == '-' || i == (key_col_idx - 1))
      continue;
    if (exp_col_idx == 0 && key_col_idx != 0) {
      export_key_and_column0(mb, stmt, i, exp_col_idx, storage_types[i],
              key_col_idx, storage_types[key_col_idx - 1], row_count);
      mb.write_kv();
    } else {
      mb.reset_for_next_col();
      export_column(mb, stmt, i, exp_col_idx, storage_types[i], row_count);
      if (exp_col_idx == 0)
        mb.write_kv();
      else
        mb.build_and_write_col_val();
    }
    sqlite3_reset(stmt);
    exp_col_idx++;
  }
  mb.write_final_val_table();

  t = print_time_taken(t, "Time taken for build: ");

  madras_dv1::static_dict sd;
  sd.load(out_file.c_str());
  printf("Tbl name: %s\n", sd.get_table_name());
  printf("Col types: %s\n", sd.get_column_types());
  printf("Col encodings: %s\n", sd.get_column_encodings());
  uint16_t sd_col_count = sd.get_column_count();
  printf("Col Count: %u, Cols:", sd_col_count);
  for (int i = 0; i < sd_col_count; i++)
    printf(" %s", sd.get_column_name(i));
  printf("\n");

  if (dont_verify) {
    sqlite3_finalize(stmt_col_names);
    sqlite3_finalize(stmt);
    sqlite3_close(db);
    return 1;
  }

  sqlite3_reset(stmt);
  int64_t ins_seq_id = 0;
  uint8_t *key = new uint8_t[sd.max_key_len];
  uint32_t ptr_count[column_count];
  int64_t int_sums[column_count];
  double dbl_sums[column_count];
  memset(ptr_count, '\xFF', sizeof(uint32_t) * column_count);
  memset(int_sums, '\0', sizeof(int64_t) * column_count);
  memset(dbl_sums, '\0', sizeof(double) * column_count);
  while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
    uint32_t node_id = mb.get_node_id_from_sequence(ins_seq_id);
    int col_val_idx = 0;
    if (key_col_idx > 0) {
      const uint8_t *sql_key = (const uint8_t *) sqlite3_column_blob(stmt, key_col_idx - 1);
      int sql_key_len = sqlite3_column_bytes(stmt, key_col_idx - 1);
      int key_len;
      bool is_found = sd.reverse_lookup_from_node_id(node_id, &key_len, key);
      if (sql_key == NULL && key_len == -1) {
        // Ok
      } else if (key_len != sql_key_len)
        std::cerr << "Key len not matching: " << node_id << ", " << ins_seq_id << ", " << sql_key_len << ":" << key_len << std::endl;
      else {
        if (memcmp(key, sql_key, key_len) != 0)
          std::cerr << "Key not matching" << node_id << ", " << ins_seq_id << std::endl;
      }
    }
    for (int i = 0; i < column_count; i++) {
      char exp_col_type = storage_types[i];
      if (exp_col_type == '-')
        continue;
      if (i == (key_col_idx - 1))
        continue;
      if (sqlite3_column_type(stmt, i) == SQLITE_NULL) {
        uint8_t val_buf[sd.max_val_len];
        int val_len = 8;
        bool is_success = sd.get_col_val(node_id, col_val_idx, &val_len, val_buf);
        if (is_success) {
          if (val_len != -1) {
            val_buf[val_len] = 0;
            std::cout << "Val not null: " << node_id << ", " << col_val_idx << ": " << val_buf << std::endl;
          }
        }
      } else
      if (exp_col_type == LPDT_TEXT || exp_col_type == LPDT_BIN || exp_col_type == LPDT_WORDS) {
        const uint8_t *sql_val = (const uint8_t *) sqlite3_column_blob(stmt, i);
        int sql_val_len = sqlite3_column_bytes(stmt, i);
        int val_len = sd.get_max_val_len(col_val_idx) + 1;
        uint8_t val_buf[val_len];
        bool is_success = sd.get_col_val(node_id, col_val_idx, &val_len, val_buf, &ptr_count[col_val_idx]);
        if (is_success) {
          if (val_len == -1 && sql_val == NULL) {
            // NULL value
          } else {
            val_buf[val_len] = '\0';
            if (val_len != sql_val_len) {
              std::cout << "Val len mismatch: " << node_id << ", " << col_val_idx << " - " << ": " << val_len << ": " << sql_val_len << std::endl;
              std::cout << "Expected: " << sql_val << std::endl;
              std::cout << "Found: " << val_buf << std::endl;
            } else {
              if (memcmp(sql_val, val_buf, val_len) != 0) {
                std::cout << "Val not matching: " << node_id << ", " << col_val_idx << std::endl;
                std::cout << "Expected: " << sql_val << std::endl;
                std::cout << "Found: " << val_buf << std::endl;
              }
            }
          }
        }
      } else if (exp_col_type == LPDT_S64_INT || exp_col_type == LPDT_U64_INT) {
        int64_t sql_val = sqlite3_column_int64(stmt, i);
        uint8_t val[16];
        int val_len = 8;
        bool is_success = sd.get_col_val(node_id, col_val_idx, &val_len, val, &ptr_count[col_val_idx]);
        if (is_success) {
          int64_t i64 = *((int64_t *) val);
          if (i64 != sql_val)
            std::cerr << "Val not matching: " << node_id << ", " << ins_seq_id << ", " << col_val_idx << " - " << sql_val << ":" << i64 << std::endl;
          int_sums[col_val_idx] += i64;
        } else
          std::cerr << "Val not found: " << node_id << ", " << ins_seq_id << ", " << sql_val << std::endl;
      } else if ((exp_col_type >= LPDT_S64_DEC1 && exp_col_type <= LPDT_S64_DEC9) ||
                 (exp_col_type >= LPDT_U64_DEC1 && exp_col_type <= LPDT_U64_DEC9) ||
                 (exp_col_type >= LPDT_U15_DEC1 && exp_col_type <= LPDT_U15_DEC2)) {
        double sql_val = leopard::cmn::round(sqlite3_column_double(stmt, i), exp_col_type);
        uint8_t val[16];
        int val_len;
        bool is_success = sd.get_col_val(node_id, col_val_idx, &val_len, val);
        if (is_success) {
          double dbl_val = *((double *) val);
          if (dbl_val != sql_val)
            std::cerr << "Val not matching: " << node_id << ", " << ins_seq_id << ", " << col_val_idx << " - " << sql_val << ":" << dbl_val << std::endl;
          dbl_sums[col_val_idx] += dbl_val;
        } else
          std::cerr << "Val not found: " << node_id << ", " << ins_seq_id << ", " << col_val_idx << " = " << sql_val << std::endl;
      }
      col_val_idx++;
    }
    if (ins_seq_id >= row_count)
      break;
    ins_seq_id++;
  }
  printf("Totals:");
  for (int i = 0; i < sd_col_count; i++) {
    printf(" %s: ", sd.get_column_name(i));
    if (int_sums[i] != 0)
      printf(" %lld", int_sums[i]);
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
