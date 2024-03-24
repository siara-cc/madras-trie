#include <iostream>
#include <string>
#include <sqlite3.h>
#include <sys/stat.h>

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

void export_key_and_column0(madras_dv1::builder& bldr, sqlite3_stmt *stmt,
    int sql_col_idx, int exp_col_idx, int exp_col_type, int key_col_idx) {
  int rc;
  int64_t ins_seq_id = 0;
  uint8_t key_buf[10];
  while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
    uint8_t *key;
    int key_len;
    if (key_col_idx == 0) {
      key = key_buf;
      int8_t vlen = leopard::gen::get_svint61_len(ins_seq_id);
      leopard::gen::copy_svint61(ins_seq_id, key, vlen);
      key_len = vlen;
    } else {
      key = (uint8_t *) sqlite3_column_blob(stmt, key_col_idx - 1);
      key_len = sqlite3_column_bytes(stmt, key_col_idx - 1);
    }
    const void *val;
    int val_len = 8;
    int64_t s64;
    double dbl;
    if (exp_col_type == LPDT_TEXT || exp_col_type == LPDT_BIN) {
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
    if (key_len == 0) {
      bldr.insert((const uint8_t *) "", 1, val, val_len);
    } else {
      bldr.insert(key, key_len, val, val_len);
    }
    ins_seq_id++;
  }
}

void export_column(madras_dv1::builder& bldr, sqlite3_stmt *stmt,
    int sql_col_idx, int exp_col_idx, int exp_col_type) {
  int rc;
  const char *null_val = "";
  int64_t ins_seq_id = 0;
  uint8_t key_buf[10];
  while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
    const void *val;
    int val_len = 8;
    int64_t s64;
    double dbl;
    if (exp_col_type == LPDT_TEXT || exp_col_type == LPDT_BIN) {
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
    if (val == NULL) {
      val = null_val;
      val_len = 1;
    }
    bool is_success = bldr.insert_col_val(val, val_len);
    if (!is_success) {
      std::cerr << "Error inserting into builder" << std::endl;
      return;
    }
    ins_seq_id++;
  }
}

int main(int argc, char* argv[]) {

  sqlite3 *db;
  sqlite3_stmt *stmt;
  sqlite3_stmt *stmt_col_names;
  int rc;
  if (argc < 6) {
    std::cout << std::endl;
    std::cout << "Usage: export_sqlite <db_file> <table_or_select> <key_col_idx> <storage_types>" << std::endl;
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
  std::string sql;
  if (sqlite3_strnicmp(argv[2], "select ", 7) == 0) {
    sql = argv[2];
  } else {
    sql = "SELECT * FROM ";
    sql += argv[2];
    table_name = argv[2];
  }

  rc = sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt_col_names, NULL);
  if (rc != SQLITE_OK) {
    std::cerr << "SQL error: " << sqlite3_errmsg(db) << std::endl;
    sqlite3_close(db);
    return 1;
  }
  sqlite3_step(stmt_col_names);

  int columnCount = sqlite3_column_count(stmt_col_names);
  const char *storage_types = argv[4];
  if (columnCount < strlen(storage_types)) {
    std::cerr << "Storage types not specified for all columns" << std::endl;
    sqlite3_finalize(stmt_col_names);
    sqlite3_close(db);
    return 1;
  }
  const char *encoding_types = argv[5];

  int key_col_idx = atoi(argv[3]);
  std::string column_names = table_name;
  if (key_col_idx == 0)
    column_names.append(",key");
  else {
    column_names.append(",");
    column_names.append(sqlite3_column_name(stmt_col_names, key_col_idx - 1));
  }

  std::string col_types;
  std::string col_encodings;
  col_types.append(1, key_col_idx == 0 ? 'i' : storage_types[key_col_idx - 1]);
  col_encodings.append(1, key_col_idx == 0 ? 'u' : encoding_types[key_col_idx - 1]);
  int exp_col_count = 0;
  for (int i = 0; i < columnCount; i++) {
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
  madras_dv1::builder mb(out_file.c_str(), column_names.c_str(), exp_col_count, col_encodings.c_str(), col_types.c_str());
  mb.set_print_enabled();

  int exp_col_idx = 0;
  for (int i = 0; i < columnCount; i++) {
    if (storage_types[i] == '-' || i == (key_col_idx - 1))
      continue;
    if (exp_col_idx == 0) {
      export_key_and_column0(mb, stmt, i, exp_col_idx, storage_types[i], key_col_idx);
      mb.build();
    } else {
      mb.reset_for_next_col();
      export_column(mb, stmt, i, exp_col_idx, storage_types[i]);
      mb.build_and_write_col_val();
    }
    sqlite3_reset(stmt);
    exp_col_idx++;
  }
  mb.write_final_val_table();

  t = print_time_taken(t, "Time taken for build: ");

  madras_dv1::static_dict sd;
  sd.load(out_file.c_str());

  sqlite3_reset(stmt);
  int64_t ins_seq_id = 0;
  uint8_t *key = new uint8_t[sd.max_key_len];
  while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
    uint32_t node_id = mb.get_node_id_from_sequence(ins_seq_id);
    int col_val_idx = 0;
    if (key_col_idx > 0) {
      const uint8_t *sql_key = (const uint8_t *) sqlite3_column_blob(stmt, key_col_idx - 1);
      int sql_key_len = sqlite3_column_bytes(stmt, key_col_idx - 1);
      uint8_t *null_val = (uint8_t *) "";
      if (sql_key == NULL) {
        sql_key = null_val;
        sql_key_len = 1;
      }
      int key_len;
      bool is_found = sd.reverse_lookup_from_node_id(node_id, &key_len, key);
      if (key_len != sql_key_len)
        std::cerr << "Key len not matching: " << node_id << ", " << ins_seq_id << ", " << sql_key_len << ":" << key_len << std::endl;
      else {
        if (memcmp(key, sql_key, key_len) != 0)
          std::cerr << "Key not matching" << node_id << ", " << ins_seq_id << std::endl;
      }
    }
    for (int i = 0; i < columnCount; i++) {
      char exp_col_type = storage_types[i];
      if (exp_col_type == '-')
        continue;
      if (i == (key_col_idx - 1))
        continue;
      if (exp_col_type == LPDT_TEXT || exp_col_type == LPDT_BIN) {
        const uint8_t *sql_val = (const uint8_t *) sqlite3_column_blob(stmt, i);
        int sql_val_len = sqlite3_column_bytes(stmt, i);
        uint8_t val_buf[sql_val_len + 1]; // todo: allocate max_len
        int val_len;
        bool is_success = sd.get_col_val(node_id, col_val_idx, &val_len, val_buf);
        if (is_success) {
          val_buf[val_len] = '\0';
          if (val_len != sql_val_len)
            std::cout << "Val len mismatch: " << node_id << ", " << val_len << ": " << sql_val_len << std::endl;
          else {
            if (memcmp(sql_val, val_buf, val_len) != 0)
              std::cout << "Val not maching: " << node_id << ", " << sql_val << ": " << val_buf << std::endl;
          }
        }
      } else if (exp_col_type == LPDT_S64_INT || exp_col_type == LPDT_U64_INT) {
        int64_t sql_val = sqlite3_column_int64(stmt, i);
        uint8_t val[16];
        int val_len;
        bool is_success = sd.get_col_val(node_id, col_val_idx, &val_len, val);
        if (is_success) {
          int64_t i64 = exp_col_type == LPDT_S64_INT ? sd.get_val_int60(val) : (int64_t) sd.get_val_int61(val);
          if (i64 != sql_val)
            std::cerr << "Val not matching: " << node_id << ", " << ins_seq_id << ", " << col_val_idx << ", " << sql_val << ":" << i64 << std::endl;
        } else
          std::cerr << "Val not found: " << node_id << ", " << ins_seq_id << ", " << sql_val << std::endl;
      } else if ((exp_col_type >= LPDT_S64_DEC1 && exp_col_type <= LPDT_S64_DEC9) ||
                 (exp_col_type >= LPDT_U64_DEC1 && exp_col_type <= LPDT_U64_DEC9) ||
                 (exp_col_type >= LPDT_U15_DEC1 && exp_col_type <= LPDT_U15_DEC2)) {
        char base_type = (exp_col_type >= LPDT_S64_DEC1 && exp_col_type <= LPDT_S64_DEC9) ? LPDT_S64_DEC1 : LPDT_U64_DEC1;
        if (exp_col_type >= LPDT_U15_DEC1 && exp_col_type <= LPDT_U15_DEC2)
          base_type = LPDT_U15_DEC1;
        double sql_val = sqlite3_column_double(stmt, i);
        uint8_t val[16];
        int val_len;
        bool is_success = sd.get_col_val(node_id, col_val_idx, &val_len, val);
        if (is_success) {
          double dbl_val = 0;
          int int_val = 0;
          switch (base_type) {
            case LPDT_S64_DEC1 ... LPDT_S64_DEC9:
              dbl_val = sd.get_val_int60_dbl(val, exp_col_type);
              break;
            case LPDT_U64_DEC1 ... LPDT_U64_DEC9:
              dbl_val = sd.get_val_int61_dbl(val, exp_col_type);
              break;
            case LPDT_U15_DEC1 ... LPDT_U15_DEC2:
              dbl_val = sd.get_val_int15_dbl(val, exp_col_type);
              break;
          }
          if (dbl_val != sql_val)
            std::cerr << "Val not matching: " << node_id << ", " << ins_seq_id << ", " << col_val_idx << ", " << sql_val << ":" << dbl_val << ":" << int_val << std::endl;
        } else
          std::cerr << "Val not found: " << node_id << ", " << ins_seq_id << ", " << sql_val << std::endl;
      }
      col_val_idx++;
    }
    ins_seq_id++;
  }
  delete [] key;

  sqlite3_finalize(stmt_col_names);
  sqlite3_finalize(stmt);
  sqlite3_close(db);

  t = print_time_taken(t, "Time taken for verification: ");

  return 0;

}
