#include <iostream>
#include <string>
#include <sqlite3.h>

#include "madras_builder_dv1.hpp"

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
      int8_t vlen = leopard::gen::get_svint60_len(ins_seq_id);
      leopard::gen::copy_svint60(ins_seq_id, key, vlen);
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
    } else if (exp_col_type == LPDT_S64_INT) {
      s64 = sqlite3_column_int64(stmt, sql_col_idx);
      val = &s64;
    } else if (exp_col_type >= LPDT_S64_DEC1 && exp_col_type <= LPDT_S64_DEC9) {
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
    } else if (exp_col_type == LPDT_S64_INT) {
      s64 = sqlite3_column_int64(stmt, sql_col_idx);
      val = &s64;
    } else if (exp_col_type >= LPDT_S64_DEC1 && exp_col_type <= LPDT_S64_DEC9) {
      dbl = sqlite3_column_double(stmt, sql_col_idx);
      val = &dbl;
    }
    bool is_success = bldr.memtrie.insert_col_val(val, val_len);
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
  if (argc < 5) {
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
    std::cout << std::endl;
    return 1;
  }

  rc = sqlite3_open(argv[1], &db);
  if (rc) {
    std::cerr << "Can't open database: " << sqlite3_errmsg(db) << std::endl;
    return 1;
  }

  std::string sql;
  if (sqlite3_strnicmp(argv[2], "select ", 7) == 0) {
    sql = argv[2];
  } else {
    sql = "SELECT * FROM ";
    sql += argv[2];
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

  int key_col_idx = atoi(argv[3]);
  std::string column_names;
  if (key_col_idx == 0)
    column_names.append("key");
  else
    column_names.append(sqlite3_column_name(stmt_col_names, key_col_idx - 1));

  std::string value_types;
  int exp_col_count = 0;
  for (int i = 0; i < columnCount; i++) {
    if (storage_types[i] == '-' || storage_types[i] == '^')
      continue;
    const char* column_name = sqlite3_column_name(stmt_col_names, i);
    column_names.append(",");
    column_names.append(column_name);
    value_types.append(1, storage_types[i]);
    exp_col_count++;
  }

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
  madras_dv1::builder mb(out_file.c_str(), column_names.c_str(), exp_col_count, "d", value_types.c_str());
  mb.set_print_enabled();

  int exp_col_idx = 0;
  for (int i = 0; i < columnCount; i++) {
    if (storage_types[i] == '-' || storage_types[i] == '^')
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

  sqlite3_finalize(stmt_col_names);
  sqlite3_finalize(stmt);
  sqlite3_close(db);

  return 0;

}
