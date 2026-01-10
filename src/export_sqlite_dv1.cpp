#include <iostream>
#include <cstring>
#include <time.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <limits.h>
#include <inttypes.h>
#include <sys/stat.h>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>

#include <sqlite3.h>
#include <duckdb.h>

#include "ds_common/src/gen.hpp"

#include "static_trie_map.hpp"
#include "madras_builder.hpp"

#define DO_IMPORT 'i'
#define DO_VERIFY 'v'
#define DO_BOTH 'b'

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
  if (data_type == MST_DECV)
    return input;
  double p10 = gen::pow10(data_type - MST_DEC0);
  int64_t i64 = static_cast<int64_t>(input * p10);
  double ret = i64;
  ret /= p10;
  return ret;
}

class data_iface {
  public:
    virtual int init(const char *filename, const char *table_name, const char *sql) = 0;
    virtual int col_count() = 0;
    virtual const char *col_name(size_t col_idx) = 0;
    virtual int col_type(size_t col_idx) = 0;
    virtual bool next() = 0;
    virtual bool is_null(size_t col_idx) = 0;
    virtual void populate_values(madras_dv1::mdx_val_in *values, size_t *value_lens, size_t imp_col_idx, char imp_col_type, char encoding_type, size_t ins_seq_id, size_t sql_col_idx) = 0;
    virtual int64_t get_i64(size_t col_idx) = 0;
    virtual double get_dbl(size_t col_idx) = 0;
    virtual const uint8_t *get_text_bin(size_t col_idx, size_t &col_len) = 0;
    virtual void reset() = 0;
    virtual void close() = 0;
    virtual ~data_iface() {}
    static data_iface *get_data_reader(const char *input_type);
    static bool is_white_space(char c) {
      return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v';
    }
};

class sqlite_reader : public data_iface {
  private:
    sqlite3 *db = nullptr;
    sqlite3_stmt *stmt = nullptr;
    sqlite3_stmt *stmt_col_names = nullptr;

  public:
    int init(const char *filename, const char *table_name, const char *sql) {
      int rc = sqlite3_open(filename, &db);
      if (rc) {
        printf("Can't open database: %s\n", sqlite3_errmsg(db));
        return 1;
      }
      rc = sqlite3_prepare_v2(db, sql, -1, &stmt_col_names, nullptr);
      if (rc != SQLITE_OK) {
        printf("SQL error: %s\n", sqlite3_errmsg(db));
        return 1;
      }
      sqlite3_step(stmt_col_names);
      rc = sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr);
      if (rc != SQLITE_OK) {
        printf("SQL error: %s\n", sqlite3_errmsg(db));
        return 1;
      }
      return 0;
    }
    int col_count() {
      return sqlite3_column_count(stmt_col_names);
    }
    const char *col_name(size_t col_idx) {
      return sqlite3_column_name(stmt_col_names, col_idx);
    }
    int col_type(size_t col_idx) {
      return sqlite3_column_type(stmt, col_idx);
    }
    bool is_null(size_t col_idx) {
      return sqlite3_column_type(stmt, col_idx) == SQLITE_NULL;
    }
    bool next() {
      return sqlite3_step(stmt) == SQLITE_ROW;
    }
    void populate_values(madras_dv1::mdx_val_in *values, size_t *value_lens, size_t imp_col_idx, char imp_col_type, char encoding_type, size_t ins_seq_id, size_t sql_col_idx) {
      int64_t s64;
      double dbl;
      if (sqlite3_column_type(stmt, sql_col_idx) == SQLITE_NULL) {
        if (imp_col_type == MST_TEXT || imp_col_type == MST_BIN)
          values[imp_col_idx].i64 = 0;
        else {
          s64 = INT64_MIN;
          memcpy(&values[imp_col_idx], &s64, 8);
        }
        value_lens[imp_col_idx] = 0;
      } else if (imp_col_type == MST_TEXT) {
        values[imp_col_idx].txt_bin = sqlite3_column_text(stmt, sql_col_idx);
        value_lens[imp_col_idx] = sqlite3_column_bytes(stmt, sql_col_idx);
      } else if (imp_col_type == MST_BIN) {
        values[imp_col_idx].txt_bin = (const uint8_t *) sqlite3_column_blob(stmt, sql_col_idx);
        value_lens[imp_col_idx] = sqlite3_column_bytes(stmt, sql_col_idx);
      } else if (imp_col_type >= MST_DATE_US && imp_col_type <= MST_DATETIME_ISOT_MS) {
        const uint8_t *dt_txt_db = sqlite3_column_text(stmt, sql_col_idx);
        int64_t dt_val = madras_dv1::dt_str_to_i64(dt_txt_db, imp_col_type);
        values[imp_col_idx].i64 = dt_val;
        value_lens[imp_col_idx] = 8;
      } else if (imp_col_type == MST_INT) {
        s64 = sqlite3_column_int64(stmt, sql_col_idx);
        values[imp_col_idx].i64 = s64;
        value_lens[imp_col_idx] = 8;
      } else if (imp_col_type >= MST_DECV && imp_col_type <= MST_DEC9) {
        values[imp_col_idx].dbl = sqlite3_column_double(stmt, sql_col_idx);
        value_lens[imp_col_idx] = 8;
      }
    }
    int64_t get_i64(size_t col_idx) {
      return sqlite3_column_int64(stmt, col_idx);
    }
    double get_dbl(size_t col_idx) {
      return sqlite3_column_double(stmt, col_idx);
    }
    const uint8_t *get_text_bin(size_t col_idx, size_t &col_len) {
      col_len = sqlite3_column_bytes(stmt, col_idx);
      return (const uint8_t *) sqlite3_column_blob(stmt, col_idx);
    }
    void reset() {
      sqlite3_reset(stmt);
    }
    void close() {
      if (stmt_col_names != nullptr)
        sqlite3_finalize(stmt_col_names);
      if (stmt != nullptr)
        sqlite3_finalize(stmt);
      if (db != nullptr)
        sqlite3_close(db);
      stmt_col_names = nullptr;
      stmt = nullptr;
      db = nullptr;
    }
};

class duckdb_reader : public data_iface {
  private:
    duckdb_database db = nullptr;
    duckdb_connection conn = nullptr;
    duckdb_result result;
    duckdb_prepared_statement prep = nullptr;
    size_t current_row = 0;
    size_t total_rows = 0;

  public:
    int init(const char *filename, const char *table_name, const char *sql) {
      duckdb_state state;

      state = duckdb_open(filename, &db);
      if (state != DuckDBSuccess) {
        printf("Can't open DuckDB database\n");
        return 1;
      }

      state = duckdb_connect(db, &conn);
      if (state != DuckDBSuccess) {
        printf("Can't connect to DuckDB database\n");
        return 1;
      }

      state = duckdb_prepare(conn, sql, &prep);
      if (state != DuckDBSuccess) {
        printf("SQL prepare error\n");
        return 1;
      }

      state = duckdb_execute_prepared(prep, &result);
      if (state != DuckDBSuccess) {
        printf("SQL execution error\n");
        return 1;
      }

      total_rows = duckdb_row_count(&result);
      current_row = 0;
      return 0;
    }

    int col_count() {
      return duckdb_column_count(&result);
    }

    const char *col_name(size_t col_idx) {
      return duckdb_column_name(&result, col_idx);
    }

    int col_type(size_t col_idx) {
      return duckdb_column_type(&result, col_idx);
    }

    bool is_null(size_t col_idx) {
      return duckdb_value_is_null(&result, col_idx, current_row);
    }

    bool next() {
      if (current_row < total_rows) {
        ++current_row;
        return current_row <= total_rows;
      }
      return false;
    }

    void populate_values(madras_dv1::mdx_val_in *values, size_t *value_lens, size_t imp_col_idx, char imp_col_type, char encoding_type, size_t ins_seq_id, size_t sql_col_idx) {
      int64_t s64;
      double dbl;
      if (duckdb_value_is_null(&result, sql_col_idx, current_row - 1)) {
        if (imp_col_type == MST_TEXT || imp_col_type == MST_BIN)
          values[imp_col_idx].i64 = 0;
        else {
          s64 = INT64_MIN;
          memcpy(&values[imp_col_idx], &s64, 8);
        }
        value_lens[imp_col_idx] = 0;
      } else if (imp_col_type == MST_TEXT) {
        values[imp_col_idx].txt_bin = (const uint8_t *)duckdb_value_varchar(&result, sql_col_idx, current_row - 1);
        value_lens[imp_col_idx] = strlen((const char *)values[imp_col_idx].txt_bin);
      } else if (imp_col_type == MST_BIN) {
        values[imp_col_idx].txt_bin = (const uint8_t *)duckdb_value_varchar(&result, sql_col_idx, current_row - 1);
        value_lens[imp_col_idx] = strlen((const char *)values[imp_col_idx].txt_bin);
      } else if (imp_col_type >= MST_DATE_US && imp_col_type <= MST_DATETIME_ISOT_MS) {
        const char *dt_txt_db = duckdb_value_varchar(&result, sql_col_idx, current_row - 1);
        int64_t dt_val = madras_dv1::dt_str_to_i64((const uint8_t *)dt_txt_db, imp_col_type);
        values[imp_col_idx].i64 = dt_val;
        value_lens[imp_col_idx] = 8;
      } else if (imp_col_type == MST_INT) {
        s64 = duckdb_value_int64(&result, sql_col_idx, current_row - 1);
        values[imp_col_idx].i64 = s64;
        value_lens[imp_col_idx] = 8;
      } else if (imp_col_type >= MST_DECV && imp_col_type <= MST_DEC9) {
        dbl = duckdb_value_double(&result, sql_col_idx, current_row - 1);
        values[imp_col_idx].dbl = dbl;
        value_lens[imp_col_idx] = 8;
      }
    }

    int64_t get_i64(size_t col_idx) {
      return duckdb_value_int64(&result, col_idx, current_row - 1);
    }

    double get_dbl(size_t col_idx) {
      return duckdb_value_double(&result, col_idx, current_row - 1);
    }

    const uint8_t *get_text_bin(size_t col_idx, size_t &col_len) {
      const char *str = duckdb_value_varchar(&result, col_idx, current_row - 1);
      col_len = strlen(str);
      return (const uint8_t *)str;
    }

    void reset() {
      current_row = 0;
    }

    void close() {
      duckdb_destroy_result(&result);
      if (prep)
        duckdb_destroy_prepare(&prep);
      if (conn)
        duckdb_disconnect(&conn);
      if (db)
        duckdb_close(&db);
      prep = nullptr;
      conn = nullptr;
      db = nullptr;
    }
};

class deli_reader : public data_iface {
private:
  std::istream* input = nullptr;
  std::ifstream file;
  std::string current_line;
  std::vector<std::string> headers;
  std::vector<std::string> current_row;
  size_t row_number = 0;
  char delimiter = ',';
  bool has_header = true;
  bool is_all_empty(std::string cell) {
    for (size_t i = 0; i < cell.length(); i++) {
      char c = cell[i];
      if (c != ' ' && c != '\n' && c != '\t' && c != '\r')
        return false;
    }
    return true;
  }
  std::vector<std::string> parse_line(const std::string &line) {
      std::vector<std::string> result;
      std::string cell;
      bool in_quotes = false;
      for (size_t i = 0; i < line.size(); ++i) {
        char c = line[i];
        if (in_quotes) {
          if (c == '"') {
            if (i + 1 < line.size() && line[i + 1] == '"') {
              cell += '"';
              ++i;
            } else {
              in_quotes = false;
              while (i < line.size() && line[i] != delimiter) {
                i++;
              }
              i--;
            }
          } else {
            cell += c;
          }
        } else {
          if (c == '"' && is_all_empty(cell)) {
            cell.clear();
            in_quotes = true;
          } else if (c == delimiter) {
            result.push_back(cell);
            cell.clear();
          } else {
            cell += c;
          }
        }
      }
      result.push_back(cell); // Add the last field
      return result;
  }

public:
  deli_reader(char delim = ',', bool header = true) {
    delimiter = delim;
    has_header = header;
  }
  int init(const char *filename, const char *table_name, const char *sql) {
    if (strncmp(filename, "stdin", 5) == 0) {
      input = &std::cin;
    } else {
      file.open(filename);
      if (!file.is_open()) {
        printf("Can't open file: %s\n", filename);
        return 1;
      }
      input = &file;
    }
    if (has_header && std::getline(*input, current_line))
      headers = parse_line(current_line);
    return 0;
  }

  void set_header(const std::vector<std::string> &header_vec) {
    headers = header_vec;
  }

  int col_count() {
    return headers.size();
  }

  const char *col_name(size_t col_idx) {
    return headers[col_idx].c_str();
  }

  int col_type(size_t) {
    return MST_TEXT;
  }

  bool is_null(size_t col_idx) {
    if (col_idx >= current_row.size())
      return true;
    if (strncmp(current_row[col_idx].data(), "\\N", 2) == 0)
      return true;
    return false;    
  }

  bool next() {
    if (!std::getline(*input, current_line))
      return false;
    current_row = parse_line(current_line);
    ++row_number;
    return true;
  }

  void populate_values(madras_dv1::mdx_val_in *values, size_t *value_lens, size_t exp_col_idx,
                       char exp_col_type, char encoding_type, size_t ins_seq_id, size_t col_idx) {
    if (col_idx >= current_row.size()) {
      values[exp_col_idx].i64 = 0;
      value_lens[exp_col_idx] = 0;
      return;
    }
    const std::string &cell = current_row[col_idx];
    if (exp_col_type == MST_TEXT || exp_col_type == MST_BIN) {
      if (strncmp(cell.data(), "\\N", 2) == 0) {
        values[exp_col_idx].txt_bin = madras_dv1::NULL_VALUE;
        value_lens[exp_col_idx] = madras_dv1::NULL_VALUE_LEN;
      } else {
        values[exp_col_idx].txt_bin = (const uint8_t *) cell.data();
        value_lens[exp_col_idx] = cell.size();
      }
    } else if (exp_col_type >= MST_DATE_US && exp_col_type <= MST_DATETIME_ISOT_MS) {
      if (strncmp(cell.c_str(), "\\N", 2) == 0)
        values[exp_col_idx].i64 = INT64_MIN;
      else {
        int64_t dt_val = madras_dv1::dt_str_to_i64((const uint8_t *)cell.data(), exp_col_type);
        values[exp_col_idx].i64 = dt_val;
        value_lens[exp_col_idx] = 8;
      }
    } else if (exp_col_type == MST_INT) {
      if (strncmp(cell.c_str(), "\\N", 2) == 0)
        values[exp_col_idx].i64 = INT64_MIN;
      else {
        try {
          values[exp_col_idx].i64 = std::stoll(cell);
        } catch (...) {
          printf("Error converting to int at row: %zu, col: %zu, val: %s\n", ins_seq_id, col_idx, cell.c_str());
        }
      }
      value_lens[exp_col_idx] = 8;
    } else if (exp_col_type >= MST_DECV && exp_col_type <= MST_DEC9) {
      if (strncmp(cell.c_str(), "\\N", 2) == 0)
        values[exp_col_idx].dbl = -0.0;
      else {
        try {
          values[exp_col_idx].dbl = std::stod(cell);
        } catch (...) {
          printf("Error converting to double at row: %zu, col: %zu, val: %s\n", ins_seq_id, col_idx, cell.c_str());
        }
      }
      value_lens[exp_col_idx] = 8;
    }
  }

  int64_t get_i64(size_t col_idx) {
    if (strncmp(current_row[col_idx].data(), "\\N", 2) == 0)
      return INT64_MIN;
    int64_t i64 = 0;
    try {
      i64 = std::stoll(current_row[col_idx]);
    } catch (...) {
      printf("Error converting to int at row: %zu, col: %zu, val: %s\n", row_number, col_idx, current_row[col_idx].c_str());
    }
    return i64;
  }

  double get_dbl(size_t col_idx) {
    if (strncmp(current_row[col_idx].data(), "\\N", 2) == 0)
      return -0.0;
    double dbl = 0;
    try {
      dbl = std::stod(current_row[col_idx]);
    } catch (...) {
      printf("Error converting to double at row: %zu, col: %zu, val: %s\n", row_number, col_idx, current_row[col_idx].c_str());
    }
    return dbl;
  }

  const uint8_t *get_text_bin(size_t col_idx, size_t &col_len) {
    if (strncmp(current_row[col_idx].data(), "\\N", 2) == 0) {
      col_len = madras_dv1::NULL_VALUE_LEN;
      return madras_dv1::NULL_VALUE;
    }
    const std::string &cell = current_row[col_idx];
    col_len = cell.size();
    return (const uint8_t *)cell.data();
  }

  void reset() {
    input->clear();
    input->seekg(0);
    if (has_header && std::getline(*input, current_line))
      headers = parse_line(current_line);
    row_number = 0;
  }

  void close() {
    if (input != &std::cin)
      file.close();
    headers.clear();
    current_row.clear();
  }
};

class xml_reader : public data_iface {
private:
  std::istream* input = nullptr;
  std::ifstream file;
  std::string current_line;
  std::vector<std::string> headers;
  std::vector<std::string> current_row;
  size_t row_number = 0;
  bool is_eof = false;
  std::vector<std::string> parse_record(const std::string &line) {
    std::vector<std::string> result;
    std::string cell;
    return result;
  }
  bool is_white_space(char c) {
    return (c == ' ' || c == '\t' || c == '\n' || c == '\r');
  }
  char skip_white_space() {
    int ch;
    while ((ch = input->get()) != EOF) {
      char c = static_cast<char>(ch);
      if (!is_white_space(c))
        break;
    }
    return static_cast<char>(ch);
  }
  void skip_root() {
  }
  bool read_first_record() {
    return false;
  }
  bool read_record() {
    return false;
  }

public:
  xml_reader() {
  }
  int init(const char *filename, const char *table_name, const char *sql) {
    if (strncmp(filename, "stdin", 5) == 0) {
      input = &std::cin;
    } else {
      file.open(filename);
      if (!file.is_open()) {
        printf("Can't open file: %s\n", filename);
        return 1;
      }
      input = &file;
    }
    skip_root();
    is_eof = read_first_record();
    return 0;
  }

  void set_header(const std::vector<std::string> &header_vec) {
    headers = header_vec;
  }

  int col_count() {
    return headers.size();
  }

  const char *col_name(size_t col_idx) {
    return headers[col_idx].c_str();
  }

  int col_type(size_t) {
    return MST_TEXT;
  }

  bool is_null(size_t col_idx) {
    if (col_idx >= current_row.size())
      return true;
    if (strncmp(current_row[col_idx].data(), "\\N", 2) == 0)
      return true;
    return false;    
  }

  bool next() {
    bool is_next = is_eof;
    is_eof = read_record();
    ++row_number;
    return is_next;
  }

  void populate_values(madras_dv1::mdx_val_in *values, size_t *value_lens, size_t exp_col_idx,
                       char exp_col_type, char encoding_type, size_t ins_seq_id, size_t col_idx) {
    if (col_idx >= current_row.size()) {
      values[exp_col_idx].i64 = 0;
      value_lens[exp_col_idx] = 0;
      return;
    }
    const std::string &cell = current_row[col_idx];
    if (exp_col_type == MST_TEXT || exp_col_type == MST_BIN) {
      if (strncmp(cell.data(), "\\N", 2) == 0) {
        values[exp_col_idx].txt_bin = madras_dv1::NULL_VALUE;
        value_lens[exp_col_idx] = madras_dv1::NULL_VALUE_LEN;
      } else {
        values[exp_col_idx].txt_bin = (const uint8_t *) cell.data();
        value_lens[exp_col_idx] = cell.size();
      }
    } else if (exp_col_type >= MST_DATE_US && exp_col_type <= MST_DATETIME_ISOT_MS) {
      if (strncmp(cell.c_str(), "\\N", 2) == 0)
        values[exp_col_idx].i64 = INT64_MIN;
      else {
        int64_t dt_val = madras_dv1::dt_str_to_i64((const uint8_t *)cell.data(), exp_col_type);
        values[exp_col_idx].i64 = dt_val;
        value_lens[exp_col_idx] = 8;
      }
    } else if (exp_col_type == MST_INT) {
      if (strncmp(cell.c_str(), "\\N", 2) == 0)
        values[exp_col_idx].i64 = INT64_MIN;
      else {
        try {
          values[exp_col_idx].i64 = std::stoll(cell);
        } catch (...) {
          printf("Error converting to int at row: %zu, col: %zu, val: %s\n", ins_seq_id, col_idx, cell.c_str());
        }
      }
      value_lens[exp_col_idx] = 8;
    } else if (exp_col_type >= MST_DECV && exp_col_type <= MST_DEC9) {
      if (strncmp(cell.c_str(), "\\N", 2) == 0)
        values[exp_col_idx].dbl = -0.0;
      else {
        try {
          values[exp_col_idx].dbl = std::stod(cell);
        } catch (...) {
          printf("Error converting to double at row: %zu, col: %zu, val: %s\n", ins_seq_id, col_idx, cell.c_str());
        }
      }
      value_lens[exp_col_idx] = 8;
    }
  }

  int64_t get_i64(size_t col_idx) {
    if (strncmp(current_row[col_idx].data(), "\\N", 2) == 0)
      return INT64_MIN;
    int64_t i64 = 0;
    try {
      i64 = std::stoll(current_row[col_idx]);
    } catch (...) {
      printf("Error converting to int at row: %zu, col: %zu, val: %s\n", row_number, col_idx, current_row[col_idx].c_str());
    }
    return i64;
  }

  double get_dbl(size_t col_idx) {
    if (strncmp(current_row[col_idx].data(), "\\N", 2) == 0)
      return -0.0;
    double dbl = 0;
    try {
      dbl = std::stod(current_row[col_idx]);
    } catch (...) {
      printf("Error converting to double at row: %zu, col: %zu, val: %s\n", row_number, col_idx, current_row[col_idx].c_str());
    }
    return dbl;
  }

  const uint8_t *get_text_bin(size_t col_idx, size_t &col_len) {
    if (strncmp(current_row[col_idx].data(), "\\N", 2) == 0) {
      col_len = madras_dv1::NULL_VALUE_LEN;
      return madras_dv1::NULL_VALUE;
    }
    const std::string &cell = current_row[col_idx];
    col_len = cell.size();
    return (const uint8_t *)cell.data();
  }

  void reset() {
    input->clear();
    input->seekg(0);
    row_number = 0;
  }

  void close() {
    if (input != &std::cin)
      file.close();
    headers.clear();
    current_row.clear();
  }
};

data_iface *data_iface::get_data_reader(const char *input_type) {
  if (strncmp(input_type, "sqlite3", 7) == 0)
    return new sqlite_reader();
  if (strncmp(input_type, "duckdb", 6) == 0)
    return new duckdb_reader();
  if (strncmp(input_type, "csv", 3) == 0)
    return new deli_reader(',', true);
  if (strncmp(input_type, "tsv", 3) == 0)
    return new deli_reader('\t', true);
  return nullptr;
}

bool emit_nid_cb_func(void *ctx, uintxx_t node_id) {
   uintxx_t *rec_node_id = (uintxx_t *) ctx;
   if (*rec_node_id == node_id) {
     *rec_node_id = UINTXX_MAX;
     return true;
   }
   return false;
}

void print_usage() {
  printf("\nUsage: import_to_mdx <input_type> <file> <what_to_do> <in_file>\n");
  printf("              <out_file>  <table_or_select> <pk_cols>\n");
  printf("              <storage_types> <encoding_types> [row_count]\n\n");
  printf("  <input_type>      - One of sqlite3, duckdb, csv, tsv, ndjson, xml\n\n");
  printf("  <what_to_do>      - i = import, v = verify, b = both\n\n");
  printf("  <in_file>         - File name [with path]\n\n");
  printf("  <out_file>        - Output File name (if ., then creates <file_name>.mdsi)\n\n");
  printf("  <table_or_select> - Name of table or select statement\n");
  printf("                      Optionally, table name can be followed with\n");
  printf("                          comma-separated list of column names\n\n");
  printf("  <pk_cols>         - Primary key column index\n");
  printf("                          (starting with 1, use + for composite).\n");
  printf("                      If 0, no primary index is created\n\n");
  printf("  <storage_types>   - String having storage type letter for each column.\n");
  printf("                      Following types are supported:\n");
  printf("                      t : text\n");
  printf("                      * : binary data\n");
  printf("                      i : signed 64-bit integer\n");
  printf("                      . : IEEE 64-bit double (lossless)\n");
  printf("                      1 to 9 : store as double truncated to as many decimals\n");
  printf("                      ` : Date only (US mm/dd/yyyy)\n");
  printf("                      a : Date only (Europe dd/mm/yyyy)\n");
  printf("                      b : Date only (ISO yyyy-mm-dd)\n");
  printf("                      c : Date only (US mm/dd/yyyy hh:mi:ss)\n");
  printf("                      d : Date time (Europe dd/mm/yyyy hh:mi:ss)\n");
  printf("                      e : Date time (ISO yyyy-mm-dd hh:mi:ss)\n");
  printf("                      f : Date time (ISOT yyyy-mm-ddThh:mi:ss)\n");
  printf("                      g : Date time with ms (ISO yyyy-mm-dd hh:mi:ss.nnn)\n");
  printf("                      h : Date time with ms (ISOT yyyy-mm-ddThh:mi:ss.nnn)\n");
  printf("                      S : Secondary index (virtual columns at end)\n");
  printf("                      - : Exclude column\n");
  printf("                      Note: Formats specified for date/time are only for info\n");
  printf("                            All date/time are stored as integers internally.\n\n");
  printf("  <encoding_types>  - String having encoding type letter for each column.\n");
  printf("                      Following types are supported:\n");
  printf("                      u : Make unique values (remove duplicates before storing)\n");
  printf("                      d : Make unique values and apply delta coding\n");
  printf("                          (only for numeric columns)\n");
  printf("                      v : Store with fast variable number format\n");
  printf("                      t : Store as trie\n");
  printf("                      T : Store as two-way trie\n");
  printf("                      w : words (split into words for better compression)\n");
  printf("                      W : words two-way\n\n");
  printf("                      Note: Any column with any data type may be stored as trie\n");
  printf("                            but w/W is only for text columns.\n\n");
  printf("  [row_count]       - No. of rows to import. If 0, all rows are imported.\n\n");
  printf("  [secondary_idxs]  - Secondary indices to be created at end\n");
  printf("                        Specify columns same way as pk_cols\n");
  printf("                        Use comma to specify multiple indices.\n");
  printf("                        For each index, specify storage_type as S\n");
  printf("                                        and encoding_type as T.\n\n");
}

int main(int argc, char* argv[]) {

  int rc;
  if (argc < 8) {
    print_usage();
    return 1;
  }

  const char *print_each_mismatch = std::getenv("MDX_PRINT_MISMATCH");
  bool to_print_mismatch = false;
  if (print_each_mismatch != nullptr && strcmp(print_each_mismatch, "yes") == 0)
    to_print_mismatch = true;
  struct timespec t;
  clock_gettime(CLOCK_REALTIME, &t);

  data_iface *data_reader = data_iface::get_data_reader(argv[1]);
  if (data_reader == nullptr) {
    printf("Invalid input type: %s", argv[1]);
    return 1;
  }
  char what_to_do = DO_BOTH;
  if (strlen(argv[2]) > 0)
    what_to_do = argv[2][0];
  if (what_to_do != DO_BOTH && what_to_do != DO_IMPORT && what_to_do != DO_VERIFY) {
    printf("Action can only be 'i' or 'v' or 'b'\n");
    return 1;
  }

  const char *filename = argv[3];
  if (strncmp(filename, "stdin", 5) == 0 && what_to_do == DO_BOTH) {
    printf("If reading from stdin, cannot do only either of import or verification and not both\n");
    return 1;
  }

  std::string out_file = argv[4];
  if (out_file.length() > 0 && out_file[0] == '.') {
    out_file = filename;
    out_file += ".mdsi";
  }

  const char *table_name = "vtab";
  const char *arg_sel_or_tbl = argv[5];
  std::string sql;
  if (sqlite3_strnicmp(arg_sel_or_tbl, "select ", 7) == 0) {
    sql = arg_sel_or_tbl;
  } else {
    sql = "SELECT * FROM ";
    sql += arg_sel_or_tbl;
    table_name = arg_sel_or_tbl;
  }

  rc = data_reader->init(filename, table_name, sql.c_str());
  if (rc) {
    data_reader->close();
    return 1;
  }

  int sql_column_count = data_reader->col_count();
  int column_count = sql_column_count;
  const char *storage_types = argv[7];
  if (column_count > strlen(storage_types)) {
    printf("Storage types not specified for all columns\n");
    data_reader->close();
    return 1;
  }
  if (column_count < strlen(storage_types))
    column_count = strlen(storage_types);
  const char *encoding_types = argv[8];
  int row_count = INT_MAX;
  if (argc > 9)
    row_count = atoi(argv[9]);
  if (row_count == 0)
    row_count = INT_MAX;

  const char *sk_col_positions = "";
  if (argc > 10)
    sk_col_positions = argv[10];
    
  const char *col_positions_str = argv[6];
  std::vector<uint16_t> col_positions;
  if (col_positions_str != nullptr && col_positions_str[0] != '0' && col_positions_str[0] != '\0') {
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
        printf("Storage type cannot be '-' for primary column\n");
        data_reader->close();
        return 1;
      }
      continue;
    }
    if (encoding_types[i] != 'u') {
      if (is_pk_col) {
        printf("Encoding type should be u for primary columns\n");
        data_reader->close();
        return 1;
      }
    }
    if (!is_pk_col)
      col_positions.push_back(i);
  }

  std::string column_names = table_name;
  std::string col_types;
  std::string col_encodings;
  size_t sec_col_count = 0;
  size_t imp_col_count = 0;
  for (size_t i = 0; i < col_positions.size(); i++) {
    uint16_t col_pos = col_positions[i];
    if (col_pos < sql_column_count) {
      const char* column_name = data_reader->col_name(col_pos);
      column_names.append(",");
      column_names.append(column_name);
    }
    col_types.append(1, storage_types[col_pos]);
    col_encodings.append(1, encoding_types[col_pos]);
    imp_col_count++;
    if (storage_types[col_pos] == MST_SEC_2WAY) {
      sec_col_count++;
      if (encoding_types[col_pos] != MSE_TRIE_2WAY) {
        printf("Encoding type should be T for secondary indices\n");
        data_reader->close();
        return 1;
      }
    } else if (sec_col_count > 0) {
      printf("Secondary indices should be defined at the end\n");
      data_reader->close();
      return 1;
    }
  }
  printf("Col Count: %lu, PK Col Count: %lu, Table/Key/Column names: %s, types: %s, encodings: %s\n",
    imp_col_count, pk_col_count, column_names.c_str(), col_types.c_str(), col_encodings.c_str());

  if (imp_col_count == 0) {
    printf("At least 1 column to be specified for import\n");
    data_reader->close();
    return 1;
  }

  size_t imp_col_idx = 0;
  size_t ins_seq_id = 0;
  madras_dv1::mdx_val_in values[imp_col_count];
  size_t value_lens[imp_col_count];

  madras_dv1::bldr_options bldr_opts = madras_dv1::dflt_opts;
  bldr_opts.inner_tries = true;
  bldr_opts.sort_nodes_on_freq = false;
  madras_dv1::builder mb(out_file.c_str(), column_names.c_str(), imp_col_count, 
      col_types.c_str(), col_encodings.c_str(), 0, pk_col_count,
      &bldr_opts, &madras_dv1::dflt_word_splitter, sk_col_positions);

  if (what_to_do == DO_IMPORT || what_to_do == DO_BOTH) {
    mb.set_print_enabled();
    mb.open_file();

    while (data_reader->next()) {
      imp_col_idx = 0;
      for (size_t i = 0; i < col_positions.size(); i++) {
        size_t sql_col_idx = col_positions[i];
        char imp_col_type = storage_types[sql_col_idx];
        char encoding_type = encoding_types[sql_col_idx];
        data_reader->populate_values(values, value_lens, imp_col_idx, imp_col_type, encoding_type, ins_seq_id, sql_col_idx);
        imp_col_idx++;
      }
      if (mb.insert_record(values, value_lens)) {
        printf("Unexpected: Record found: %zu. Check Primary key definition.\n", ins_seq_id);
        data_reader->close();
        return 1;
      }
      if (ins_seq_id >= row_count)
        break;
      ins_seq_id++;
      if ((ins_seq_id % 100000) == 0) {
        putchar('.');
        fflush(stdout);
      }
    }

    printf("\n");

    mb.build_and_write_all();

    t = print_time_taken(t, "Time taken for build: ");
  }

  madras_dv1::static_trie_map stm;
  stm.load(out_file.c_str());
  printf("Tbl name: %s\n", stm.get_table_name());
  printf("Col types: %s\n", stm.get_column_types());
  printf("Col encodings: %s\n", stm.get_column_encodings());
  uint16_t stm_col_count = stm.get_column_count();
  printf("Col Count: %u, Cols:", stm_col_count);
  for (int i = 0; i < stm_col_count; i++)
    printf(", %s (%" PRIuXX ")", stm.get_column_name(i), stm.get_max_val_len(i));
  printf("\n");

  if (what_to_do == DO_IMPORT) {
    data_reader->close();
    return 1;
  }

  if (strncmp(filename, "stdin", 5) != 0)
    data_reader->reset();

  ins_seq_id = 0;
  madras_dv1::input_ctx in_ctx;
  uint8_t *key = new uint8_t[stm.get_max_key_len()];
  uintxx_t ptr_count[column_count];
  int64_t int_sums[column_count];
  double dbl_sums[column_count];
  size_t errors[column_count];
  bool has_error = false;
  memset(ptr_count, '\xFF', sizeof(uintxx_t) * column_count);
  memset(int_sums, '\0', sizeof(int64_t) * column_count);
  memset(dbl_sums, '\0', sizeof(double) * column_count);
  memset(errors, '\0', sizeof(size_t) * column_count);
  while (data_reader->next()) {
    if (ins_seq_id > row_count)
      break;
    uintxx_t node_id = ins_seq_id;
    if (pk_col_count > 0) {
      gen::byte_vec key_rec;
      for (size_t i = 0; i < pk_col_count; i++) {
        size_t sql_col_idx = col_positions[i];
        char imp_col_type = storage_types[sql_col_idx];
        char encoding_type = encoding_types[sql_col_idx];
        data_reader->populate_values(values, value_lens, i, imp_col_type, encoding_type, ins_seq_id, sql_col_idx);
        mb.append_rec_value(imp_col_type, encoding_type, values[i], value_lens[i],
            key_rec, i < (pk_col_count - 1) ? APPEND_REC_KEY_MIDDLE : APPEND_REC_KEY_LAST);
      }
      in_ctx.key = key_rec.data();
      in_ctx.key_len = key_rec.size();
      bool is_found = stm.lookup(in_ctx);
      if (!is_found) {
        errors[0]++; has_error = true;
        if (to_print_mismatch)
          printf("Key not found: nid: %" PRIuXX ", seq: %zu, len: %zu\n", in_ctx.node_id, ins_seq_id, key_rec.size());
        ins_seq_id++;
        continue;
      }
      node_id = in_ctx.node_id;
    }
    for (size_t col_idx = pk_col_count; col_idx < (imp_col_count - sec_col_count); col_idx++) {
      size_t sql_col_idx = col_positions[col_idx];
      char imp_col_type = storage_types[sql_col_idx];
      char encoding_type = encoding_types[sql_col_idx];
      if (data_reader->is_null(sql_col_idx)) {
        uint8_t val_buf[stm.get_max_val_len()];
        madras_dv1::mdx_val mv;
        mv.txt_bin = val_buf;
        size_t val_len = 8;
        const uint8_t *ret_buf = stm.get_col_val(node_id, col_idx, &val_len, mv); // , &ptr_count[col_idx]);
        size_t null_value_len;
        uint8_t *null_value = stm.get_null_value(null_value_len);
        if (imp_col_type == MST_TEXT || imp_col_type == MST_BIN) {
          if (val_len != null_value_len || memcmp(val_buf, null_value, null_value_len) != 0) {
            errors[col_idx]++; has_error = true;
            if (to_print_mismatch) {
              printf("Val not null: nid: %" PRIuXX ", seq: %lu, col: %lu, A:%lu,[%.*s]/%lu\n", node_id, ins_seq_id, col_idx, val_len, (int) val_len, val_buf, null_value_len);
              printf("%d, %d\n", val_buf[0], val_buf[1]);
            }
          }
        } else {
          int64_t i64 = mv.i64;
          if (i64 != INT64_MIN) {
            errors[col_idx]++; has_error = true;
            if (to_print_mismatch)
              printf("Val not null: nid: %" PRIuXX ", seq: %lu, col: %lu, A: %lld\n", node_id, ins_seq_id, col_idx, i64);
          }
        }
      } else
      if (imp_col_type == MST_TEXT || imp_col_type == MST_BIN) {
        size_t sql_val_len = 0;
        const uint8_t *sql_val = data_reader->get_text_bin(sql_col_idx, sql_val_len);
        size_t val_len = stm.get_max_val_len(col_idx) + 1;
        uint8_t val_buf[val_len];
        madras_dv1::mdx_val mv;
        mv.txt_bin = val_buf;
        const uint8_t *ret_buf = stm.get_col_val(node_id, col_idx, &val_len, mv); // , &ptr_count[col_idx]);
        if (sql_val == nullptr) {
          size_t empty_value_len;
          uint8_t *empty_value = stm.get_empty_value(empty_value_len);
          if (val_len != empty_value_len || memcmp(ret_buf, empty_value, empty_value_len) != 0) {
            errors[col_idx]++; has_error = true;
            if (to_print_mismatch) {
              printf("Val not empty: nid: %" PRIuXX ", seq: %lu, col: %lu, A:%lu,[%.*s]/%lu\n", node_id, ins_seq_id, col_idx, val_len, (int) val_len, ret_buf, empty_value_len);
              printf("%d, %d\n", ret_buf[0], ret_buf[1]);
            }
          }
        } else if (val_len != sql_val_len) {
          errors[col_idx]++; has_error = true;
          if (to_print_mismatch) {
            printf("Val len mismatch: nid: %" PRIuXX ", seq: %lu, col: %lu, E:%lu/A:%lu\n", node_id, ins_seq_id, col_idx, sql_val_len, val_len);
            printf("Expected: [%s]\n", (sql_val == nullptr ? "nullptr" : (const char *) sql_val));
            printf("Found: [%.*s]\n", (int) val_len, ret_buf);
          }
        } else {
          if (memcmp(sql_val, ret_buf, val_len) != 0) {
            errors[col_idx]++; has_error = true;
            if (to_print_mismatch) {
              printf("Val mismatch: nid: %" PRIuXX ", seq: %lu, col: %lu, E:%lu/A:%lu\n", node_id, ins_seq_id, col_idx, sql_val_len, val_len);
              printf("Expected: [%s]\n", (sql_val == nullptr ? "nullptr" : (const char *) sql_val));
              printf("Found: [%.*s]\n", (int) val_len, ret_buf);
            }
          }
        }
        if (encoding_type == MSE_WORDS_2WAY) {
          std::vector<uintxx_t> word_positions(sql_val_len + 1);
          madras_dv1::splitter_result sr = madras_dv1::dflt_word_splitter.split_into_words(sql_val, sql_val_len,
                    UINT32_MAX, word_positions.data());
          for (size_t i = 0; i < sr.word_count; i++) {
            volatile uintxx_t rec_node_id = node_id;
            const char *word = (const char *) sql_val + word_positions[i];
            size_t word_len = word_positions[i + 1] - word_positions[i];
            stm.shortlist_word_records(col_idx, word, word_len, emit_nid_cb_func, (void *) &rec_node_id);
            if (rec_node_id != UINTXX_MAX) {
              errors[col_idx]++; has_error = true;
              if (to_print_mismatch) {
                printf("Word reverse lookkup fail: %zu, [%.*s], [%.*s]\n", sql_val_len, (int) sql_val_len, sql_val, (int) word_len, word);
              }
            }
          }
        }
      } else if (imp_col_type >= MST_DATE_US && imp_col_type <= MST_DATETIME_ISOT_MS) {
        size_t sql_val_len = 0;
        const uint8_t *sql_val = data_reader->get_text_bin(sql_col_idx, sql_val_len);
        size_t val_len = 8;
        madras_dv1::mdx_val mv;
        stm.get_col_val(node_id, col_idx, &val_len, mv); // , &ptr_count[col_idx]);
        char dt_txt[50];
        val_len = madras_dv1::dt_i64_to_str(mv.i64, dt_txt, sizeof(dt_txt), imp_col_type);
        if (val_len != sql_val_len) {
          errors[col_idx]++; has_error = true;
          if (to_print_mismatch) {
            printf("Val len mismatch: nid: %" PRIuXX ", seq: %lu, col: %lu, E:%lu/A:%lu\n", node_id, ins_seq_id, col_idx, sql_val_len, val_len);
            printf("Expected: [%s]\n", (sql_val == nullptr ? "nullptr" : (const char *) sql_val));
            printf("Found: [%.*s]\n", (int) val_len, dt_txt);
          }
        } else {
          if (memcmp(sql_val, dt_txt, val_len) != 0) {
            errors[col_idx]++; has_error = true;
            if (to_print_mismatch) {
              printf("Val mismatch: nid: %" PRIuXX ", seq: %lu, col: %lu, E:%lu/A:%lu\n", node_id, ins_seq_id, col_idx, sql_val_len, val_len);
              printf("Expected: [%s]\n", (sql_val == nullptr ? "nullptr" : (const char *) sql_val));
              printf("Found: [%.*s]\n", (int) val_len, dt_txt);
            }
          }
        }
      } else if (imp_col_type == MST_INT) {
        int64_t sql_val = data_reader->get_i64(sql_col_idx);
        size_t val_len = 8;
        madras_dv1::mdx_val mv;
        stm.get_col_val(node_id, col_idx, &val_len, mv); // , &ptr_count[col_idx]);
        int64_t i64 = mv.i64;
        if (i64 != sql_val) {
          errors[col_idx]++; has_error = true;
          if (to_print_mismatch)
            printf("Int not matching: nid: %" PRIuXX ", seq: %zu, col: %zu - e%lld:a%lld\n", node_id, ins_seq_id, col_idx, sql_val, i64);
        }
        int_sums[col_idx] += i64;
      } else if (imp_col_type >= MST_DECV && imp_col_type <= MST_DEC9) {
        double sql_val = round_dbl(data_reader->get_dbl(sql_col_idx), imp_col_type);
        size_t val_len;
        madras_dv1::mdx_val mv;
        stm.get_col_val(node_id, col_idx, &val_len, mv); // , &ptr_count[col_idx]);
        double dbl_val = mv.dbl;
        if (dbl_val != sql_val) {
          errors[col_idx]++; has_error = true;
          if (to_print_mismatch)
            printf("Dbl not matching: nid: %" PRIuXX ", seq: %zu, col: %zu - e%lf:a%lf\n", node_id, ins_seq_id, col_idx, sql_val, dbl_val);
        }
        dbl_sums[col_idx] += dbl_val;
      }
      if (encoding_type == MSE_TRIE_2WAY) {
        gen::byte_vec key_rec;
        data_reader->populate_values(values, value_lens, col_idx, imp_col_type, encoding_type, ins_seq_id, sql_col_idx);
        mb.append_rec_value(imp_col_type, encoding_type, values[col_idx], value_lens[col_idx], key_rec, APPEND_REC_KEY_LAST);
        in_ctx.key = key_rec.data();
        in_ctx.key_len = key_rec.size();
        madras_dv1::static_trie_map *col_trie = stm.get_col_trie_map(col_idx);
        bool is_found = col_trie->lookup(in_ctx);
        if (!is_found) {
          errors[col_idx]++; has_error = true;
          if (to_print_mismatch)
            printf("Trie value not found: node_id: %" PRIuXX ", col: %zu, len: %zu\n", node_id, col_idx, key_rec.size());
        } else {
          volatile uintxx_t rec_node_id = node_id;
          madras_dv1::static_trie_map::emit_rev_nids(col_trie, in_ctx.node_id, emit_nid_cb_func, (void *) &rec_node_id);
          if (rec_node_id != UINTXX_MAX) {
            errors[col_idx]++; has_error = true;
            if (to_print_mismatch) {
              size_t sql_val_len = 0;
              const uint8_t *sql_val = data_reader->get_text_bin(sql_col_idx, sql_val_len);
              printf("Col trie reverse lookup fail: node_id: %" PRIuXX ", col: %zu, value: [%.*s] len: %zu\n", node_id, col_idx, (int) sql_val_len, sql_val, key_rec.size());
            }
          }
        }
      }
    }
    // if (errors[0] > 0)
    //   printf("errors > 0: %lu\n", ins_seq_id);
    ins_seq_id++;
    if ((ins_seq_id % 10000) == 0) {
      putchar(has_error ? 'x' : '.');
      fflush(stdout);
    }
  }
  printf("\n");
  printf("Totals:");
  for (int i = 0; i < stm_col_count; i++) {
    printf(" %s:", stm.get_column_name(i));
    if (int_sums[i] != 0)
      printf(" %" PRId64, int_sums[i]);
    if (dbl_sums[i] != 0)
      printf(" %f", dbl_sums[i]);
  }
  printf("\n");
  printf("\nMISMATCHES:");
  size_t mm_count = 0;
  for (size_t i = 0; i < stm_col_count; i++) {
    if (errors[i] > 0) {
      printf(" %s: (%lu),", stm.get_column_name(i), errors[i]);
      mm_count++;
    }
  }
  if (mm_count == 0)
    printf("No mismatch");
  else
    printf("Use `export MDX_PRINT_MISMATCH=yes` to print mismatches");
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
  data_reader->close();
  delete data_reader;

  t = print_time_taken(t, "Time taken for verification: ");

  return 0;

}
