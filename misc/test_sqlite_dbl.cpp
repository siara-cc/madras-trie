#include <iostream>
#include <cstring>
#include <time.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <sqlite3.h>
#include <inttypes.h>
#include <sys/stat.h>

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

int countDecimalPlaces(double num) {
    int count = 0;
    const double epsilon = 1e-308; // Precision threshold
    double fractional = num - floor(num); // Extract fractional part

    while (fractional > epsilon && count < 16) {
        fractional *= 10;
        fractional = fractional - floor(fractional); // Remove integer part
        count++;
    }

    return count;
}

double tens[] = {1, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19, 1e20, 1e21, 1e22};
double dbl_div[] = {1.0000000000000000001, 10.000000000000000001, 100.00000000000000001, 1000.0000000000000001,
                    10000.000000000000001, 100000.00000000000001, 1000000.0000000000001, 10000000.000000000001,
                    100000000.00000000001, 1000000000.0000000001, 10000000000.000000001, 100000000000.00000001,
                    1000000000000.0000001, 10000000000000.000001, 100000000000000.00001, 1000000000000000.0001,
                    10000000000000000.001, 100000000000000000.01, 1000000000000000000.1};

uint8_t convert_to_int(double dbl, int64_t& i64) {
  const double epsilon = 1e-20;
  const size_t zero_nine_thres = 5;
  i64 = (int64_t) dbl;
  int64_t ifrac = 0;
  size_t frac_width = 0;
  double frac = fabs(dbl - i64);
  int digit, prev_digit = 8;
  size_t rpt_count = 0;
  while (frac > epsilon && frac_width < 20) {
    frac *= 10;
    digit = frac;
    frac -= digit;
    // printf("d: %d, pd: %d, fw: %lu\n", digit, prev_digit, frac_width);
    if ((prev_digit == 0 && digit == 0) ||
        (prev_digit == 9 && digit == 9)) {
      rpt_count++;
      if (rpt_count > zero_nine_thres) {
        frac_width--;
        ifrac = (ifrac - prev_digit) / 10;
        if (digit == 9)
          ifrac++;
        i64 = (i64 * tens[frac_width]) + (dbl < 0 ? -ifrac : ifrac);
        return frac_width;
      }
      continue;
    }
    while (rpt_count > 0) {
      ifrac = (ifrac * 10) + prev_digit;
      frac_width++;
      rpt_count--;
    }
    ifrac = (ifrac * 10) + digit;
    frac_width++;
    prev_digit = digit;
  }
  if (frac_width == 20) {
    memcpy(&i64, &dbl, 8);
    return UINT8_MAX;
  }
  i64 = (i64 * tens[frac_width]) + (dbl < 0 ? -ifrac : ifrac);
  return frac_width;
}

bool check_dbl_error(double dbl) {
  int64_t i64;
  uint8_t c = convert_to_int(dbl, i64);
  if (c == UINT8_MAX) {
    //printf("Orig: %.20lf\n", dbl);
    return false;
  }
  double convert_back = (double) i64 / tens[c];
  // double convert_back = (double) i64 / dbl_div[c];
  if (dbl != convert_back) {
    // printf("orig: %.16lf, converted: %.16lf, i: %lld, dc: %d\n", dbl, convert_back, i64, c);
    return false;
  }
  // printf("Dbl: %.20lf, dc: %d\n", dbl, c);
  return true;
}

int main(int argc, char* argv[]) {

  struct timespec t;
  clock_gettime(CLOCK_REALTIME, &t);

  // check_dbl_error(54.6838400000000036);
  // return 1;

  sqlite3 *db;
  sqlite3_stmt *stmt;
  int rc;
  if (argc < 4) {
    printf("Usage: dbl_test <db file> <tbl> <col>\n");
    return 1;
  }

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
    sql = "SELECT ";
    sql += argv[3];
    sql += " FROM ";
    sql += arg_sel_or_tbl;
    table_name = arg_sel_or_tbl;
  }

  rc = sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr);
  if (rc != SQLITE_OK) {
    std::cerr << "SQL error: " << sqlite3_errmsg(db) << std::endl;
    sqlite3_close(db);
    return 1;
  }

  size_t err_count = 0;
  size_t total_count = 0;
  // double dbl_total = 0;
  while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
    total_count++;
    if (sqlite3_column_type(stmt, 0) == SQLITE_NULL)
      continue;
    // if (total_count > 10)
    //   break;
    double dbl = sqlite3_column_double(stmt, 0);
    // dbl_total += dbl;
    if (!check_dbl_error(dbl))
      err_count++;
  }
  printf("Error count: %lu, total count: %lu\n", err_count, total_count);
  // printf("Dbl total: %lf\n", dbl_total);

  sqlite3_finalize(stmt);
  sqlite3_close(db);

  t = print_time_taken(t, "Time taken: ");

  return 0;

}
