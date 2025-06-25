#include "duckdb.hpp"
#include <iostream>
#include <string>
#include <sys/stat.h>

using namespace duckdb;

double wc_time_taken_in_secs(struct timespec t) {
  struct timespec t_end;
  clock_gettime(CLOCK_REALTIME, &t_end);
  return (t_end.tv_sec - t.tv_sec) + (t_end.tv_nsec - t.tv_nsec) / 1e9;
}

struct timespec wc_print_time_taken(struct timespec t, const char *msg) {
  double time_taken = wc_time_taken_in_secs(t); // in seconds
  printf("%s %lf\n", msg, time_taken);
  clock_gettime(CLOCK_REALTIME, &t);
  return t;
}

double time_taken_in_secs(clock_t t) {
  t = clock() - t;
  return ((double)t)/CLOCKS_PER_SEC;
}

clock_t print_time_taken(clock_t t, const char *msg) {
  double time_taken = time_taken_in_secs(t); // in seconds
  printf("%s %f\n", msg, time_taken);
  return clock();
}

int main(int argc, char *argv[]) {
    if (argc != 4) {
        std::cerr << "Usage: " << argv[0] << " <database_file> <table_name> <column_name>\n";
        return 1;
    }

    std::string db_file = argv[1];
    std::string table_name = argv[2];
    std::string column_name = argv[3];

    try {
        DuckDB db(db_file);
        Connection con(db);

		clock_t t = clock();
		struct timespec wc_t;
		clock_gettime(CLOCK_REALTIME, &wc_t);
	
        // Construct SQL SUM query
        std::string query = "SELECT SUM(" + column_name + ") FROM " + table_name + ";";
        auto result = con.Query(query);

        if (!result || result->HasError()) {
            std::cerr << "Query failed: " << result->GetError() << "\n";
            return 1;
        }

        // Use Fetch() to get the result DataChunk
        auto chunk = result->Fetch();
        if (!chunk || chunk->size() == 0) {
            std::cerr << "Query returned no rows.\n";
            return 1;
        }

        auto val = chunk->GetValue(0, 0);
        if (val.IsNull()) {
            std::cout << "Sum is NULL (e.g. all values may be NULL).\n";
        } else {
            std::cout << "Sum of column \"" << column_name << "\": " << val.ToString() << "\n";
        }
		t = print_time_taken(t, "Time taken: ");
		wc_t = wc_print_time_taken(wc_t, "Time taken (wc): ");

    } catch (const std::exception &ex) {
        std::cerr << "Exception: " << ex.what() << "\n";
        return 1;
    }

    return 0;
}
