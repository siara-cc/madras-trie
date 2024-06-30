#include "duckdb.hpp"
#include <iostream>
#include <string>
#include <sys/resource.h>
#include <sys/proc_info.h>
#include <libproc.h>

using namespace duckdb;
using namespace std;

int main() {
    // Initialize DuckDB
    DuckDB db(nullptr);

    // Connect to an in-memory database
    Connection con(db);

    // Load Parquet file
    con.Query("CREATE TABLE parquet_data USING parquet_auto('example.parquet')");

    // Execute SELECT statement
    auto result = con.Query("SELECT * FROM parquet_data");

    // Display results
    cout << "Results:" << endl;
    for (auto &row : result) {
        for (size_t i = 0; i < result.column_count(); i++) {
            cout << row[i].ToString() << "\t";
        }
        cout << endl;
    }

    // Show process stats
    struct rusage usage;
    if (getrusage(RUSAGE_SELF, &usage) == 0) {
        cout << "\nProcess stats:" << endl;
        cout << "User CPU time: " << usage.ru_utime.tv_sec << " seconds, " << usage.ru_utime.tv_usec << " microseconds" << endl;
        cout << "System CPU time: " << usage.ru_stime.tv_sec << " seconds, " << usage.ru_stime.tv_usec << " microseconds" << endl;
        cout << "Maximum resident set size: " << usage.ru_maxrss << " KB" << endl;
        // Add more stats as needed
    } else {
        cerr << "Error getting process stats." << endl;
    }

    // Show disk I/O stats
    pid_t pid = getpid();
    struct proc_taskinfo taskinfo;
    if (proc_pidinfo(pid, PROC_PIDTASKINFO, 0, &taskinfo, sizeof(taskinfo)) > 0) {
        cout << "\nDisk I/O stats:" << endl;
        cout << "Bytes read from disk: " << taskinfo.pti_diskread << " bytes" << endl;
        // Add more disk I/O stats as needed
    } else {
        cerr << "Error getting disk I/O stats." << endl;
    }

    return 0;
}

