#include "duckdb.hpp"
#include <iostream>
#include <string>
#include <stdint.h>
#include <stdio.h>

using namespace duckdb;

int main(int argc, char* argv[]) {
    if (argc != 4) {
        std::cerr << "Usage: " << argv[0] << " <database_file> <table_name> <column_idx>\n";
        return 1;
    }

    std::string db_file = argv[1];
    std::string table_name = argv[2];

    try {
        DuckDB db(db_file);
        Connection con(db);

        idx_t col_index = atoi(argv[3]);
		auto rel = con.Table(table_name);
		auto result = rel->Execute();

		int64_t sum_int = 0;
        double sum_dbl = 0.0;

        while (true) {
            auto chunk = result->Fetch();
            if (!chunk || chunk->size() == 0) break;

            Vector &col = chunk->data[col_index];
            ValidityMask &mask = FlatVector::Validity(col);
            auto physical_type = col.GetType().InternalType();

            switch (physical_type) {
                case PhysicalType::INT32: {
                    auto data = FlatVector::GetData<int32_t>(col);
                    for (idx_t i = 0; i < chunk->size(); i++) {
                        if (!mask.RowIsValid(i)) continue;
                        sum_int += data[i];
                    }
                    break;
                }
                case PhysicalType::INT64: {
                    auto data = FlatVector::GetData<int64_t>(col);
                    for (idx_t i = 0; i < chunk->size(); i++) {
                        if (!mask.RowIsValid(i)) continue;
                        sum_int += data[i];
                    }
                    break;
                }
                case PhysicalType::FLOAT: {
                    auto data = FlatVector::GetData<float>(col);
                    for (idx_t i = 0; i < chunk->size(); i++) {
                        if (!mask.RowIsValid(i)) continue;
                        sum_dbl += data[i];
                    }
                    break;
                }
                case PhysicalType::DOUBLE: {
                    auto data = FlatVector::GetData<double>(col);
                    for (idx_t i = 0; i < chunk->size(); i++) {
                        if (!mask.RowIsValid(i)) continue;
                        sum_dbl += data[i];
                    }
                    break;
                }
                default:
                    std::cerr << "Unsupported column type for summing.\n";
                    return 1;
            }
        }

        printf("Sum of column: %lld / %lf\n", sum_int, sum_dbl);

    } catch (const std::exception &ex) {
        std::cerr << "Exception: " << ex.what() << "\n";
        return 1;
    }

    return 0;
}
