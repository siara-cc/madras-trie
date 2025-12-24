#include "duckdb.hpp"
#include <iostream>

// custom_reader.cpp

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <memory>
#include <vector>

#include <string>
#include <iostream>
#include <arrow/c/helpers.h>

#include "madras_dv1.hpp"

using namespace madras_dv1;

std::vector<static_trie_map *> stm_vec;
std::vector<std::shared_ptr<arrow::Schema>> stm_schema_vec;

void append_field(arrow::FieldVector& fv, static_trie_map *stm, size_t i) {
  std::shared_ptr<arrow::DataType> dt;
  char data_type = stm->get_column_type(i);
  switch (data_type) {
    case MST_BIN:
      dt = arrow::binary();
      printf("binary\n");
      break;
    case MST_TEXT:
      dt = arrow::utf8();
      printf("text\n");
      break;
    case MST_INT:
      dt = arrow::int64();
      printf("int64\n");
      break;
    case MST_DECV ... MST_DEC9:
      dt = arrow::float64();
      printf("float64\n");
      break;
    case MST_DATE_US ... MST_DATE_ISO:
      printf("date64\n");
      dt = arrow::date64();
      break;
    case MST_DATETIME_US ... MST_DATETIME_ISOT_MS:
      printf("timestamp\n");
      dt = arrow::timestamp(arrow::TimeUnit::MILLI);
      break;
  }
  printf("col_name: %s\n", stm->get_column_name(i));
  fv.push_back(arrow::field(stm->get_column_name(i), dt));
}

extern "C" size_t ExportSchema(struct ArrowSchema* out_schema, const char* file_path) {
    printf("Fiile: %s\n", file_path);
    size_t trie_num = stm_vec.size();
    static_trie_map *stm = new static_trie_map();
    stm_vec.push_back(stm);
    // stm->load(file_path);
    stm->map_file_to_mem(file_path);
    arrow::FieldVector fv;
    size_t col_count = stm->get_column_count();
    for (size_t i = 0; i < col_count; i++) {
      append_field(fv, stm, i);
    }
    stm_schema_vec.push_back(arrow::schema(fv));
    arrow::ExportSchema(*stm_schema_vec[trie_num], out_schema);  // Exports schema to FFI_ArrowSchema
    return trie_num;
}

#define BATCH_SIZE 1024
class CustomRecordBatchReader : public arrow::RecordBatchReader {
  private:
    size_t trie_num;
    size_t *projection;
    size_t projection_len;
    size_t batch_count;
    size_t node_count;
    std::vector<arrow::ArrayBuilder *> ab_vec;
    std::vector<madras_dv1::value_retriever_base *> mdx_vals;
    std::vector<madras_dv1::val_ctx *> mdx_val_ctxs;
    std::shared_ptr<arrow::Schema> projected_schema;

  public:
    CustomRecordBatchReader(size_t _trie_num, const size_t *_projection, size_t _projection_len)
        : trie_num (_trie_num), projection_len (_projection_len) {
      projection = new size_t[projection_len];
      memcpy(projection, _projection, sizeof(size_t) * _projection_len);
      node_count = stm_vec[trie_num]->get_node_count();
      batch_count = node_count / BATCH_SIZE;
      if (node_count % BATCH_SIZE)
        batch_count++;
      arrow::FieldVector fv;
      for (size_t i = 0; i < projection_len; i++) {
        size_t col_idx = projection[i];
        char data_type = stm_vec[trie_num]->get_column_type(col_idx);
        char enc_type = stm_vec[trie_num]->get_column_encoding(col_idx);
        mdx_vals.push_back(stm_vec[trie_num]->get_value_retriever(col_idx));
        mdx_val_ctxs.push_back(new madras_dv1::val_ctx());
        printf("Col_Idx: %zu, %c, %c\n", col_idx, data_type, enc_type);
        mdx_val_ctxs[i]->init(stm_vec[trie_num]->get_max_val_len(col_idx), true, enc_type == MSE_VINTGB);
        mdx_vals[i]->fill_val_ctx(0, *mdx_val_ctxs[i]);
        append_field(fv, stm_vec[trie_num], col_idx);
        switch (data_type) {
          case MST_BIN:
            ab_vec.push_back(new arrow::BinaryBuilder());
            break;
          case MST_TEXT:
            ab_vec.push_back(new arrow::StringBuilder());
            break;
          case MST_INT:
            ab_vec.push_back(new arrow::Int64Builder());
            break;
          case MST_DECV ... MST_DEC9:
            ab_vec.push_back(new arrow::DoubleBuilder());
            break;
          case MST_DATE_US ... MST_DATE_ISO:
            ab_vec.push_back(new arrow::Date64Builder());
            break;
          // case MST_DATETIME_US ... MST_DATETIME_ISOT_MS:
          //  ab_vec.push_back(new arrow::TimestampBuilder());
          //   break;
        }
      }
      for (size_t i = 0; i < projection_len; i++) {
        ab_vec[i]->Reserve(64);
      }
      projected_schema = arrow::schema(fv);
    }
    ~CustomRecordBatchReader() {
      delete [] projection;
      for (size_t i = 0; i < ab_vec.size(); i++)
        delete ab_vec[i];
      for (size_t i = 0; i < mdx_val_ctxs.size(); i++)
        delete mdx_val_ctxs[i];
    }

    void makeArrayVint(size_t idx, size_t col_idx, char data_type, uint32_t node_id, uint32_t node_id_end, arrow::ArrayVector &array_vec) {
      uint32_t length = node_id_end - node_id;
      int64_t *i64_arr = new int64_t[length];
      madras_dv1::block_retriever_base *block_retriever = stm_vec[trie_num]->get_block_retriever<'D'>(col_idx);
      madras_dv1::mdx_val mv;
      block_retriever->block_operation(node_id, length, mv, i64_arr);
      std::shared_ptr<arrow::Buffer> data_buffer = 
        std::make_shared<arrow::Buffer>(
            reinterpret_cast<const uint8_t*>(i64_arr),
            length * sizeof(int64_t));
        std::shared_ptr<arrow::Buffer> null_bitmap = nullptr;
        std::shared_ptr<arrow::ArrayData> array_data = 
            arrow::ArrayData::Make(
                arrow::int64(),     // data type
                length,             // length of array
                {null_bitmap, data_buffer} // buffers: [null_bitmap, values]
            );
      array_vec.push_back(arrow::MakeArray(array_data));
    }

    void makeArrayOther(size_t idx, char data_type, uint32_t node_id, uint32_t node_id_end, std::shared_ptr<arrow::Array> &array) {
      ab_vec[idx]->Reset();
      for (size_t j = node_id; j < node_id_end; j++) {
        mdx_vals[idx]->next_val(*mdx_val_ctxs[idx]);
        switch (data_type) {
          case MST_BIN:
            ((arrow::BinaryBuilder *) ab_vec[idx])->Append(mdx_val_ctxs[idx]->val->txt_bin, *mdx_val_ctxs[idx]->val_len);
            break;
          case MST_TEXT:
            ((arrow::StringBuilder *) ab_vec[idx])->Append((const char *) mdx_val_ctxs[idx]->val->txt_bin, *mdx_val_ctxs[idx]->val_len);
            break;
          case MST_INT:
            ((arrow::Int64Builder *) ab_vec[idx])->Append(mdx_val_ctxs[idx]->val->i64);
            break;
          case MST_DECV ... MST_DEC9:
            ((arrow::DoubleBuilder *) ab_vec[idx])->Append(mdx_val_ctxs[idx]->val->dbl);
            break;
          case MST_DATE_US ... MST_DATE_ISO:
            ((arrow::Date64Builder *) ab_vec[idx])->Append(mdx_val_ctxs[idx]->val->dbl);
            break;
          // case MST_DATETIME_US ... MST_DATETIME_ISOT_MS:
          //   ab_vec.push_back(new arrow::TimestampBuilder());
          //   break;
        }
      }
      ab_vec[idx]->Finish(&array);
    }
    // This method reads the next record batch
    arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* batch) override {
        if (index_ < batch_count) {
          size_t node_id = index_ * BATCH_SIZE;
          size_t node_id_end = node_id + BATCH_SIZE;
          if (node_id_end >= node_count)
            node_id_end = node_count;
          arrow::ArrayVector array_vec;
          for (size_t i = 0; i < projection_len; i++) {
            size_t col_idx = projection[i];
            char data_type = stm_vec[trie_num]->get_column_type(col_idx);
            char enc_type = stm_vec[trie_num]->get_column_encoding(col_idx);
            std::shared_ptr<arrow::Array> array;
            if (enc_type == 'v')
              makeArrayVint(i, col_idx, data_type, node_id, node_id_end, array_vec);
            else {
              makeArrayOther(i, data_type, node_id, node_id_end, array);
              array_vec.push_back(array);
            }
            // printf("av: %s\n", array_vec[i]->ToString().c_str());
          }
          // Create RecordBatch
          *batch = arrow::RecordBatch::Make(projected_schema, node_id_end - node_id, array_vec);
          index_++;
          return arrow::Status::OK();
        }
        *batch = nullptr;
        return arrow::Status::OK();
    }

    // Return the schema for the record batches
    std::shared_ptr<arrow::Schema> schema() const override {
        return projected_schema;
    }

private:
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
    size_t index_;
};

// Export function for FFI
extern "C" void ExportRecordBatchReader(struct ArrowArrayStream* out_stream,
    const char *file_path, size_t trie_num,
    const size_t* projection, size_t projection_len) {

    for (size_t i = 0; i < projection_len; i++)
      printf("%zu, ", projection[i]);
    printf("\n");
    // Create reader
    auto reader = std::make_shared<CustomRecordBatchReader>(trie_num, projection, projection_len);

    // Export to ArrowArrayStream
    arrow::ExportRecordBatchReader(reader, out_stream);
}

int main(int argc, char *argv[]) {
    try {
        // 1) Set up DuckDB database (in-memory, since we don't store data yet)
        duckdb::DuckDB db(nullptr); // ":memory:" mode
        duckdb::Connection con(db);

        // 2) Prepare ArrowArrayStream from your custom reader
        struct ArrowArrayStream stream;
        const char* file_path = argv[1]; // Replace with your file
        size_t trie_num = 0;
        size_t projection[] = {0, 1, 2}; // Replace with your desired column indices
        size_t projection_len = sizeof(projection) / sizeof(projection[0]);

        ArrowSchema out_schema;
        ExportSchema(&out_schema, file_path);

        ExportRecordBatchReader(&stream, file_path, trie_num, projection, projection_len);

        // 3) Register the Arrow stream in DuckDB as a view using arrow_scan
        //    Note: We use a pointer cast as DuckDB expects a uintptr_t argument.
        auto result = con.TableFunction("arrow_scan1", {
            duckdb::Value::POINTER((uintptr_t)&stream),
            duckdb::Value::POINTER((uintptr_t)&out_schema),
            duckdb::Value::POINTER((uintptr_t)nullptr)  // context, if needed
        });

    // 4) Run queries on the arrow_scan result directly (without storing)
        //    Note: arrow_result here is a DuckDB::TableRef, so you use it in From().
        auto query_result = con.Query("Select * from arrow_scan1()")->Fetch();

        // 5) Print the result
        std::cout << "=== Query Result (First 10 rows) ===\n";
        query_result->Print();

        std::cout << "Demo complete. Data read via DuckDB directly from your Arrow source.\n";

        // Cleanup
        // if (out_schema->release) {
        //     out_schema->release(out_schema);
        // }
        // delete out_schema;

      } catch (std::exception &e) {
        std::cerr << "DuckDB error: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}
