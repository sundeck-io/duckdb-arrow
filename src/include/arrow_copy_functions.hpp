#pragma once

#include "duckdb.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "arrow_scan_ipc.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

namespace duckdb {

struct ArrowIPCWriteBindData : public TableFunctionData {
  //! The SQL types to write
  vector<LogicalType> sql_types;
  //! The column names to write
  vector<string> column_names;
  //! The chunk size to use for writing
  idx_t chunk_size = STANDARD_VECTOR_SIZE;
  //! Whether to preserve insertion order
  bool preserve_insertion_order = false;
};

struct ArrowIPCWriteGlobalState : public GlobalFunctionData {
  //! The Arrow schema
  std::shared_ptr<arrow::Schema> schema;
  //! The Arrow IPC file writer
  std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
  //! Current chunk being written
  idx_t current_chunk = 0;
};

struct ArrowIPCWriteLocalState : public LocalFunctionData {
  explicit ArrowIPCWriteLocalState(ClientContext &context,
                                   const vector<LogicalType> &types) {
    appender = make_uniq<ArrowAppender>(
        types, STANDARD_VECTOR_SIZE, context.GetClientProperties(),
        ArrowTypeExtensionData::GetExtensionTypes(context, types));
  }
  //! The Arrow appender for creating record batches
  unique_ptr<ArrowAppender> appender;
  //! Current count of rows in the appender
  idx_t current_count = 0;
};

bool IsTypeNotSupported(const LogicalType &type);
vector<unique_ptr<Expression>> ArrowIPCWriteSelect(CopyToSelectInput &input);
unique_ptr<FunctionData>
ArrowIPCWriteBind(ClientContext &context, CopyFunctionBindInput &input,
                  const vector<string> &names,
                  const vector<LogicalType> &sql_types);
unique_ptr<GlobalFunctionData>
ArrowIPCWriteInitializeGlobal(ClientContext &context, FunctionData &bind_data,
                              const string &file_path);
unique_ptr<LocalFunctionData>
ArrowIPCWriteInitializeLocal(ExecutionContext &context,
                             FunctionData &bind_data);
void ArrowIPCWriteSink(ExecutionContext &context, FunctionData &bind_data,
                       GlobalFunctionData &gstate, LocalFunctionData &lstate,
                       DataChunk &input);
void ArrowIPCWriteCombine(ExecutionContext &context, FunctionData &bind_data,
                          GlobalFunctionData &gstate,
                          LocalFunctionData &lstate);
void ArrowIPCWriteFinalize(ClientContext &context, FunctionData &bind_data,
                           GlobalFunctionData &gstate);

CopyFunctionExecutionMode
ArrowIPCWriteExecutionMode(bool preserve_insertion_order,
                           bool supports_batch_index);

void ArrowIPCCopySerialize(Serializer &serializer,
                           const FunctionData &bind_data_p,
                           const CopyFunction &function);

unique_ptr<FunctionData> ArrowIPCCopyDeserialize(Deserializer &deserializer,
                                                 CopyFunction &function);

unique_ptr<FunctionData>
ArrowIPCCopyFromBind(ClientContext &context, CopyInfo &info,
                     vector<string> &names,
                     vector<LogicalType> &expected_types);

unique_ptr<FunctionData>
ArrowIPCCopyFromFunction(ClientContext &context, vector<string> &names,
                         vector<LogicalType> &expected_types);

} // namespace duckdb
