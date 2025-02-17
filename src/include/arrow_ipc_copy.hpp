#pragma once

#include "duckdb.hpp"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/buffer.h"
#include "arrow/io/file.h"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include <memory>

namespace duckdb {

struct ArrowIPCWriteBindData : public TableFunctionData {
    vector<LogicalType> sql_types;
    vector<string> column_names;
    idx_t max_file_size = 1024 * 1024; // 1MB default
    string uuid;
};

struct ArrowIPCWriteGlobalState : public GlobalFunctionData {
    vector<string> written_files;
    mutex lock;
};

struct ArrowIPCWriteLocalState : public LocalFunctionData {
    explicit ArrowIPCWriteLocalState(ClientContext &context, const vector<LogicalType> &types)
        : buffer(BufferAllocator::Get(context), types) {
        buffer.InitializeAppend(append_state);
    }

    ColumnDataCollection buffer;
    ColumnDataAppendState append_state;
    string file_name;
};

namespace ArrowIPCCopyFunction {

CopyFunction GetFunction();

unique_ptr<FunctionData> Bind(ClientContext &context, CopyFunctionBindInput &input,
                           const vector<string> &names, const vector<LogicalType> &sql_types);

unique_ptr<GlobalFunctionData> InitializeGlobal(ClientContext &context, FunctionData &bind_data,
                                             const string &file_path);

unique_ptr<LocalFunctionData> InitializeLocal(ExecutionContext &context, FunctionData &bind_data);

void Sink(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
        LocalFunctionData &lstate, DataChunk &input);

void Combine(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
           LocalFunctionData &lstate);

void Finalize(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate);

CopyFunctionExecutionMode ExecutionMode(bool preserve_insertion_order, bool supports_batch_index);

} // namespace ArrowIPCCopyFunction
