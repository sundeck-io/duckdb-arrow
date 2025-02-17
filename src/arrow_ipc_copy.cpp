#include "arrow_ipc_copy.hpp"

#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/result_arrow_wrapper.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_properties.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension_util.hpp"

#include "arrow/io/memory.h"
#include "arrow/ipc/options.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/c/bridge.h"

#include <random>

namespace duckdb {
namespace ArrowIPCCopyFunction {

using std::string;
using std::vector;

unique_ptr<FunctionData> Bind(ClientContext &context, CopyFunctionBindInput &input,
                           const vector<string> &names, const vector<LogicalType> &sql_types) {
    auto result = make_uniq<ArrowIPCWriteBindData>();
    result->sql_types = sql_types;
    result->column_names = names;

    // Generate random UUID-like string for file naming
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 15);
    const char *hex = "0123456789abcdef";
    string uuid;
    for (int i = 0; i < 32; i++) {
        uuid += hex[dis(gen)];
        if (i == 7 || i == 11 || i == 15 || i == 19) {
            uuid += '-';
        }
    }
    result->uuid = uuid;

    return std::move(result);
}

unique_ptr<GlobalFunctionData> InitializeGlobal(ClientContext &context, FunctionData &bind_data,
                                                     const string &file_path) {
    return make_uniq<ArrowIPCWriteGlobalState>();
}

unique_ptr<LocalFunctionData> InitializeLocal(ExecutionContext &context, FunctionData &bind_data) {
    auto &arrow_bind = bind_data.Cast<ArrowIPCWriteBindData>();
    return make_uniq<ArrowIPCWriteLocalState>(context.client, arrow_bind.sql_types);
}

void Sink(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                LocalFunctionData &lstate, DataChunk &input) {
    auto &local_state = lstate.Cast<ArrowIPCWriteLocalState>();
    auto &arrow_bind = bind_data.Cast<ArrowIPCWriteBindData>();

    // Append data to the local buffer
    local_state.buffer.Append(local_state.append_state, input);

    // Check if we need to flush based on size
    if (local_state.buffer.SizeInBytes() >= arrow_bind.max_file_size) {
        Combine(context, bind_data, gstate, lstate);
        local_state.buffer.Reset();
        local_state.buffer.InitializeAppend(local_state.append_state);
    }
}

void Combine(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                   LocalFunctionData &lstate) {
    auto &global_state = gstate.Cast<ArrowIPCWriteGlobalState>();
    auto &local_state = lstate.Cast<ArrowIPCWriteLocalState>();
    auto &arrow_bind = bind_data.Cast<ArrowIPCWriteBindData>();

    if (local_state.buffer.Count() == 0) {
        return;
    }

    // Create Arrow schema
    ArrowSchema schema;
    auto properties = context.client.GetClientProperties();
    ArrowConverter::ToArrowSchema(&schema, arrow_bind.sql_types, arrow_bind.column_names, properties);
    auto arrow_schema = arrow::ImportSchema(&schema).ValueOrDie();

    // Create file name with UUID and thread number
    if (local_state.file_name.empty()) {
        local_state.file_name = arrow_bind.uuid + "-" + std::to_string(0) + ".arrow";
    }
    auto &fs = FileSystem::GetFileSystem(context.client);
    auto handle = fs.OpenFile(local_state.file_name, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);

    if (local_state.buffer.Count() == 0) {
        return;
    }
    
    // Convert buffer to Arrow record batch
    duckdb::DataChunk chunk;
    chunk.Initialize(context.client, local_state.buffer.Types());
    local_state.buffer.FetchChunk(0, chunk);
    
    ArrowArray arr;
    duckdb::ArrowConverter::ToArrowArray(chunk, &arr, context.client.GetClientProperties(), {});
    auto record_batch = arrow::ImportRecordBatch(&arr, arrow_schema).ValueOrDie();

    // Write to file using Arrow IPC format
    auto sink = arrow::io::FileOutputStream::Open(handle->path).ValueOrDie();
    auto options = arrow::ipc::IpcWriteOptions::Defaults();
    auto writer = arrow::ipc::MakeFileWriter(sink.get(), arrow_schema, options).ValueOrDie();
    
    auto status = writer->WriteRecordBatch(*record_batch);
    if (!status.ok()) {
        throw duckdb::IOException("Failed to write Arrow IPC record batch: " + status.ToString());
    }
    
    status = writer->Close();
    if (!status.ok()) {
        throw duckdb::IOException("Failed to close Arrow IPC writer: " + status.ToString());
    }
    
    status = sink->Close();
    if (!status.ok()) {
        throw duckdb::IOException("Failed to close Arrow IPC file: " + status.ToString());
    }

    // Add file to list of written files
    {
        std::lock_guard<std::mutex> lock(global_state.lock);
        global_state.written_files.push_back(local_state.file_name);
    }
}

void Finalize(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate) {
    auto &global_state = gstate.Cast<ArrowIPCWriteGlobalState>();
    
    // Create result table with written files
    vector<LogicalType> types = {LogicalType::VARCHAR};
    vector<string> names = {"file_name"};
    auto result_collection = make_uniq<ColumnDataCollection>(context, types);
    DataChunk chunk;
    chunk.Initialize(context, types);
    chunk.SetCardinality(1);
    
    for (const auto &file : global_state.written_files) {
        chunk.SetValue(0, 0, Value(file));
        chunk.Verify();
        result_collection->Append(chunk);
    }
    
    auto result = make_uniq<MaterializedQueryResult>(StatementType::COPY_STATEMENT, StatementProperties(),
                                                   names, std::move(result_collection), context.GetClientProperties());
}

CopyFunctionExecutionMode ExecutionMode(bool preserve_insertion_order, bool supports_batch_index) {
    if (!preserve_insertion_order) {
        return CopyFunctionExecutionMode::PARALLEL_COPY_TO_FILE;
    }
    if (supports_batch_index) {
        return CopyFunctionExecutionMode::BATCH_COPY_TO_FILE;
    }
    return CopyFunctionExecutionMode::REGULAR_COPY_TO_FILE;
}

CopyFunction GetFunction() {
    CopyFunction function("arrow_ipc");
    function.copy_to_bind = Bind;
    function.copy_to_initialize_global = InitializeGlobal;
    function.copy_to_initialize_local = InitializeLocal;
    function.copy_to_sink = Sink;
    function.copy_to_combine = Combine;
    function.copy_to_finalize = Finalize;
    function.execution_mode = ExecutionMode;
    function.extension = "arrow";
    return function;
}

} // namespace ArrowIPCCopyFunction
} // namespace duckdb
