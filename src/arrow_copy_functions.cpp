#include "arrow_copy_functions.hpp"
#include "arrow_to_ipc.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "arrow/c/bridge.h"
#include "arrow/io/file.h"
#include "arrow/ipc/writer.h"

namespace duckdb {

bool IsTypeNotSupported(const LogicalType &type) {
  // Arrow supports all DuckDB types through arrow_to_ipc.cpp
  return false;
}

unique_ptr<FunctionData>
ArrowIPCWriteBind(ClientContext &context, CopyFunctionBindInput &input,
                  const vector<string> &names,
                  const vector<LogicalType> &sql_types) {
  auto bind_data = make_uniq<ArrowIPCWriteBindData>();
  bind_data->sql_types = sql_types;
  bind_data->column_names = names;
  bind_data->preserve_insertion_order =
      DBConfig::GetConfig(context).options.preserve_insertion_order;
  return std::move(bind_data);
}

unique_ptr<GlobalFunctionData>
ArrowIPCWriteInitializeGlobal(ClientContext &context, FunctionData &bind_data,
                              const string &file_path) {
  auto &arrow_bind = bind_data.Cast<ArrowIPCWriteBindData>();
  auto global_state = make_uniq<ArrowIPCWriteGlobalState>();

  // Create Arrow schema directly
  vector<std::shared_ptr<arrow::Field>> fields;
  for (idx_t i = 0; i < arrow_bind.sql_types.size(); i++) {
    auto sql_type = arrow_bind.sql_types[i];
    auto field_name = arrow_bind.column_names[i];

    // Convert DuckDB type to Arrow type
    std::shared_ptr<arrow::DataType> arrow_type;
    switch (sql_type.id()) {
      case LogicalTypeId::INTEGER:
        arrow_type = arrow::int32();
        break;
      case LogicalTypeId::BIGINT:
        arrow_type = arrow::int64();
        break;
      // Add more type conversions as needed
      default:
        throw IOException("Unsupported type for Arrow IPC: " +
                        sql_type.ToString());
    }

    fields.push_back(arrow::field(field_name, arrow_type));
  }

  // Create final schema
  global_state->schema = std::make_shared<arrow::Schema>(fields);

  // Create Arrow file writer
  auto &fs = FileSystem::GetFileSystem(context);
  auto handle =
      fs.OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE |
                                 FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
  auto result = arrow::io::FileOutputStream::Open(handle->path);
  if (!result.ok()) {
    throw IOException("Failed to open Arrow IPC output file: " +
                      result.status().ToString());
  }
  auto output_stream = result.ValueOrDie();

  // Initialize writer with schema
  auto options = arrow::ipc::IpcWriteOptions::Defaults();
  auto writer_result =
      arrow::ipc::MakeFileWriter(output_stream, global_state->schema, options);
  if (!writer_result.ok()) {
    throw IOException("Failed to create Arrow IPC writer: " +
                      writer_result.status().ToString());
  }
  global_state->writer = std::move(writer_result).ValueOrDie();

  return std::move(global_state);
}

vector<unique_ptr<Expression>> ArrowIPCWriteSelect(CopyToSelectInput &input) {
  vector<unique_ptr<Expression>> result;
  bool any_change = false;

  for (auto &expr : input.select_list) {
    const auto &type = expr->return_type;
    const auto &name = expr->GetAlias();

    // All types supported by Arrow IPC
    result.push_back(std::move(expr));
  }
  // If no changes were made, return empty vector to avoid unnecessary
  // projection
  return {};
}

void ArrowIPCWriteSink(ExecutionContext &context, FunctionData &bind_data,
                       GlobalFunctionData &gstate, LocalFunctionData &lstate,
                       DataChunk &input) {
  auto &arrow_bind = bind_data.Cast<ArrowIPCWriteBindData>();
  auto &global_state = gstate.Cast<ArrowIPCWriteGlobalState>();
  auto &local_state = lstate.Cast<ArrowIPCWriteLocalState>();

  if (!local_state.appender) {
    // Create appender with column types directly
    local_state.appender =
        make_uniq<ArrowAppender>(arrow_bind.sql_types, arrow_bind.chunk_size,
                                 context.client.GetClientProperties(),
                                 ArrowTypeExtensionData::GetExtensionTypes(
                                     context.client, arrow_bind.sql_types));
  }

  // Append input chunk directly
  local_state.appender->Append(input, 0, input.size(), input.size());
  local_state.current_count += input.size();

  // If chunk size is reached, flush to Arrow IPC file
  if (local_state.current_count >= arrow_bind.chunk_size) {
    // Create record batch and write
    ArrowArray arr = local_state.appender->Finalize();
    auto record_batch_result =
        arrow::ImportRecordBatch(&arr, global_state.schema);
    if (!record_batch_result.ok()) {
      throw IOException("Failed to import Arrow record batch: " +
                        record_batch_result.status().ToString());
    }
    auto record_batch = record_batch_result.ValueOrDie();
    auto status = global_state.writer->WriteRecordBatch(*record_batch);
    if (!status.ok()) {
      throw IOException("Failed to write Arrow IPC record batch: " +
                        status.ToString());
    }

    // Reset local state and ensure proper cleanup
    local_state.appender.reset();
    local_state.current_count = 0;
  }
}

void ArrowIPCWriteCombine(ExecutionContext &context, FunctionData &bind_data,
                          GlobalFunctionData &gstate,
                          LocalFunctionData &lstate) {
  auto &arrow_bind = bind_data.Cast<ArrowIPCWriteBindData>();
  auto &global_state = gstate.Cast<ArrowIPCWriteGlobalState>();
  auto &local_state = lstate.Cast<ArrowIPCWriteLocalState>();

  // Flush any remaining data
  if (local_state.appender && local_state.current_count > 0) {
    ArrowArray arr = local_state.appender->Finalize();
    auto record_batch =
        arrow::ImportRecordBatch(&arr, global_state.schema).ValueOrDie();
    auto status = global_state.writer.get()->WriteRecordBatch(*record_batch);
    if (!status.ok()) {
      throw IOException("Failed to write Arrow IPC record batch: " +
                        status.ToString());
    }
  }
}

unique_ptr<LocalFunctionData>
ArrowIPCWriteInitializeLocal(ExecutionContext &context,
                             FunctionData &bind_data) {
  auto &arrow_bind = bind_data.Cast<ArrowIPCWriteBindData>();
  return make_uniq<ArrowIPCWriteLocalState>(context.client,
                                            arrow_bind.sql_types);
}

void ArrowIPCWriteFinalize(ClientContext &context, FunctionData &bind_data,
                           GlobalFunctionData &gstate) {
  auto &global_state = gstate.Cast<ArrowIPCWriteGlobalState>();

  // Ensure writer is properly closed and resources are released
  if (global_state.writer) {
    // Flush any remaining data and close the writer
    auto status = global_state.writer->Close();
    if (!status.ok()) {
      throw IOException("Failed to finalize Arrow IPC file: " +
                        status.ToString());
    }
    // Reset writer to release resources
    global_state.writer.reset();
  }

  // Clean up schema
  if (global_state.schema) {
    global_state.schema.reset();
  }
}

CopyFunctionExecutionMode
ArrowIPCWriteExecutionMode(bool preserve_insertion_order,
                           bool supports_batch_index) {
  return CopyFunctionExecutionMode::REGULAR_COPY_TO_FILE;
}

void ArrowIPCCopySerialize(Serializer &serializer,
                           const FunctionData &bind_data_p,
                           const CopyFunction &function) {
  auto &bind_data = bind_data_p.Cast<ArrowIPCWriteBindData>();
  serializer.WriteProperty(100, "sql_types", bind_data.sql_types);
  serializer.WriteProperty(101, "column_names", bind_data.column_names);
  serializer.WriteProperty(102, "chunk_size", bind_data.chunk_size);
  serializer.WriteProperty(103, "preserve_insertion_order",
                           bind_data.preserve_insertion_order);
}

unique_ptr<FunctionData> ArrowIPCCopyDeserialize(Deserializer &deserializer,
                                                 CopyFunction &function) {
  auto data = make_uniq<ArrowIPCWriteBindData>();
  data->sql_types =
      deserializer.ReadProperty<vector<LogicalType>>(100, "sql_types");
  data->column_names =
      deserializer.ReadProperty<vector<string>>(101, "column_names");
  data->chunk_size = deserializer.ReadProperty<idx_t>(102, "chunk_size");
  data->preserve_insertion_order =
      deserializer.ReadProperty<bool>(103, "preserve_insertion_order");
  return std::move(data);
}

unique_ptr<FunctionData>
ArrowIPCCopyFromBind(ClientContext &context, CopyInfo &info,
                     vector<string> &names,
                     vector<LogicalType> &expected_types) {
  auto &fs = FileSystem::GetFileSystem(context);
  auto handle = fs.OpenFile(info.file_path, FileFlags::FILE_FLAGS_READ);
  auto file_size = fs.GetFileSize(*handle);

  // Read file into memory
  vector<uint8_t> file_buffer(file_size);
  handle->Read(file_buffer.data(), file_size);

  // Create stream decoder and buffer
  auto stream_decoder = make_uniq<BufferingArrowIPCStreamDecoder>();
  auto consume_result = stream_decoder->Consume(file_buffer.data(), file_size);
  if (!consume_result.ok()) {
    throw IOException("Invalid Arrow IPC file");
  }

  // Get stream factory pointer and functions
  auto stream_factory_produce =
      (stream_factory_produce_t)&ArrowIPCStreamBufferReader::CreateStream;
  auto stream_factory_get_schema =
      (stream_factory_get_schema_t)&ArrowIPCStreamBufferReader::GetSchema;

  // Store decoder and get buffer pointer
  auto result = make_uniq<ArrowIPCScanFunctionData>(stream_factory_produce, 0);
  result->stream_decoder = std::move(stream_decoder);

  // Store buffer pointer - keep the shared_ptr alive
  auto ipc_buffer = result->stream_decoder->buffer();
  // Create reader and store it in a shared_ptr to keep it alive
  auto reader = duckdb::make_shared_ptr<ArrowIPCStreamBufferReader>(ipc_buffer);
  auto *reader_raw = reader.get();
  result->stream_factory_ptr = (uintptr_t)reader_raw;

  // Store reader in the dependency field to keep it alive and manage its
  // lifecycle
  result->dependency = std::move(reader);

  // Set up schema using the buffer pointer
  stream_factory_get_schema((ArrowArrayStream *)result->stream_factory_ptr,
                            result->schema_root.arrow_schema);

  // Ensure schema is properly initialized
  if (!result->schema_root.arrow_schema.release) {
    throw IOException("Failed to initialize Arrow schema");
  }

  // Let ArrowSchemaWrapper handle schema cleanup
  // Do not modify release callbacks - they will be handled by the wrapper
  for (idx_t i = 0; i < result->schema_root.arrow_schema.n_children; i++) {
    if (!result->schema_root.arrow_schema.children[i]->release) {
      throw InvalidInputException("arrow_scan: released schema passed");
    }
  }

  // Set up column info
  for (idx_t col_idx = 0;
       col_idx < (idx_t)result->schema_root.arrow_schema.n_children;
       col_idx++) {
    auto &schema = *result->schema_root.arrow_schema.children[col_idx];
    if (!schema.release) {
      throw InvalidInputException("arrow_scan: released schema passed");
    }
    auto arrow_type =
        ArrowType::GetArrowLogicalType(DBConfig::GetConfig(context), schema);

    if (schema.dictionary) {
      auto dictionary_type = ArrowType::GetArrowLogicalType(
          DBConfig::GetConfig(context), *schema.dictionary);
      expected_types.emplace_back(dictionary_type->GetDuckType());
      arrow_type->SetDictionary(std::move(dictionary_type));
    } else {
      expected_types.emplace_back(arrow_type->GetDuckType());
    }
    result->arrow_table.AddColumn(col_idx, std::move(arrow_type));
    auto name = string(schema.name);
    if (name.empty()) {
      name = string("v") + to_string(col_idx);
    }
    names.push_back(name);
  }
  QueryResult::DeduplicateColumns(names);

  return std::move(result);
}

unique_ptr<FunctionData>
ArrowIPCCopyFromFunction(ClientContext &context, vector<string> &names,
                         vector<LogicalType> &expected_types) {
  // Create a dummy bind data since we don't have file data yet
  auto result = make_uniq<ArrowIPCScanFunctionData>(nullptr, 0);
  return std::move(result);
}

unique_ptr<PreparedBatchData>
ArrowIPCWritePrepareBatch(ClientContext &context, FunctionData &bind_data,
                          GlobalFunctionData &gstate,
                          unique_ptr<ColumnDataCollection> input_collection) {
  auto &arrow_bind = bind_data.Cast<ArrowIPCWriteBindData>();
  auto result = make_uniq<ArrowIPCWriteBatchData>();

  // Store the collection
  result->collection = std::move(input_collection);

  // Create appender with column types directly
  result->appender = make_uniq<ArrowAppender>(
      arrow_bind.sql_types, arrow_bind.chunk_size,
      context.GetClientProperties(),
      ArrowTypeExtensionData::GetExtensionTypes(context, arrow_bind.sql_types));

  return std::move(result);
}

void ArrowIPCWriteFlushBatch(ClientContext &context, FunctionData &bind_data,
                             GlobalFunctionData &gstate,
                             PreparedBatchData &batch_p) {
  auto &global_state = gstate.Cast<ArrowIPCWriteGlobalState>();
  auto &batch = batch_p.Cast<ArrowIPCWriteBatchData>();

  // Process each chunk in the collection
  for (auto &chunk : batch.collection->Chunks()) {
    // Append chunk directly
    batch.appender->Append(chunk, 0, chunk.size(), chunk.size());
  }

  // Create record batch and write
  ArrowArray arr = batch.appender->Finalize();
  auto record_batch_result =
      arrow::ImportRecordBatch(&arr, global_state.schema);
  if (!record_batch_result.ok()) {
    throw IOException("Failed to import Arrow record batch: " +
                      record_batch_result.status().ToString());
  }
  auto record_batch = record_batch_result.ValueOrDie();
  auto status = global_state.writer->WriteRecordBatch(*record_batch);
  if (!status.ok()) {
    throw IOException("Failed to write Arrow IPC record batch: " +
                      status.ToString());
  }
}

} // namespace duckdb
