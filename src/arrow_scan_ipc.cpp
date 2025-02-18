#include "arrow_scan_ipc.hpp"

namespace duckdb {

TableFunction ArrowIPCTableFunction::GetFunction() {
  child_list_t<LogicalType> make_buffer_struct_children{
      {"ptr", LogicalType::UBIGINT}, {"size", LogicalType::UBIGINT}};

  TableFunction scan_arrow_ipc_func("arrow", {LogicalType::VARCHAR},
                                    ArrowIPCTableFunction::ArrowScanFunction,
                                    ArrowIPCTableFunction::ArrowScanBind,
                                    ArrowTableFunction::ArrowScanInitGlobal,
                                    ArrowTableFunction::ArrowScanInitLocal);

  scan_arrow_ipc_func.cardinality = ArrowTableFunction::ArrowScanCardinality;
  scan_arrow_ipc_func.projection_pushdown = true;
  scan_arrow_ipc_func.filter_pushdown = false;
  scan_arrow_ipc_func.named_parameters["format"] = LogicalType::VARCHAR;
  scan_arrow_ipc_func.named_parameters["compression"] = LogicalType::VARCHAR;

  return scan_arrow_ipc_func;
}

unique_ptr<FunctionData> ArrowIPCTableFunction::ArrowScanBind(
    ClientContext &context, TableFunctionBindInput &input,
    vector<LogicalType> &return_types, vector<string> &names) {
  auto stream_decoder = make_uniq<BufferingArrowIPCStreamDecoder>();

  // Get file path from input
  auto file_path = input.inputs[0].GetValue<string>();
  auto &fs = FileSystem::GetFileSystem(context);
  auto handle = fs.OpenFile(file_path, FileFlags::FILE_FLAGS_READ);

  // Read file into memory
  auto file_size = fs.GetFileSize(*handle);
  vector<uint8_t> buffer(file_size);
  handle->Read(buffer.data(), file_size);

  // Feed file into decoder
  auto consume_result = stream_decoder->Consume(buffer.data(), file_size);
  if (!consume_result.ok()) {
    throw IOException("Invalid Arrow IPC file");
  }

  if (!stream_decoder->buffer()->is_eos()) {
    throw IOException("Arrow IPC file is incomplete");
  }

  // These are the params I need to produce from the ipc buffers using the
  // WebDB.cc code
  auto stream_factory_ptr = (uintptr_t)&stream_decoder->buffer();
  auto stream_factory_produce =
      (stream_factory_produce_t)&ArrowIPCStreamBufferReader::CreateStream;
  auto stream_factory_get_schema =
      (stream_factory_get_schema_t)&ArrowIPCStreamBufferReader::GetSchema;
  auto result = make_uniq<ArrowIPCScanFunctionData>(stream_factory_produce,
                                                    stream_factory_ptr);

  // Store decoder
  result->stream_decoder = std::move(stream_decoder);

  // TODO Everything below this is identical to the bind in
  // duckdb/src/function/table/arrow.cpp
  auto &data = *result;
  stream_factory_get_schema((ArrowArrayStream *)stream_factory_ptr,
                            data.schema_root.arrow_schema);
  for (idx_t col_idx = 0;
       col_idx < (idx_t)data.schema_root.arrow_schema.n_children; col_idx++) {
    auto &schema = *data.schema_root.arrow_schema.children[col_idx];
    if (!schema.release) {
      throw InvalidInputException("arrow_scan: released schema passed");
    }
    auto arrow_type =
        ArrowType::GetArrowLogicalType(DBConfig::GetConfig(context), schema);

    if (schema.dictionary) {
      auto dictionary_type = ArrowType::GetArrowLogicalType(
          DBConfig::GetConfig(context), *schema.dictionary);
      return_types.emplace_back(dictionary_type->GetDuckType());
      arrow_type->SetDictionary(std::move(dictionary_type));
    } else {
      return_types.emplace_back(arrow_type->GetDuckType());
    }
    result->arrow_table.AddColumn(col_idx, std::move(arrow_type));
    auto format = string(schema.format);
    auto name = string(schema.name);
    if (name.empty()) {
      name = string("v") + to_string(col_idx);
    }
    names.push_back(name);
  }
  QueryResult::DeduplicateColumns(names);
  return std::move(result);
}

// Same as regular arrow scan, except ArrowToDuckDB call TODO: refactor to allow
// nicely overriding this
void ArrowIPCTableFunction::ArrowScanFunction(ClientContext &context,
                                              TableFunctionInput &data_p,
                                              DataChunk &output) {
  if (!data_p.local_state) {
    return;
  }
  auto &data = data_p.bind_data->CastNoConst<ArrowScanFunctionData>();
  auto &state = data_p.local_state->Cast<ArrowScanLocalState>();
  auto &global_state = data_p.global_state->Cast<ArrowScanGlobalState>();

  //! Out of tuples in this chunk
  if (state.chunk_offset >= (idx_t)state.chunk->arrow_array.length) {
    if (!ArrowScanParallelStateNext(context, data_p.bind_data.get(), state,
                                    global_state)) {
      return;
    }
  }
  int64_t output_size =
      MinValue<int64_t>(STANDARD_VECTOR_SIZE,
                        state.chunk->arrow_array.length - state.chunk_offset);
  data.lines_read += output_size;
  if (global_state.CanRemoveFilterColumns()) {
    state.all_columns.Reset();
    state.all_columns.SetCardinality(output_size);
    ArrowToDuckDB(state, data.arrow_table.GetColumns(), state.all_columns,
                  data.lines_read - output_size, false);
    output.ReferenceColumns(state.all_columns, global_state.projection_ids);
  } else {
    output.SetCardinality(output_size);
    ArrowToDuckDB(state, data.arrow_table.GetColumns(), output,
                  data.lines_read - output_size, false);
  }

  output.Verify();
  state.chunk_offset += output.size();
}

} // namespace duckdb
