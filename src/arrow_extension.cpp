#define DUCKDB_EXTENSION_MAIN

#include "arrow_extension.hpp"
#include "arrow_stream_buffer.hpp"
#include "arrow_scan_ipc.hpp"
#include "arrow_to_ipc.hpp"
#include "arrow_copy_functions.hpp"

#include "duckdb.hpp"
#include "duckdb/function/copy_function.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/arrow/result_arrow_wrapper.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/function/table/arrow.hpp"
#endif

namespace duckdb {

static void LoadInternal(DatabaseInstance &instance) {
  ExtensionUtil::RegisterFunction(instance, ToArrowIPCFunction::GetFunction());
  ExtensionUtil::RegisterFunction(instance,
                                  ArrowIPCTableFunction::GetFunction());

  CopyFunction function("arrow");
  function.copy_to_select = ArrowIPCWriteSelect;
  function.extension = "arrow";
  ExtensionUtil::RegisterFunction(instance, function);
}

void ArrowExtension::Load(DuckDB &db) { LoadInternal(*db.instance); }
std::string ArrowExtension::Name() { return "arrow"; }

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void arrow_init(duckdb::DatabaseInstance &db) {
  LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *arrow_version() {
  return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
