#define DUCKDB_EXTENSION_MAIN

#include "arrow_extension.hpp"
#include "arrow_stream_buffer.hpp"
#include "arrow_scan_ipc.hpp"
#include "arrow_to_ipc.hpp"
#include "arrow_ipc_copy.hpp"

#include "duckdb.hpp"
#include <memory>
#include <limits>

#include "duckdb/common/arrow/result_arrow_wrapper.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

class ArrowExtension : public Extension {
public:
  void Load(DuckDB &db) override {
    auto &db_instance = *db.instance;

    ExtensionUtil::RegisterFunction(db_instance, ToArrowIPCFunction::GetFunction());
    ExtensionUtil::RegisterFunction(db_instance, ArrowIPCTableFunction::GetFunction());

    // Register Arrow IPC copy function
    CopyFunction function("arrow_ipc");
    function.copy_to_bind = ArrowIPCCopyFunction::Bind;
    function.copy_to_initialize_global = ArrowIPCCopyFunction::InitializeGlobal;
    function.copy_to_initialize_local = ArrowIPCCopyFunction::InitializeLocal;
    function.copy_to_sink = ArrowIPCCopyFunction::Sink;
    function.copy_to_combine = ArrowIPCCopyFunction::Combine;
    function.copy_to_finalize = ArrowIPCCopyFunction::Finalize;
    function.execution_mode = ArrowIPCCopyFunction::ExecutionMode;
    function.extension = "arrow";
    ExtensionUtil::RegisterFunction(db_instance, function);
  }

  std::string Name() override {
    return "arrow";
  }
};

} // namespace duckdb

#ifdef DUCKDB_BUILD_LOADABLE_EXTENSION
extern "C" {

DUCKDB_EXTENSION_API void arrow_init(duckdb::duckdb::DatabaseInstance &db) {
  auto extension = std::unique_ptr<duckdb::ArrowExtension>(new duckdb::ArrowExtension());
  duckdb::DuckDB ddb(db);
  extension->Load(ddb);
}

DUCKDB_EXTENSION_API const char *arrow_version() {
  return duckdb::DuckDB::LibraryVersion();
}

} // extern "C"
#endif

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
