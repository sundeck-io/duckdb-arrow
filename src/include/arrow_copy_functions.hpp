#pragma once

#include "duckdb.hpp"
#include "duckdb/function/copy_function.hpp"

namespace duckdb {

bool IsTypeNotSupported(const LogicalType &type);
vector<unique_ptr<Expression>> ArrowIPCWriteSelect(CopyToSelectInput &input);

} // namespace duckdb
