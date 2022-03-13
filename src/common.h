#ifndef _COMMON_H
#define _COMMON_H

#include <sqlite3.h>

#include <memory>

// clang-format off
struct sqlite3_delete { void operator ()(sqlite3 *ptr) const { sqlite3_close(ptr); } };
struct sqlite3_stmt_delete { void operator ()(sqlite3_stmt *ptr) const { sqlite3_finalize(ptr); } };
// clang-format on

using query_ptr_t = std::unique_ptr<sqlite3_stmt, sqlite3_stmt_delete>;

#endif//_COMMON_H
