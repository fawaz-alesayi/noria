/**
 * noria.cpp - Noria integration for better-sqlite3
 *
 * This file provides the C++ wrapper around the Noria FFI library,
 * enabling transparent cache lookups and CDC capture.
 *
 * Incremental CDC Architecture:
 * - Pre-update hook captures INSERT/UPDATE/DELETE changes directly
 * - Changes propagate incrementally through the dataflow graph
 * - Rollback hook clears pending changes on transaction rollback
 * - Use { fresh: true } option to call Flush() before reads when consistency is needed
 */

#include "../noria-ffi/noria.h"
#include <vector>
#include <string>
#include <cstring>
#include <cctype>
// ============================================================================
// C++ Profiling Infrastructure
// ============================================================================
// Uses clock_gettime instead of <chrono> to avoid conflicts with
// the better-sqlite3 macros (#define first() 0 in macros.cpp).

// C++ profiling: disabled by default for production performance.
// To enable, build with: npm run build-release -- --DNORIA_CPP_PROFILING=1
// Or use: sudo perf record -g node your-benchmark.js (recommended)
#ifndef NORIA_CPP_PROFILING
#define NORIA_CPP_PROFILING 0
#endif

#if NORIA_CPP_PROFILING

#include <time.h>
#include <stdint.h>
#include <pthread.h>

// Simple mutex wrapper using pthread (avoids <mutex> header)
class SimpleMutex {
public:
    SimpleMutex() { pthread_mutex_init(&mutex_, nullptr); }
    ~SimpleMutex() { pthread_mutex_destroy(&mutex_); }
    void lock() { pthread_mutex_lock(&mutex_); }
    void unlock() { pthread_mutex_unlock(&mutex_); }
private:
    pthread_mutex_t mutex_;
};

class SimpleLockGuard {
public:
    explicit SimpleLockGuard(SimpleMutex& m) : mutex_(m) { mutex_.lock(); }
    ~SimpleLockGuard() { mutex_.unlock(); }
private:
    SimpleMutex& mutex_;
};

// Get current time in nanoseconds
static inline uint64_t get_time_ns() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

// Timing statistics for a single span
struct CppTimingStats {
    uint64_t total_ns;
    uint64_t call_count;
    uint64_t max_ns;
    uint64_t min_ns;

    CppTimingStats() : total_ns(0), call_count(0), max_ns(0), min_ns(UINT64_MAX) {}
};

// Global timing data storage
class CppProfiler {
public:
    static CppProfiler& instance() {
        static CppProfiler profiler;
        return profiler;
    }

    void record(const char* name, uint64_t elapsed_ns) {
        SimpleLockGuard lock(mutex_);

        // Find or create stats entry
        int idx = find_or_create(name);
        if (idx < 0) return;

        CppTimingStats& stats = stats_[idx];
        stats.total_ns += elapsed_ns;
        stats.call_count++;
        if (elapsed_ns > stats.max_ns) stats.max_ns = elapsed_ns;
        if (elapsed_ns < stats.min_ns) stats.min_ns = elapsed_ns;
    }

    void reset() {
        SimpleLockGuard lock(mutex_);
        count_ = 0;
    }

    std::string report() {
        SimpleLockGuard lock(mutex_);

        std::string result = "\n╔══════════════════════════════════════════════════════════════════════╗\n";
        result += "║                     C++ LAYER TIMING PROFILE                         ║\n";
        result += "╚══════════════════════════════════════════════════════════════════════╝\n\n";
        result += "Span                          │ Calls    │   Total    │    Avg    │    Max\n";
        result += "──────────────────────────────┼──────────┼────────────┼───────────┼───────────\n";

        for (int i = 0; i < count_; i++) {
            const CppTimingStats& stats = stats_[i];
            double avg = stats.call_count > 0 ? (double)stats.total_ns / stats.call_count : 0;

            char line[256];
            snprintf(line, sizeof(line), "%-29s │ %8lu │ %8.2fms │ %7.1fμs │ %7.1fμs\n",
                     names_[i],
                     (unsigned long)stats.call_count,
                     stats.total_ns / 1e6,
                     avg / 1e3,
                     stats.max_ns / 1e3);
            result += line;
        }
        return result;
    }

private:
    static const int MAX_SPANS = 64;
    SimpleMutex mutex_;
    const char* names_[MAX_SPANS];
    CppTimingStats stats_[MAX_SPANS];
    int count_ = 0;

    int find_or_create(const char* name) {
        // Linear search (acceptable for small number of spans)
        for (int i = 0; i < count_; i++) {
            if (strcmp(names_[i], name) == 0) return i;
        }
        // Create new entry
        if (count_ >= MAX_SPANS) return -1;
        names_[count_] = name;
        stats_[count_] = CppTimingStats();
        return count_++;
    }
};

// RAII timing guard
class CppProfileGuard {
public:
    explicit CppProfileGuard(const char* name) : name_(name), start_(get_time_ns()) {}

    ~CppProfileGuard() {
        uint64_t elapsed = get_time_ns() - start_;
        CppProfiler::instance().record(name_, elapsed);
    }

private:
    const char* name_;
    uint64_t start_;
};

#define CPP_PROFILE(name) CppProfileGuard _cpp_guard_##__LINE__(name)
#define CPP_PROFILE_RESET() CppProfiler::instance().reset()
#define CPP_PROFILE_REPORT() CppProfiler::instance().report()

#else
// No-op when profiling disabled
#define CPP_PROFILE(name)
#define CPP_PROFILE_RESET()
#define CPP_PROFILE_REPORT() std::string("")
#endif

// Forward declaration of Noria class for callback
class Noria;

// Forward declaration of pre-update hook
extern "C" void PreUpdateHookCallback(
    void* user_data,
    sqlite3* db,
    int op,
    const char* db_name,
    const char* table_name,
    sqlite3_int64 rowid1,
    sqlite3_int64 rowid2
);

// Forward declaration of rollback hook (clears CDC queue on rollback)
extern "C" void RollbackHookCallback(void* user_data);

// Bind NoriaValue to sqlite3_stmt parameter
static void BindNoriaValue(sqlite3_stmt* stmt, int idx, const NoriaValue& nv) {
    switch (nv.value_type) {
        case NORIA_NULL:
            sqlite3_bind_null(stmt, idx);
            break;
        case NORIA_INTEGER:
            sqlite3_bind_int64(stmt, idx, nv.int_value);
            break;
        case NORIA_FLOAT:
            sqlite3_bind_double(stmt, idx, nv.float_value);
            break;
        case NORIA_TEXT:
            sqlite3_bind_text(stmt, idx, nv.text_ptr, nv.text_len, SQLITE_TRANSIENT);
            break;
        case NORIA_BLOB:
            sqlite3_bind_blob(stmt, idx, nv.blob_ptr, nv.blob_len, SQLITE_TRANSIENT);
            break;
    }
}

// Convert sqlite3 column to NoriaValue (for upquery results)
static NoriaValue SqliteColumnToNoriaValue(sqlite3_stmt* stmt, int col) {
    NoriaValue nv;
    memset(&nv, 0, sizeof(nv));
    int type = sqlite3_column_type(stmt, col);
    switch (type) {
        case SQLITE_NULL:
            nv.value_type = NORIA_NULL;
            break;
        case SQLITE_INTEGER:
            nv.value_type = NORIA_INTEGER;
            nv.int_value = sqlite3_column_int64(stmt, col);
            break;
        case SQLITE_FLOAT:
            nv.value_type = NORIA_FLOAT;
            nv.float_value = sqlite3_column_double(stmt, col);
            break;
        case SQLITE_TEXT:
            nv.value_type = NORIA_TEXT;
            // Note: These pointers are only valid until next step/finalize
            // The Rust side will copy the data in noria_rows_add_row
            nv.text_ptr = reinterpret_cast<const char*>(sqlite3_column_text(stmt, col));
            nv.text_len = sqlite3_column_bytes(stmt, col);
            break;
        case SQLITE_BLOB:
            nv.value_type = NORIA_BLOB;
            nv.blob_ptr = reinterpret_cast<const uint8_t*>(sqlite3_column_blob(stmt, col));
            nv.blob_len = sqlite3_column_bytes(stmt, col);
            break;
        default:
            nv.value_type = NORIA_NULL;
    }
    return nv;
}

// Pre-update hook CDC: Direct change capture without session extension overhead
// The pre-update hook provides values directly without SQL parsing

// Forward declaration of upquery callback
static int UpqueryCallbackImpl(
    void* user_data,
    const char* sql,
    const NoriaValue* params,
    int param_count,
    void** out_rows,
    int* out_row_count
);

// Noria wrapper class that manages engine lifetime and provides convenient methods
class Noria {
public:
    Noria(sqlite3* db) : handle_(nullptr), db_(db), enabled_(true), has_any_views_(false), preupdate_hook_enabled_(false) {
        handle_ = noria_create(db);
        if (!handle_) {
            enabled_ = false;
        } else {
            // Register upquery callback so Rust can call back to execute SQLite queries
            noria_set_upquery_callback(handle_, UpqueryCallbackImpl, this);
            // Pre-update hook is registered lazily when first view is registered
        }
    }

    // Process CDC directly in pre-update hook (no session extension overhead)
    // This is called BEFORE the actual database change occurs
    void ProcessPreUpdateChange(const char* table_name, int op) {
        if (!db_ || !table_name || !has_any_views_) return;

        // Only process for tables that have views
        if (noria_table_has_views(handle_, table_name) == 0) return;

        int n_cols = sqlite3_preupdate_count(db_);
        if (n_cols <= 0) return;

        // Temporary storage for values (text/blob need to stay alive during queue call)
        std::vector<NoriaValue> values(n_cols);
        std::vector<std::string> text_storage(n_cols);
        std::vector<std::vector<uint8_t>> blob_storage(n_cols);

        switch (op) {
            case SQLITE_INSERT: {
                // Get new values for INSERT
                for (int i = 0; i < n_cols; i++) {
                    sqlite3_value* val = nullptr;
                    if (sqlite3_preupdate_new(db_, i, &val) == SQLITE_OK && val) {
                        int type = sqlite3_value_type(val);
                        switch (type) {
                            case SQLITE_INTEGER:
                                values[i].value_type = NORIA_INTEGER;
                                values[i].int_value = sqlite3_value_int64(val);
                                break;
                            case SQLITE_FLOAT:
                                values[i].value_type = NORIA_FLOAT;
                                values[i].float_value = sqlite3_value_double(val);
                                break;
                            case SQLITE_TEXT: {
                                const char* text = reinterpret_cast<const char*>(sqlite3_value_text(val));
                                int len = sqlite3_value_bytes(val);
                                text_storage[i] = std::string(text, len);
                                values[i].value_type = NORIA_TEXT;
                                values[i].text_ptr = text_storage[i].c_str();
                                values[i].text_len = len;
                                break;
                            }
                            case SQLITE_BLOB: {
                                const uint8_t* blob = reinterpret_cast<const uint8_t*>(sqlite3_value_blob(val));
                                int len = sqlite3_value_bytes(val);
                                blob_storage[i].assign(blob, blob + len);
                                values[i].value_type = NORIA_BLOB;
                                values[i].blob_ptr = blob_storage[i].data();
                                values[i].blob_len = len;
                                break;
                            }
                            default:
                                values[i].value_type = NORIA_NULL;
                        }
                    } else {
                        values[i].value_type = NORIA_NULL;
                    }
                }
                noria_queue_insert(handle_, table_name, values.data(), n_cols);
                break;
            }

            case SQLITE_DELETE: {
                // Get old values for DELETE
                for (int i = 0; i < n_cols; i++) {
                    sqlite3_value* val = nullptr;
                    if (sqlite3_preupdate_old(db_, i, &val) == SQLITE_OK && val) {
                        int type = sqlite3_value_type(val);
                        switch (type) {
                            case SQLITE_INTEGER:
                                values[i].value_type = NORIA_INTEGER;
                                values[i].int_value = sqlite3_value_int64(val);
                                break;
                            case SQLITE_FLOAT:
                                values[i].value_type = NORIA_FLOAT;
                                values[i].float_value = sqlite3_value_double(val);
                                break;
                            case SQLITE_TEXT: {
                                const char* text = reinterpret_cast<const char*>(sqlite3_value_text(val));
                                int len = sqlite3_value_bytes(val);
                                text_storage[i] = std::string(text, len);
                                values[i].value_type = NORIA_TEXT;
                                values[i].text_ptr = text_storage[i].c_str();
                                values[i].text_len = len;
                                break;
                            }
                            case SQLITE_BLOB: {
                                const uint8_t* blob = reinterpret_cast<const uint8_t*>(sqlite3_value_blob(val));
                                int len = sqlite3_value_bytes(val);
                                blob_storage[i].assign(blob, blob + len);
                                values[i].value_type = NORIA_BLOB;
                                values[i].blob_ptr = blob_storage[i].data();
                                values[i].blob_len = len;
                                break;
                            }
                            default:
                                values[i].value_type = NORIA_NULL;
                        }
                    } else {
                        values[i].value_type = NORIA_NULL;
                    }
                }
                noria_queue_delete(handle_, table_name, values.data(), n_cols);
                break;
            }

            case SQLITE_UPDATE: {
                // Get old and new values for UPDATE
                std::vector<NoriaValue> old_values(n_cols);
                std::vector<std::string> old_text_storage(n_cols);
                std::vector<std::vector<uint8_t>> old_blob_storage(n_cols);

                // Get old values
                for (int i = 0; i < n_cols; i++) {
                    sqlite3_value* val = nullptr;
                    if (sqlite3_preupdate_old(db_, i, &val) == SQLITE_OK && val) {
                        int type = sqlite3_value_type(val);
                        switch (type) {
                            case SQLITE_INTEGER:
                                old_values[i].value_type = NORIA_INTEGER;
                                old_values[i].int_value = sqlite3_value_int64(val);
                                break;
                            case SQLITE_FLOAT:
                                old_values[i].value_type = NORIA_FLOAT;
                                old_values[i].float_value = sqlite3_value_double(val);
                                break;
                            case SQLITE_TEXT: {
                                const char* text = reinterpret_cast<const char*>(sqlite3_value_text(val));
                                int len = sqlite3_value_bytes(val);
                                old_text_storage[i] = std::string(text, len);
                                old_values[i].value_type = NORIA_TEXT;
                                old_values[i].text_ptr = old_text_storage[i].c_str();
                                old_values[i].text_len = len;
                                break;
                            }
                            case SQLITE_BLOB: {
                                const uint8_t* blob = reinterpret_cast<const uint8_t*>(sqlite3_value_blob(val));
                                int len = sqlite3_value_bytes(val);
                                old_blob_storage[i].assign(blob, blob + len);
                                old_values[i].value_type = NORIA_BLOB;
                                old_values[i].blob_ptr = old_blob_storage[i].data();
                                old_values[i].blob_len = len;
                                break;
                            }
                            default:
                                old_values[i].value_type = NORIA_NULL;
                        }
                    } else {
                        old_values[i].value_type = NORIA_NULL;
                    }
                }

                // Get new values
                for (int i = 0; i < n_cols; i++) {
                    sqlite3_value* val = nullptr;
                    if (sqlite3_preupdate_new(db_, i, &val) == SQLITE_OK && val) {
                        int type = sqlite3_value_type(val);
                        switch (type) {
                            case SQLITE_INTEGER:
                                values[i].value_type = NORIA_INTEGER;
                                values[i].int_value = sqlite3_value_int64(val);
                                break;
                            case SQLITE_FLOAT:
                                values[i].value_type = NORIA_FLOAT;
                                values[i].float_value = sqlite3_value_double(val);
                                break;
                            case SQLITE_TEXT: {
                                const char* text = reinterpret_cast<const char*>(sqlite3_value_text(val));
                                int len = sqlite3_value_bytes(val);
                                text_storage[i] = std::string(text, len);
                                values[i].value_type = NORIA_TEXT;
                                values[i].text_ptr = text_storage[i].c_str();
                                values[i].text_len = len;
                                break;
                            }
                            case SQLITE_BLOB: {
                                const uint8_t* blob = reinterpret_cast<const uint8_t*>(sqlite3_value_blob(val));
                                int len = sqlite3_value_bytes(val);
                                blob_storage[i].assign(blob, blob + len);
                                values[i].value_type = NORIA_BLOB;
                                values[i].blob_ptr = blob_storage[i].data();
                                values[i].blob_len = len;
                                break;
                            }
                            default:
                                values[i].value_type = NORIA_NULL;
                        }
                    } else {
                        values[i].value_type = NORIA_NULL;
                    }
                }

                // Queue as delete + insert
                noria_queue_delete(handle_, table_name, old_values.data(), n_cols);
                noria_queue_insert(handle_, table_name, values.data(), n_cols);
                break;
            }
        }
    }

    // Enable pre-update hook for CDC (called when first view is registered)
    void EnablePreUpdateHook() {
        if (db_ && !preupdate_hook_enabled_) {
            sqlite3_preupdate_hook(db_, PreUpdateHookCallback, this);
            // Also register rollback hook to clear pending events on rollback
            sqlite3_rollback_hook(db_, RollbackHookCallback, this);
            preupdate_hook_enabled_ = true;
        }
    }

    // Clear pending CDC events (called on rollback)
    void ClearPendingCdc() {
        if (handle_) {
            noria_clear_queue(handle_);
        }
    }

    // Execute an upquery - called by Rust via callback when cache miss occurs
    int ExecuteUpquery(
        const char* sql,
        const NoriaValue* params,
        int param_count,
        void** out_rows,
        int* out_row_count
    ) {
        CPP_PROFILE("cpp_execute_upquery");

        if (!db_ || !sql) {
            return -1;
        }

        sqlite3_stmt* stmt = nullptr;
        {
            CPP_PROFILE("cpp_upquery_prepare");
            if (sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr) != SQLITE_OK) {
                return -1;
            }
        }

        // Bind parameters
        {
            CPP_PROFILE("cpp_upquery_bind");
            for (int i = 0; i < param_count; i++) {
                BindNoriaValue(stmt, i + 1, params[i]);
            }
        }

        // Create rows container using Rust FFI
        void* rows_container = noria_rows_create();
        if (!rows_container) {
            sqlite3_finalize(stmt);
            return -1;
        }

        int row_count = 0;

        // Execute and collect rows
        {
            CPP_PROFILE("cpp_upquery_execute");
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                int col_count = sqlite3_column_count(stmt);

                // Build row values - must add immediately since SQLite pointers are transient
                std::vector<NoriaValue> row_values(col_count);
                for (int c = 0; c < col_count; c++) {
                    row_values[c] = SqliteColumnToNoriaValue(stmt, c);
                }

                // Add row to container (Rust will copy string/blob data)
                noria_rows_add_row(rows_container, row_values.data(), col_count);
                row_count++;
            }
        }

        sqlite3_finalize(stmt);

        *out_rows = rows_container;
        *out_row_count = row_count;
        return 0;
    }

    ~Noria() {
        // Unregister hooks if they were registered
        if (preupdate_hook_enabled_ && db_) {
            sqlite3_preupdate_hook(db_, nullptr, nullptr);
            sqlite3_rollback_hook(db_, nullptr, nullptr);
        }
        if (handle_) {
            noria_destroy(handle_);
            handle_ = nullptr;
        }
    }

    // Check if Noria is available
    inline bool IsEnabled() const { return enabled_ && handle_ != nullptr; }

    // Fast check if any views exist (cached, updated on RegisterView)
    inline bool HasAnyViews() const { return has_any_views_; }

    // Register a view for a SELECT statement, returns view_id or -1 on failure
    int RegisterView(const char* sql) {
        CPP_PROFILE("cpp_register_view");

        if (!IsEnabled()) return -1;

        // First, extract table names from SQL and register their schemas
        // This ensures the SQL converter knows about the tables before creating views
        {
            CPP_PROFILE("cpp_register_tables_from_sql");
            RegisterTablesFromSql(sql);
        }

        int result;
        {
            CPP_PROFILE("cpp_register_view_ffi");
            result = noria_register_view(handle_, sql);
        }

        if (result >= 0) {
            has_any_views_ = true;  // Update cached state
            // Enable pre-update hook for direct CDC
            EnablePreUpdateHook();
        }
        return result;
    }

    // Register a table's schema with Noria (queries SQLite for column info)
    void RegisterTableSchema(const char* table_name) {
        if (!IsEnabled() || !db_) return;

        // Query SQLite for table schema
        char pragma_sql[256];
        snprintf(pragma_sql, sizeof(pragma_sql), "PRAGMA table_info(%s)", table_name);

        sqlite3_stmt* stmt = nullptr;
        if (sqlite3_prepare_v2(db_, pragma_sql, -1, &stmt, nullptr) != SQLITE_OK) {
            return;
        }

        std::vector<std::string> column_names;
        std::vector<const char*> column_ptrs;

        // Column info: cid, name, type, notnull, dflt_value, pk
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            const char* col_name = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
            if (col_name) {
                column_names.push_back(col_name);
            }
        }
        sqlite3_finalize(stmt);

        if (column_names.empty()) return;

        // Build pointer array for FFI
        column_ptrs.reserve(column_names.size());
        for (const auto& name : column_names) {
            column_ptrs.push_back(name.c_str());
        }

        noria_register_table_schema(handle_, table_name, column_ptrs.data(),
                                    static_cast<int>(column_ptrs.size()));
    }

    // Extract table names from SQL and register their schemas
    void RegisterTablesFromSql(const char* sql) {
        if (!sql) return;

        // Simple SQL parser to extract table names after FROM and JOIN keywords
        // This is a basic implementation - handles common cases
        std::string sql_upper(sql);
        for (auto& c : sql_upper) c = toupper(c);

        std::vector<std::string> tables;
        size_t pos = 0;

        // Look for FROM clause
        size_t from_pos = sql_upper.find("FROM");
        if (from_pos != std::string::npos) {
            pos = from_pos + 4;
            while (pos < sql_upper.length() && isspace(sql_upper[pos])) pos++;

            // Extract table name (until space, comma, WHERE, JOIN, etc.)
            size_t start = pos;
            while (pos < sql_upper.length() &&
                   !isspace(sql_upper[pos]) &&
                   sql_upper[pos] != ',' &&
                   sql_upper[pos] != '(' &&
                   sql_upper[pos] != ')') {
                pos++;
            }
            if (pos > start) {
                tables.push_back(std::string(sql + start, pos - start));
            }
        }

        // Look for JOIN clauses
        const char* join_keywords[] = {"JOIN", "INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "CROSS JOIN"};
        for (const char* join_kw : join_keywords) {
            pos = 0;
            while ((pos = sql_upper.find(join_kw, pos)) != std::string::npos) {
                pos += strlen(join_kw);
                while (pos < sql_upper.length() && isspace(sql_upper[pos])) pos++;

                size_t start = pos;
                while (pos < sql_upper.length() &&
                       !isspace(sql_upper[pos]) &&
                       sql_upper[pos] != '(' &&
                       sql_upper[pos] != ')') {
                    pos++;
                }
                if (pos > start) {
                    tables.push_back(std::string(sql + start, pos - start));
                }
            }
        }

        // Register each table's schema
        for (const auto& table : tables) {
            RegisterTableSchema(table.c_str());
        }
    }

    // Check if a view exists for the given SQL
    bool HasView(const char* sql) {
        if (!IsEnabled()) return false;
        return noria_has_view(handle_, sql) != 0;
    }

    // Check if any views depend on the given table
    bool TableHasViews(const char* table) {
        if (!IsEnabled() || !has_any_views_) return false;
        return noria_table_has_views(handle_, table) != 0;
    }

    // Get table ID for fast CDC (returns -1 if no views depend on table)
    int GetTableId(const char* table) {
        if (!IsEnabled() || !has_any_views_) return -1;
        return noria_get_table_id(handle_, table);
    }

    // Queue invalidation by table ID (fast path - no string allocation)
    int QueueInvalidateById(int table_id) {
        if (!IsEnabled() || table_id < 0) return -1;
        return noria_queue_invalidate_by_id(handle_, table_id);
    }

    // Lookup in cache only (no SQLite fallback)
    NoriaLookupResult Lookup(int view_id, const NoriaValue* keys, int key_count) {
        CPP_PROFILE("cpp_lookup");

        if (!IsEnabled()) {
            return NoriaLookupResult{0, 0, nullptr};
        }
        return noria_lookup(handle_, view_id, keys, key_count);
    }

    // Lookup with upquery fallback to SQLite
    NoriaLookupResult LookupOrUpquery(int view_id, const NoriaValue* keys, int key_count) {
        CPP_PROFILE("cpp_lookup_or_upquery");

        if (!IsEnabled()) {
            return NoriaLookupResult{0, 0, nullptr};
        }
        return noria_lookup_or_upquery(handle_, view_id, keys, key_count);
    }

    // Fast path lookup for single integer key - skips NoriaValue overhead
    // Returns not_found on cache miss (caller should fall back to regular lookup)
    NoriaLookupResult LookupIntKey(int view_id, int64_t key) {
        CPP_PROFILE("cpp_lookup_int_key");

        if (!IsEnabled()) {
            return NoriaLookupResult{0, 0, nullptr};
        }
        return noria_lookup_int_key(handle_, view_id, key);
    }

    // Get value from lookup result
    static int GetValue(void* rows_ptr, int row_index, int col_index, NoriaValue* out_value) {
        return noria_get_value(rows_ptr, row_index, col_index, out_value);
    }

    // Get all values for a row in a single FFI call - batch optimization
    // Returns 0 on success, -1 on error, -2 if row has >32 columns
    static int GetRow(void* rows_ptr, int row_index, NoriaRowData* out_data) {
        return noria_get_row(rows_ptr, row_index, out_data);
    }

    // Get column count for a row
    static int RowColumnCount(void* rows_ptr, int row_index) {
        return noria_row_column_count(rows_ptr, row_index);
    }

    // Free rows from lookup
    static void FreeRows(void* rows) {
        if (rows) {
            noria_free_rows(rows);
        }
    }

    // Batch lookup - lookup multiple keys in a single FFI call
    // This amortizes lock acquisition and FFI crossing overhead
    NoriaBatchLookupResult LookupBatch(int view_id, const NoriaValue** keys, const int* key_counts, int num_keys) {
        CPP_PROFILE("cpp_lookup_batch");

        if (!IsEnabled()) {
            return NoriaBatchLookupResult{0, nullptr};
        }
        return noria_lookup_batch(handle_, view_id, keys, key_counts, num_keys);
    }

    // Get a single result from batch lookup
    static void* BatchGetResult(void* batch_ptr, int index, int* out_found, int* out_row_count) {
        return noria_batch_get_result(batch_ptr, index, out_found, out_row_count);
    }

    // Free batch lookup results
    static void FreeBatchResults(void* batch_ptr) {
        if (batch_ptr) {
            noria_free_batch_results(batch_ptr);
        }
    }

    // Queue insert for async CDC
    int QueueInsert(const char* table, const NoriaValue* values, int value_count) {
        if (!IsEnabled()) return -1;
        return noria_queue_insert(handle_, table, values, value_count);
    }

    // Queue delete for async CDC
    int QueueDelete(const char* table, const NoriaValue* old_values, int value_count) {
        if (!IsEnabled()) return -1;
        return noria_queue_delete(handle_, table, old_values, value_count);
    }

    // Queue update for async CDC
    int QueueUpdate(const char* table, const NoriaValue* old_values, const NoriaValue* new_values, int value_count) {
        if (!IsEnabled()) return -1;
        return noria_queue_update(handle_, table, old_values, new_values, value_count);
    }

    // Flush pending CDC events (for consistent reads)
    int Flush() {
        if (!IsEnabled()) return 0;
        return noria_flush(handle_);
    }

    // Notify CDC of changes (called after write operations)
    // Flush events to propagate through the dataflow
    void NotifyChange() {
        // In pre-update hook mode, events are already queued.
        // Flush them now to propagate through the dataflow.
        // Only flush if not in a transaction (transaction will flush on commit)
        if (has_any_views_ && sqlite3_get_autocommit(db_)) {
            noria_flush(handle_);
        }
    }

    // Get cache statistics
    NoriaCacheStats GetStats() {
        if (!IsEnabled()) {
            return NoriaCacheStats{0, 0, 0, 0, 0, 0, 0, 0, 0};
        }
        return noria_get_stats(handle_);
    }

    // Disable Noria (fallback mode)
    void Disable() { enabled_ = false; }

    // Re-enable Noria
    void Enable() { enabled_ = (handle_ != nullptr); }

private:
    NoriaHandle* handle_;
    sqlite3* db_;               // SQLite database handle
    bool enabled_;
    bool has_any_views_;        // Cached to avoid FFI calls
    bool preupdate_hook_enabled_;  // Whether pre-update hook is registered

    // Non-copyable
    Noria(const Noria&) = delete;
    Noria& operator=(const Noria&) = delete;
};

// Static upquery callback implementation - Rust calls this on cache miss
static int UpqueryCallbackImpl(
    void* user_data,
    const char* sql,
    const NoriaValue* params,
    int param_count,
    void** out_rows,
    int* out_row_count
) {
    Noria* noria = static_cast<Noria*>(user_data);
    if (!noria) {
        return -1;
    }
    return noria->ExecuteUpquery(sql, params, param_count, out_rows, out_row_count);
}

// Pre-update hook callback - captures changes for CDC
// Note: Must use extern "C" for compatibility with SQLite's C API
extern "C" void PreUpdateHookCallback(
    void* user_data,
    sqlite3* db,
    int op,
    const char* db_name,
    const char* table_name,
    sqlite3_int64 rowid1,
    sqlite3_int64 rowid2
) {
    (void)db;       // Unused (we use noria->db_ instead)
    (void)db_name;  // Unused
    (void)rowid1;   // Unused (old rowid, we use values instead)
    (void)rowid2;   // Unused (new rowid for UPDATE)

    Noria* noria = static_cast<Noria*>(user_data);
    if (!noria) return;

    // Process CDC directly in pre-update hook (no session overhead)
    noria->ProcessPreUpdateChange(table_name, op);
}

// Rollback hook callback - clears pending CDC events when transaction is rolled back
extern "C" void RollbackHookCallback(void* user_data) {
    Noria* noria = static_cast<Noria*>(user_data);
    if (!noria) return;

    // Clear pending CDC events that were queued during the rolled-back transaction
    noria->ClearPendingCdc();
}

// Helper to convert V8 value to NoriaValue
static NoriaValue V8ToNoriaValue(v8::Isolate* isolate, v8::Local<v8::Value> value) {
    NoriaValue nv;
    memset(&nv, 0, sizeof(nv));

    if (value->IsNull() || value->IsUndefined()) {
        nv.value_type = NORIA_NULL;
    } else if (value->IsNumber()) {
        double d = value.As<v8::Number>()->Value();
        // Check if it's an integer
        if (d == static_cast<double>(static_cast<int64_t>(d))) {
            nv.value_type = NORIA_INTEGER;
            nv.int_value = static_cast<int64_t>(d);
        } else {
            nv.value_type = NORIA_FLOAT;
            nv.float_value = d;
        }
    } else if (value->IsBigInt()) {
        bool lossless;
        nv.value_type = NORIA_INTEGER;
        nv.int_value = value.As<v8::BigInt>()->Int64Value(&lossless);
    } else if (value->IsString()) {
        nv.value_type = NORIA_TEXT;
        // Note: caller must keep the string alive
        v8::String::Utf8Value utf8(isolate, value.As<v8::String>());
        nv.text_ptr = *utf8;
        nv.text_len = utf8.length();
    } else if (node::Buffer::HasInstance(value)) {
        nv.value_type = NORIA_BLOB;
        nv.blob_ptr = reinterpret_cast<const uint8_t*>(node::Buffer::Data(value));
        nv.blob_len = static_cast<int>(node::Buffer::Length(value));
    }

    return nv;
}

// Helper to convert NoriaValue to V8 value
static v8::Local<v8::Value> NoriaValueToV8(v8::Isolate* isolate, const NoriaValue& nv, bool safe_ints) {
    switch (nv.value_type) {
        case NORIA_NULL:
            return v8::Null(isolate);
        case NORIA_INTEGER:
            if (safe_ints) {
                return v8::BigInt::New(isolate, nv.int_value);
            }
            return v8::Number::New(isolate, static_cast<double>(nv.int_value));
        case NORIA_FLOAT:
            return v8::Number::New(isolate, nv.float_value);
        case NORIA_TEXT:
            return StringFromUtf8(isolate, nv.text_ptr, nv.text_len);
        case NORIA_BLOB:
            return node::Buffer::Copy(
                isolate,
                reinterpret_cast<const char*>(nv.blob_ptr),
                nv.blob_len
            ).ToLocalChecked();
        default:
            return v8::Null(isolate);
    }
}

// Build a flat JS row object from Noria lookup result
// Optimized: uses batch GetRow to fetch all values in single FFI call
static v8::Local<v8::Value> NoriaRowToJS(
    v8::Isolate* isolate,
    void* rows_ptr,
    int row_index,
    const std::vector<v8::Global<v8::Name>>& column_names_global,
    bool safe_ints
) {
    // Use batch GetRow - single FFI call instead of N calls
    NoriaRowData row_data;
    int result = Noria::GetRow(rows_ptr, row_index, &row_data);

    if (result == -1) {
        // Error or invalid row
        return v8::Null(isolate);
    }

    if (result == -2) {
        // Row has >32 columns - fall back to per-column GetValue
        // This is rare in practice
        int col_count = Noria::RowColumnCount(rows_ptr, row_index);
        if (col_count <= 0) {
            return v8::Null(isolate);
        }

        size_t num_cols = std::min(static_cast<size_t>(col_count), column_names_global.size());

#if defined(NODE_MODULE_VERSION) && NODE_MODULE_VERSION >= 127
        v8::LocalVector<v8::Name> keys(isolate);
        v8::LocalVector<v8::Value> values(isolate);
        keys.reserve(num_cols);
        values.reserve(num_cols);

        for (size_t i = 0; i < num_cols; ++i) {
            keys.push_back(column_names_global[i].Get(isolate));
            NoriaValue nv;
            if (Noria::GetValue(rows_ptr, row_index, static_cast<int>(i), &nv) == 0) {
                values.push_back(NoriaValueToV8(isolate, nv, safe_ints));
            } else {
                values.push_back(v8::Null(isolate));
            }
        }

        return v8::Object::New(
            isolate,
            v8::Null(isolate),
            keys.data(),
            values.data(),
            num_cols
        );
#else
        v8::Local<v8::Context> ctx = isolate->GetCurrentContext();
        v8::Local<v8::Object> row = v8::Object::New(isolate);

        for (size_t i = 0; i < num_cols; ++i) {
            NoriaValue nv;
            if (Noria::GetValue(rows_ptr, row_index, static_cast<int>(i), &nv) == 0) {
                row->Set(ctx, column_names_global[i].Get(isolate), NoriaValueToV8(isolate, nv, safe_ints)).FromJust();
            }
        }

        return row;
#endif
    }

    // Fast path: row_data contains all values from single FFI call
    size_t num_cols = std::min(static_cast<size_t>(row_data.col_count), column_names_global.size());

    if (num_cols == 0) {
        return v8::Null(isolate);
    }

#if defined(NODE_MODULE_VERSION) && NODE_MODULE_VERSION >= 127
    // Stack-allocate for ≤32 columns (guaranteed by NORIA_MAX_ROW_COLUMNS)
    v8::Local<v8::Name> keys_stack[NORIA_MAX_ROW_COLUMNS];
    v8::Local<v8::Value> values_stack[NORIA_MAX_ROW_COLUMNS];

    for (size_t i = 0; i < num_cols; ++i) {
        keys_stack[i] = column_names_global[i].Get(isolate);
        values_stack[i] = NoriaValueToV8(isolate, row_data.values[i], safe_ints);
    }

    return v8::Object::New(
        isolate,
        v8::Null(isolate),
        keys_stack,
        values_stack,
        num_cols
    );
#else
    v8::Local<v8::Context> ctx = isolate->GetCurrentContext();
    v8::Local<v8::Object> row = v8::Object::New(isolate);

    for (size_t i = 0; i < num_cols; ++i) {
        row->Set(ctx, column_names_global[i].Get(isolate), NoriaValueToV8(isolate, row_data.values[i], safe_ints)).FromJust();
    }

    return row;
#endif
}
