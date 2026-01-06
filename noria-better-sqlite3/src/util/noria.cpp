/**
 * noria.cpp - Noria integration for better-sqlite3
 *
 * This file provides the C++ wrapper around the Noria FFI library,
 * enabling transparent cache lookups and CDC capture.
 *
 * Incremental CDC Architecture:
 * - SQLite's session extension accumulates INSERT/UPDATE/DELETE changes
 * - ProcessSessionChangeset() extracts full row data and applies to dataflow
 * - Changes propagate incrementally through the dataflow graph
 * - Use { fresh: true } option to call Flush() before reads when consistency is needed
 */

#include "../noria-ffi/noria.h"
#include <vector>
#include <string>
#include <cstring>
#include <cctype>
#include <map>
#include <utility>

// Forward declaration of Noria class for callback
class Noria;

// Buffered row data captured by pre-update hook
struct BufferedOldRow {
    std::string table_name;
    sqlite3_int64 rowid;
    int op;  // SQLITE_DELETE or SQLITE_UPDATE
    std::vector<NoriaValue> values;
    std::vector<std::string> text_storage;  // Keep string data alive
    std::vector<std::vector<uint8_t>> blob_storage;  // Keep blob data alive
};

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

#ifdef SQLITE_ENABLE_SESSION
// Session extension is available - use it for CDC
#define USE_SESSION_CDC 1
#else
#define USE_SESSION_CDC 0
#endif

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
    Noria(sqlite3* db) : handle_(nullptr), db_(db), session_(nullptr), enabled_(true), has_any_views_(false) {
        handle_ = noria_create(db);
        if (!handle_) {
            enabled_ = false;
        } else {
            // Register upquery callback so Rust can call back to execute SQLite queries
            noria_set_upquery_callback(handle_, UpqueryCallbackImpl, this);
            // NOTE: Pre-update hook is registered lazily in EnsureSession() AFTER
            // the session is created to avoid conflicts with session extension
        }
    }

    // Capture old row values from pre-update hook (called before DELETE/UPDATE)
    // NOTE: Currently unused because pre-update hook conflicts with session extension.
    // Kept for potential future use with alternative CDC approach.
    void CaptureOldRow(const char* table_name, sqlite3_int64 rowid, int op) {
        if (!db_ || !table_name || !has_any_views_) return;
        if (op != SQLITE_DELETE && op != SQLITE_UPDATE) return;

        // Only capture for tables that have views
        if (noria_table_has_views(handle_, table_name) == 0) return;

        int n_cols = sqlite3_preupdate_count(db_);
        if (n_cols <= 0) return;

        BufferedOldRow row;
        row.table_name = table_name;
        row.rowid = rowid;
        row.op = op;
        row.values.resize(n_cols);
        row.text_storage.resize(n_cols);
        row.blob_storage.resize(n_cols);

        for (int i = 0; i < n_cols; i++) {
            sqlite3_value* val = nullptr;
            if (sqlite3_preupdate_old(db_, i, &val) == SQLITE_OK && val) {
                int type = sqlite3_value_type(val);
                switch (type) {
                    case SQLITE_NULL:
                        row.values[i].value_type = NORIA_NULL;
                        break;
                    case SQLITE_INTEGER:
                        row.values[i].value_type = NORIA_INTEGER;
                        row.values[i].int_value = sqlite3_value_int64(val);
                        break;
                    case SQLITE_FLOAT:
                        row.values[i].value_type = NORIA_FLOAT;
                        row.values[i].float_value = sqlite3_value_double(val);
                        break;
                    case SQLITE_TEXT: {
                        const char* text = reinterpret_cast<const char*>(sqlite3_value_text(val));
                        int len = sqlite3_value_bytes(val);
                        row.text_storage[i] = std::string(text, len);
                        row.values[i].value_type = NORIA_TEXT;
                        row.values[i].text_ptr = row.text_storage[i].c_str();
                        row.values[i].text_len = len;
                        break;
                    }
                    case SQLITE_BLOB: {
                        const uint8_t* blob = reinterpret_cast<const uint8_t*>(sqlite3_value_blob(val));
                        int len = sqlite3_value_bytes(val);
                        row.blob_storage[i].assign(blob, blob + len);
                        row.values[i].value_type = NORIA_BLOB;
                        row.values[i].blob_ptr = row.blob_storage[i].data();
                        row.values[i].blob_len = len;
                        break;
                    }
                    default:
                        row.values[i].value_type = NORIA_NULL;
                }
            } else {
                row.values[i].value_type = NORIA_NULL;
            }
        }

        // Store using table+rowid as key
        buffered_old_rows_[std::make_pair(std::string(table_name), rowid)] = std::move(row);
    }

    // Get buffered old row if available
    BufferedOldRow* GetBufferedOldRow(const char* table_name, sqlite3_int64 rowid) {
        auto key = std::make_pair(std::string(table_name), rowid);
        auto it = buffered_old_rows_.find(key);
        if (it != buffered_old_rows_.end()) {
            return &it->second;
        }
        return nullptr;
    }

    // Clear buffered old rows after processing
    void ClearBufferedOldRows() {
        buffered_old_rows_.clear();
    }

    // Execute an upquery - called by Rust via callback when cache miss occurs
    int ExecuteUpquery(
        const char* sql,
        const NoriaValue* params,
        int param_count,
        void** out_rows,
        int* out_row_count
    ) {
        if (!db_ || !sql) {
            return -1;
        }

        sqlite3_stmt* stmt = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr) != SQLITE_OK) {
            return -1;
        }

        // Bind parameters
        for (int i = 0; i < param_count; i++) {
            BindNoriaValue(stmt, i + 1, params[i]);
        }

        // Create rows container using Rust FFI
        void* rows_container = noria_rows_create();
        if (!rows_container) {
            sqlite3_finalize(stmt);
            return -1;
        }

        int row_count = 0;

        // Execute and collect rows
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

        sqlite3_finalize(stmt);

        *out_rows = rows_container;
        *out_row_count = row_count;
        return 0;
    }

    ~Noria() {
#if USE_SESSION_CDC
        // Unregister pre-update hook first (only if session was created)
        // The hook is registered in EnsureSession() after session creation
        if (session_ && db_) {
            sqlite3_preupdate_hook(db_, nullptr, nullptr);
        }
        if (session_) {
            sqlite3session_delete(session_);
            session_ = nullptr;
        }
#endif
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
        if (!IsEnabled()) return -1;

        // First, extract table names from SQL and register their schemas
        // This ensures the SQL converter knows about the tables before creating views
        RegisterTablesFromSql(sql);

        int result = noria_register_view(handle_, sql);
        if (result >= 0) {
            has_any_views_ = true;  // Update cached state
#if USE_SESSION_CDC
            // Ensure session is created and attach all tables
            EnsureSession();
#endif
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

#if USE_SESSION_CDC
    // Ensure session exists for CDC tracking
    void EnsureSession() {
        if (!session_ && db_) {
            // Create session attached to "main" database
            if (sqlite3session_create(db_, "main", &session_) != SQLITE_OK) {
                session_ = nullptr;
                return;
            }
            // Attach all tables (NULL = track all tables)
            sqlite3session_attach(session_, nullptr);

            // NOTE: Pre-update hook conflicts with session extension's ability to record changes.
            // SQLite only allows one pre-update hook, and the session extension uses it internally.
            // For now, rely on session extension alone and accept partial old values for DELETE/UPDATE.
            // This is acceptable because:
            // 1. For simple views (no aggregates), any old value triggers proper cache eviction
            // 2. For aggregate views, we may need to fall back to full table re-scan
            // sqlite3_preupdate_hook(db_, PreUpdateHookCallback, this);
        }
    }

    // Extract CDC events from session changeset and apply incremental updates
    void ProcessSessionChangeset() {
        if (!session_ || !has_any_views_) return;

        // Only process changeset when NOT in a transaction.
        // SQLite's autocommit mode is 1 when not in a transaction.
        // This ensures rolled-back changes don't affect the cache.
        if (!sqlite3_get_autocommit(db_)) {
            // Inside a transaction - defer processing until commit
            return;
        }

        void* changeset = nullptr;
        int changeset_size = 0;

        // Get changeset (this clears recorded changes)
        if (sqlite3session_changeset(session_, &changeset_size, &changeset) != SQLITE_OK) {
            ClearBufferedOldRows();
            return;
        }

        if (changeset_size == 0 || !changeset) {
            ClearBufferedOldRows();
            return;
        }

        // Iterate through changeset to extract row data
        sqlite3_changeset_iter* iter = nullptr;
        if (sqlite3changeset_start(&iter, changeset_size, changeset) != SQLITE_OK) {
            sqlite3_free(changeset);
            ClearBufferedOldRows();
            return;
        }

        // Process each change
        while (sqlite3changeset_next(iter) == SQLITE_ROW) {
            const char* table_name = nullptr;
            int n_cols = 0;
            int op = 0;
            int indirect = 0;

            if (sqlite3changeset_op(iter, &table_name, &n_cols, &op, &indirect) != SQLITE_OK) {
                continue;
            }
            if (!table_name || n_cols <= 0) continue;

            // Check if this table has views (skip if not)
            if (noria_table_has_views(handle_, table_name) == 0) continue;

            // Extract row values based on operation type
            std::vector<NoriaValue> old_values(n_cols);
            std::vector<NoriaValue> new_values(n_cols);

            switch (op) {
                case SQLITE_INSERT: {
                    // INSERT: extract new values
                    for (int i = 0; i < n_cols; i++) {
                        sqlite3_value* val = nullptr;
                        if (sqlite3changeset_new(iter, i, &val) == SQLITE_OK && val) {
                            new_values[i] = SqliteValueToNoria(val);
                        } else {
                            new_values[i].value_type = NORIA_NULL;
                        }
                    }
                    noria_apply_insert(handle_, table_name, new_values.data(), n_cols);
                    break;
                }
                case SQLITE_DELETE: {
                    // DELETE: Try to get full old values from pre-update hook buffer
                    // First, extract the PK (first column for INTEGER PRIMARY KEY tables)
                    sqlite3_int64 rowid = 0;
                    sqlite3_value* pk_val = nullptr;
                    if (sqlite3changeset_old(iter, 0, &pk_val) == SQLITE_OK && pk_val) {
                        if (sqlite3_value_type(pk_val) == SQLITE_INTEGER) {
                            rowid = sqlite3_value_int64(pk_val);
                        }
                    }

                    // Check for buffered old row with full values
                    BufferedOldRow* buffered = GetBufferedOldRow(table_name, rowid);
                    if (buffered && buffered->values.size() == static_cast<size_t>(n_cols)) {
                        // Use buffered full row values
                        noria_apply_delete(handle_, table_name, buffered->values.data(), n_cols);
                    } else {
                        // Fallback to session values (may be partial)
                        for (int i = 0; i < n_cols; i++) {
                            sqlite3_value* val = nullptr;
                            if (sqlite3changeset_old(iter, i, &val) == SQLITE_OK && val) {
                                old_values[i] = SqliteValueToNoria(val);
                            } else {
                                old_values[i].value_type = NORIA_NULL;
                            }
                        }
                        noria_apply_delete(handle_, table_name, old_values.data(), n_cols);
                    }
                    break;
                }
                case SQLITE_UPDATE: {
                    // UPDATE: Try to get full old values from pre-update hook buffer
                    // First, extract the PK (first column for INTEGER PRIMARY KEY tables)
                    sqlite3_int64 rowid = 0;
                    sqlite3_value* pk_val = nullptr;
                    if (sqlite3changeset_old(iter, 0, &pk_val) == SQLITE_OK && pk_val) {
                        if (sqlite3_value_type(pk_val) == SQLITE_INTEGER) {
                            rowid = sqlite3_value_int64(pk_val);
                        }
                    }

                    // Check for buffered old row with full values
                    BufferedOldRow* buffered = GetBufferedOldRow(table_name, rowid);
                    if (buffered && buffered->values.size() == static_cast<size_t>(n_cols)) {
                        // Use buffered full old row values
                        old_values = buffered->values;
                    } else {
                        // Fallback to session values (may be partial)
                        for (int i = 0; i < n_cols; i++) {
                            sqlite3_value* old_val = nullptr;
                            if (sqlite3changeset_old(iter, i, &old_val) == SQLITE_OK && old_val) {
                                old_values[i] = SqliteValueToNoria(old_val);
                            } else {
                                old_values[i].value_type = NORIA_NULL;
                            }
                        }
                    }

                    // Extract new values - use session values, filling in from old if not changed
                    for (int i = 0; i < n_cols; i++) {
                        sqlite3_value* new_val = nullptr;
                        if (sqlite3changeset_new(iter, i, &new_val) == SQLITE_OK && new_val) {
                            new_values[i] = SqliteValueToNoria(new_val);
                        } else {
                            // Unchanged column - copy from old value
                            new_values[i] = old_values[i];
                        }
                    }
                    noria_apply_update(handle_, table_name, old_values.data(), new_values.data(), n_cols);
                    break;
                }
            }
        }

        sqlite3changeset_finalize(iter);
        sqlite3_free(changeset);
        ClearBufferedOldRows();  // Clear buffer after processing

        // After sqlite3session_changeset(), the session needs to be recreated
        // to continue recording changes. Delete the old session and create a new one.
        sqlite3session_delete(session_);
        session_ = nullptr;
        if (sqlite3session_create(db_, "main", &session_) == SQLITE_OK) {
            sqlite3session_attach(session_, nullptr);
        }
    }

    // Helper to convert sqlite3_value to NoriaValue
    static NoriaValue SqliteValueToNoria(sqlite3_value* val) {
        NoriaValue nv;
        memset(&nv, 0, sizeof(nv));

        if (!val) {
            nv.value_type = NORIA_NULL;
            return nv;
        }

        int type = sqlite3_value_type(val);
        switch (type) {
            case SQLITE_NULL:
                nv.value_type = NORIA_NULL;
                break;
            case SQLITE_INTEGER:
                nv.value_type = NORIA_INTEGER;
                nv.int_value = sqlite3_value_int64(val);
                break;
            case SQLITE_FLOAT:
                nv.value_type = NORIA_FLOAT;
                nv.float_value = sqlite3_value_double(val);
                break;
            case SQLITE_TEXT:
                nv.value_type = NORIA_TEXT;
                nv.text_ptr = reinterpret_cast<const char*>(sqlite3_value_text(val));
                nv.text_len = sqlite3_value_bytes(val);
                break;
            case SQLITE_BLOB:
                nv.value_type = NORIA_BLOB;
                nv.blob_ptr = reinterpret_cast<const uint8_t*>(sqlite3_value_blob(val));
                nv.blob_len = sqlite3_value_bytes(val);
                break;
            default:
                nv.value_type = NORIA_NULL;
        }
        return nv;
    }
#endif

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
        if (!IsEnabled()) {
            return NoriaLookupResult{0, 0, nullptr};
        }
        return noria_lookup(handle_, view_id, keys, key_count);
    }

    // Lookup with upquery fallback to SQLite
    NoriaLookupResult LookupOrUpquery(int view_id, const NoriaValue* keys, int key_count) {
        if (!IsEnabled()) {
            return NoriaLookupResult{0, 0, nullptr};
        }
        return noria_lookup_or_upquery(handle_, view_id, keys, key_count);
    }

    // Get value from lookup result
    static int GetValue(void* rows_ptr, int row_index, int col_index, NoriaValue* out_value) {
        return noria_get_value(rows_ptr, row_index, col_index, out_value);
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
#if USE_SESSION_CDC
        // Process any pending session changes first
        ProcessSessionChangeset();
#endif
        return noria_flush(handle_);
    }

    // Notify CDC of changes (called after write operations)
    // Process session changeset synchronously to enable incremental view updates.
    void NotifyChange() {
#if USE_SESSION_CDC
        // Synchronous CDC: Process changes immediately after write
        ProcessSessionChangeset();
#endif
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
    sqlite3* db_;               // SQLite database handle (for session)
#if USE_SESSION_CDC
    sqlite3_session* session_;  // Session for CDC tracking
#else
    void* session_;             // Placeholder when session not available
#endif
    bool enabled_;
    bool has_any_views_;  // Cached to avoid FFI calls

    // Buffered old row values from pre-update hook (for DELETE/UPDATE)
    std::map<std::pair<std::string, sqlite3_int64>, BufferedOldRow> buffered_old_rows_;

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

// Pre-update hook callback - captures full old row values before DELETE/UPDATE
// Note: Must use extern "C" for compatibility with SQLite's C API
// NOTE: Currently unused because pre-update hook conflicts with session extension.
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
    (void)rowid2;   // Unused (new rowid for UPDATE)

    Noria* noria = static_cast<Noria*>(user_data);
    if (!noria) return;

    // Capture old row for DELETE and UPDATE operations
    if (op == SQLITE_DELETE || op == SQLITE_UPDATE) {
        noria->CaptureOldRow(table_name, rowid1, op);
    }
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
static v8::Local<v8::Value> NoriaRowToJS(
    v8::Isolate* isolate,
    void* rows_ptr,
    int row_index,
    const std::vector<v8::Local<v8::Name>>& column_names,
    bool safe_ints
) {
    int col_count = Noria::RowColumnCount(rows_ptr, row_index);
    if (col_count <= 0) {
        return v8::Null(isolate);
    }

#if defined(NODE_MODULE_VERSION) && NODE_MODULE_VERSION >= 127
    v8::LocalVector<v8::Value> values(isolate);
    values.reserve(col_count);

    for (int i = 0; i < col_count; ++i) {
        NoriaValue nv;
        if (Noria::GetValue(rows_ptr, row_index, i, &nv) == 0) {
            values.emplace_back(NoriaValueToV8(isolate, nv, safe_ints));
        } else {
            values.emplace_back(v8::Null(isolate));
        }
    }

    // Use fast object construction
    v8::Local<v8::Name>* keys_ptr = const_cast<v8::Local<v8::Name>*>(column_names.data());
    return v8::Object::New(
        isolate,
        GET_PROTOTYPE(v8::Object::New(isolate)),
        keys_ptr,
        values.data(),
        std::min(static_cast<size_t>(col_count), column_names.size())
    );
#else
    v8::Local<v8::Context> ctx = isolate->GetCurrentContext();
    v8::Local<v8::Object> row = v8::Object::New(isolate);

    for (int i = 0; i < col_count && i < static_cast<int>(column_names.size()); ++i) {
        NoriaValue nv;
        if (Noria::GetValue(rows_ptr, row_index, i, &nv) == 0) {
            row->Set(ctx, column_names[i], NoriaValueToV8(isolate, nv, safe_ints)).FromJust();
        }
    }

    return row;
#endif
}
