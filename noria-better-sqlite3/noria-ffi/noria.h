/**
 * noria.h - C FFI header for Noria engine integration
 *
 * This header provides the C API for integrating Noria's incremental view
 * maintenance with SQLite/better-sqlite3.
 */

#ifndef NORIA_H
#define NORIA_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque handle to a Noria engine instance */
typedef struct NoriaHandle NoriaHandle;

/* Value types matching SQLite */
#define NORIA_NULL    0
#define NORIA_INTEGER 1
#define NORIA_FLOAT   2
#define NORIA_TEXT    3
#define NORIA_BLOB    4

/* A single value for FFI */
typedef struct {
    int value_type;
    int64_t int_value;
    double float_value;
    const char* text_ptr;
    int text_len;
    const uint8_t* blob_ptr;
    int blob_len;
} NoriaValue;

/* Result of a cache lookup */
typedef struct {
    int found;      /* 1 if found in cache, 0 if cache miss */
    int row_count;  /* Number of rows returned */
    void* rows;     /* Opaque pointer to row data (caller must free with noria_free_rows) */
} NoriaLookupResult;

/* Cache statistics */
typedef struct {
    uint64_t cache_hits;
    uint64_t cache_misses;
    uint64_t total_rows;
    int view_count;
    int node_count;
    uint64_t memory_bytes;
    uint64_t max_memory_bytes;
    uint64_t eviction_count;
    uint64_t bytes_evicted;
} NoriaCacheStats;

/**
 * Upquery callback type - Rust calls this to execute SQLite queries.
 *
 * @param user_data User-provided data pointer
 * @param sql The SQL query to execute
 * @param params Array of parameter values
 * @param param_count Number of parameters
 * @param out_rows Output: pointer to rows data (must be freed with noria_free_rows)
 * @param out_row_count Output: number of rows returned
 * @return 0 on success, non-zero on error
 */
typedef int (*NoriaUpqueryCallback)(
    void* user_data,
    const char* sql,
    const NoriaValue* params,
    int param_count,
    void** out_rows,
    int* out_row_count
);

/**
 * Create a new Noria engine attached to the given SQLite database handle.
 *
 * @param sqlite_db A sqlite3* handle from better-sqlite3
 * @return NoriaHandle* on success, NULL on failure
 */
NoriaHandle* noria_create(void* sqlite_db);

/**
 * Destroy a Noria engine and free all resources.
 * NOTE: This does NOT close the SQLite connection - better-sqlite3 owns that.
 *
 * @param handle The Noria engine handle
 */
void noria_destroy(NoriaHandle* handle);

/**
 * Set the upquery callback for handling cache misses.
 *
 * @param handle The Noria engine handle
 * @param callback The callback function
 * @param user_data User data passed to callback
 * @return 0 on success, -1 on error
 */
int noria_set_upquery_callback(
    NoriaHandle* handle,
    NoriaUpqueryCallback callback,
    void* user_data
);

/**
 * Register a table schema (required before creating views that use it).
 *
 * @param handle The Noria engine handle
 * @param table Table name
 * @param columns Array of column name pointers
 * @param column_count Number of columns
 * @return 0 on success, -1 on error
 */
int noria_register_table_schema(
    NoriaHandle* handle,
    const char* table,
    const char** columns,
    int column_count
);

/**
 * Register a SELECT query as a Noria view.
 *
 * @param handle The Noria engine handle
 * @param sql The SELECT query SQL
 * @return view ID (>= 0) on success, -1 on failure
 */
int noria_register_view(NoriaHandle* handle, const char* sql);

/**
 * Check if a view exists for the given SQL.
 *
 * @param handle The Noria engine handle
 * @param sql The SQL query
 * @return 1 if exists, 0 otherwise
 */
int noria_has_view(NoriaHandle* handle, const char* sql);

/**
 * Check if any views depend on the given table.
 *
 * @param handle The Noria engine handle
 * @param table The table name
 * @return 1 if table has views, 0 otherwise
 */
int noria_table_has_views(NoriaHandle* handle, const char* table);

/**
 * Get the table ID for a given table name.
 * Returns the table ID (>= 0) if views exist for this table, -1 otherwise.
 * Use with noria_queue_invalidate_by_id for fast CDC.
 *
 * @param handle The Noria engine handle
 * @param table The table name
 * @return table ID (>= 0) or -1 if no views depend on this table
 */
int noria_get_table_id(NoriaHandle* handle, const char* table);

/**
 * Queue a table invalidation by table ID (fast path - no string allocation).
 * Use noria_get_table_id to get the table ID first.
 *
 * @param handle The Noria engine handle
 * @param table_id The table ID from noria_get_table_id
 * @return 0 on success, -1 on error
 */
int noria_queue_invalidate_by_id(NoriaHandle* handle, int table_id);

/**
 * Lookup a value in the Noria cache by view ID.
 * Cache-only lookup - does not query SQLite on miss.
 *
 * @param handle The Noria engine handle
 * @param view_id The view ID from noria_register_view
 * @param key_values Array of key values for the WHERE clause
 * @param key_count Number of key values
 * @return NoriaLookupResult. Caller must free rows with noria_free_rows.
 */
NoriaLookupResult noria_lookup(
    NoriaHandle* handle,
    int view_id,
    const NoriaValue* key_values,
    int key_count
);

/**
 * Lookup with upquery fallback - queries SQLite on cache miss.
 * This is the typical code path for reads.
 *
 * @param handle The Noria engine handle
 * @param view_id The view ID from noria_register_view
 * @param key_values Array of key values for the WHERE clause
 * @param key_count Number of key values
 * @return NoriaLookupResult. Caller must free rows with noria_free_rows.
 */
NoriaLookupResult noria_lookup_or_upquery(
    NoriaHandle* handle,
    int view_id,
    const NoriaValue* key_values,
    int key_count
);

/**
 * Fast path lookup for single integer key.
 * Skips NoriaValue conversion overhead for the common case of integer PK lookup.
 * Cache-only lookup - returns not_found on miss (caller should fall back to regular lookup).
 *
 * @param handle The Noria engine handle
 * @param view_id The view ID from noria_register_view
 * @param key The integer key value
 * @return NoriaLookupResult. Caller must free rows with noria_free_rows.
 */
NoriaLookupResult noria_lookup_int_key(
    NoriaHandle* handle,
    int view_id,
    int64_t key
);

/* Batch lookup result */
typedef struct {
    int count;      /* Number of results (one per key) */
    void* results;  /* Opaque pointer to results (caller must free with noria_free_batch_results) */
} NoriaBatchLookupResult;

/**
 * Batch lookup - lookup multiple keys in a single FFI call.
 * This amortizes lock acquisition and FFI crossing overhead.
 *
 * @param handle The Noria engine handle
 * @param view_id The view ID from noria_register_view
 * @param keys Array of key arrays (pointer to pointers)
 * @param key_counts Array of key counts for each key array
 * @param num_keys Number of keys to lookup
 * @return NoriaBatchLookupResult. Caller must free with noria_free_batch_results.
 */
NoriaBatchLookupResult noria_lookup_batch(
    NoriaHandle* handle,
    int view_id,
    const NoriaValue** keys,
    const int* key_counts,
    int num_keys
);

/**
 * Get a single result from a batch lookup.
 *
 * @param batch_ptr The results pointer from NoriaBatchLookupResult
 * @param index The result index (0 to count-1)
 * @param out_found Output: 1 if found, 0 if cache miss
 * @param out_row_count Output: number of rows for this result
 * @return Pointer to rows data for this result (do NOT free separately), or NULL
 */
void* noria_batch_get_result(
    void* batch_ptr,
    int index,
    int* out_found,
    int* out_row_count
);

/**
 * Free batch lookup results.
 *
 * @param batch_ptr The results pointer from NoriaBatchLookupResult
 */
void noria_free_batch_results(void* batch_ptr);

/**
 * Get a specific column value from a row in the lookup result.
 *
 * @param rows_ptr The rows pointer from NoriaLookupResult
 * @param row_index The row index
 * @param col_index The column index
 * @param out_value Output parameter for the value
 * @return 0 on success, -1 on error
 */
int noria_get_value(
    void* rows_ptr,
    int row_index,
    int col_index,
    NoriaValue* out_value
);

/* Maximum columns supported by NoriaRowData */
#define NORIA_MAX_ROW_COLUMNS 32

/* Row data for batch extraction - all values in single FFI call */
typedef struct {
    int col_count;
    NoriaValue values[NORIA_MAX_ROW_COLUMNS];
} NoriaRowData;

/**
 * Get all values for a row in a single FFI call - batch optimization.
 * This eliminates N FFI calls (one per column) with a single call.
 * For rows with ≤32 columns, this avoids repeated FFI crossing overhead.
 *
 * @param rows_ptr The rows pointer from NoriaLookupResult
 * @param row_index The row index
 * @param out_data Output parameter for all row values
 * @return 0 on success, -1 on error, -2 if row has >32 columns (use noria_get_value fallback)
 */
int noria_get_row(
    void* rows_ptr,
    int row_index,
    NoriaRowData* out_data
);

/**
 * Get the number of columns in a row.
 *
 * @param rows_ptr The rows pointer from NoriaLookupResult
 * @param row_index The row index
 * @return Number of columns, or 0 on error
 */
int noria_row_column_count(void* rows_ptr, int row_index);

/**
 * Free rows returned by noria_lookup.
 *
 * @param rows The rows pointer from NoriaLookupResult
 */
void noria_free_rows(void* rows);

/**
 * Create an empty rows container for upquery callback results.
 *
 * @return Opaque pointer to rows container
 */
void* noria_rows_create(void);

/**
 * Add a row to a rows container.
 * String and blob data is copied into the container.
 *
 * @param rows_ptr The rows container from noria_rows_create
 * @param values Array of column values
 * @param value_count Number of values
 * @return 0 on success, -1 on error
 */
int noria_rows_add_row(void* rows_ptr, const NoriaValue* values, int value_count);

/**
 * Apply an INSERT CDC event (synchronous).
 *
 * @param handle The Noria engine handle
 * @param table The table name
 * @param values Array of column values
 * @param value_count Number of values
 * @return 0 on success, -1 on error
 */
int noria_apply_insert(
    NoriaHandle* handle,
    const char* table,
    const NoriaValue* values,
    int value_count
);

/**
 * Apply a DELETE CDC event (synchronous).
 *
 * @param handle The Noria engine handle
 * @param table The table name
 * @param old_values Array of column values for the deleted row
 * @param value_count Number of values
 * @return 0 on success, -1 on error
 */
int noria_apply_delete(
    NoriaHandle* handle,
    const char* table,
    const NoriaValue* old_values,
    int value_count
);

/**
 * Apply an UPDATE CDC event (synchronous).
 *
 * @param handle The Noria engine handle
 * @param table The table name
 * @param old_values Array of column values for the old row
 * @param new_values Array of column values for the new row
 * @param value_count Number of values
 * @return 0 on success, -1 on error
 */
int noria_apply_update(
    NoriaHandle* handle,
    const char* table,
    const NoriaValue* old_values,
    const NoriaValue* new_values,
    int value_count
);

/**
 * Queue an INSERT for async CDC processing.
 * Currently applies synchronously - async implementation TODO.
 */
int noria_queue_insert(
    NoriaHandle* handle,
    const char* table,
    const NoriaValue* values,
    int value_count
);

/**
 * Queue a DELETE for async CDC processing.
 * Currently applies synchronously - async implementation TODO.
 */
int noria_queue_delete(
    NoriaHandle* handle,
    const char* table,
    const NoriaValue* old_values,
    int value_count
);

/**
 * Queue an UPDATE for async CDC processing.
 * Currently applies synchronously - async implementation TODO.
 */
int noria_queue_update(
    NoriaHandle* handle,
    const char* table,
    const NoriaValue* old_values,
    const NoriaValue* new_values,
    int value_count
);

/**
 * Flush all pending CDC events synchronously.
 * Use this before consistent reads.
 *
 * @param handle The Noria engine handle
 * @return 0 on success, -1 on error
 */
int noria_flush(NoriaHandle* handle);

/**
 * Clear the write queue without processing (used on transaction rollback).
 *
 * When a transaction is rolled back, pending CDC events should be discarded
 * since the database changes did not persist.
 *
 * @param handle The Noria engine handle
 */
void noria_clear_queue(NoriaHandle* handle);

/**
 * Get cache statistics.
 *
 * @param handle The Noria engine handle
 * @return NoriaCacheStats with current statistics
 */
NoriaCacheStats noria_get_stats(NoriaHandle* handle);

/**
 * Check if there are any registered views.
 * This is a fast O(1) check useful for skipping CDC when no views exist.
 *
 * @param handle The Noria engine handle
 * @return 1 if any views exist, 0 otherwise
 */
int noria_has_any_views(NoriaHandle* handle);

#ifdef __cplusplus
}
#endif

#endif /* NORIA_H */
