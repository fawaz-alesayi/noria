/**
 * noria-sqlite - Transparent caching layer for SQLite using Noria's incremental dataflow engine
 *
 * @packageDocumentation
 */

/**
 * Statistics about the dataflow cache.
 */
export interface CacheStats {
  /** Number of nodes in the dataflow graph */
  nodeCount: number;
  /** Number of materialized (cached) nodes */
  materializedNodes: number;
  /** Total rows across all materialized views */
  totalRows: number;
}

/**
 * Result of running a statement that modifies data.
 */
export interface RunResult {
  /** Number of rows changed */
  changes: number;
  /** Row ID of the last inserted row */
  lastInsertRowid: number;
}

/**
 * A prepared SQL statement.
 *
 * For SELECT queries with parameters, this automatically uses a
 * Noria materialized view for O(1) lookups after the first call.
 */
export class Statement {
  /**
   * Execute the statement and return the first row.
   *
   * @param params - Bind parameters (numbers or strings)
   * @returns The first row as an object, or undefined if no rows
   *
   * @example
   * ```js
   * const stmt = db.prepare('SELECT * FROM users WHERE id = ?');
   * const user = stmt.get([1]);
   * console.log(user); // { id: 1, name: 'Alice' }
   * ```
   */
  get(params?: (number | string | null)[]): Record<string, unknown> | undefined;

  /**
   * Execute the statement and return all matching rows.
   *
   * @param params - Bind parameters (numbers or strings)
   * @returns An array of row objects
   *
   * @example
   * ```js
   * const stmt = db.prepare('SELECT * FROM users WHERE age > ?');
   * const users = stmt.all([25]);
   * ```
   */
  all(params?: (number | string | null)[]): Record<string, unknown>[];

  /**
   * Execute the statement without returning rows.
   * Use this for INSERT, UPDATE, DELETE statements.
   *
   * @param params - Bind parameters (numbers or strings)
   * @returns Information about the execution
   *
   * @example
   * ```js
   * const stmt = db.prepare('INSERT INTO users (name, age) VALUES (?, ?)');
   * const result = stmt.run(['Alice', 30]);
   * console.log(result.changes); // 1
   * ```
   */
  run(params?: (number | string | null)[]): RunResult;

  /**
   * Whether this statement has a materialized view (cache).
   * Only SELECT statements with parameters get materialized views.
   */
  readonly cached: boolean;
}

/**
 * A SQLite database connection with transparent Noria acceleration.
 *
 * Queries with parameters automatically get materialized views backed by
 * Noria's incremental dataflow engine. Writes propagate through the
 * dataflow graph to update views incrementally (O(delta) instead of O(n)).
 *
 * @example
 * ```js
 * const Database = require('noria-sqlite');
 *
 * const db = new Database(':memory:');
 * db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
 * db.exec("INSERT INTO users VALUES (1, 'Alice', 30)");
 *
 * // First query - upquery (cache miss → SQLite → populate cache)
 * const stmt = db.prepare('SELECT * FROM users WHERE id = ?');
 * const row = stmt.get([1]); // { id: 1, name: 'Alice', age: 30 }
 *
 * // Second query - cache hit (reads from materialized view)
 * const row2 = stmt.get([1]); // Same result, O(1) lookup
 *
 * // Writes propagate through dataflow automatically
 * db.exec("UPDATE users SET age = 31 WHERE id = 1");
 * const row3 = stmt.get([1]); // { id: 1, name: 'Alice', age: 31 }
 * ```
 */
export class Database {
  /**
   * Create a new database connection.
   *
   * @param filename - Path to database file, or ':memory:' for in-memory database
   */
  constructor(filename: string);

  /**
   * Execute one or more SQL statements that don't return data.
   * Use this for DDL (CREATE, ALTER, DROP) and DML (INSERT, UPDATE, DELETE).
   *
   * @param sql - The SQL to execute
   * @returns The database instance for chaining
   */
  exec(sql: string): this;

  /**
   * Prepare a SQL statement for execution.
   *
   * For SELECT queries with parameters, this automatically creates
   * a Noria materialized view for fast lookups.
   *
   * @param sql - The SQL statement with optional ? placeholders
   * @returns A prepared statement
   */
  prepare(sql: string): Statement;

  /**
   * Get statistics about the dataflow cache.
   *
   * @returns Cache statistics
   */
  stats(): CacheStats;

  /**
   * Close the database connection.
   */
  close(): void;
}

export = Database;
