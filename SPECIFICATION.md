# Engineering Specification: Noria for SQLite

**Version:** 1.1
**Date:** November 2025
**Goal:** Embed Noria's differential dataflow engine as a transparent, in-process caching layer for SQLite.

---

## 1. Architectural Overview

The goal is to replace the "External Server" model of Noria with an "Embedded Library" model. Noria will run as a background thread pool within the host application (Node.js, Python, Rust) and transparently intercept SQLite queries to provide O(1) partial materialized views.

### Core Principles
1.  **Single Source of Truth:** SQLite owns the data on disk. Noria owns the cache in RAM. Noria never writes to disk.
2.  **Zero Config:** No external processes, no schema files, no manual view definitions.
3.  **Drop-in Compatibility:** The library must expose APIs that mimic standard drivers (e.g., `better-sqlite3`, `sqlite3` standard lib) to work with any ORM.

---

## 2. Component Architecture

### Component A: The "Virtual Base" (Storage Layer)
*Location: `server/dataflow/src/state/sqlite_state.rs` (New)*

Noria's `Base` node currently relies on `PersistentState` (RocksDB). We will introduce a `SqliteState` implementation of the `State` trait.

*   **Function:** Serves as a read-only adapter for Noria to fetch data ("Upqueries") from the underlying SQLite database when the cache misses.
*   **Key Behavior:**
    *   `lookup(key)`: Translates to `SELECT * FROM table WHERE pk = ?` against SQLite.
    *   `process_records(ops)`: **No-op.** Noria does not persist writes; SQLite has already done so.
    *   `is_partial()`: Always `true` (conceptually).

### Component B: The "Session Observer" (Ingestion Layer)
*Location: `server/src/controller/sql/session.rs` (New)*

To ensure robust Change Data Capture (CDC) without the complexity of triggers, we will utilize the **SQLite Session Extension** (`sqlite3session`).

*   **Prerequisite:** We must bundle a custom amalgamation of SQLite compiled with `-DSQLITE_ENABLE_SESSION -DSQLITE_ENABLE_PREUPDATE_HOOK`.
*   **Mechanism:**
    1.  **Attach Session:** On connection open, attach a `sqlite3_session` to the database connection.
    2.  **Capture Changes:** The session extension automatically records all `INSERT`, `UPDATE`, and `DELETE` operations.
    3.  **Commit Hook:** On transaction commit, we extract the `changeset` (which includes both NEW and OLD values).
    4.  **Ingestor Thread:**
        *   Parses the binary changeset.
        *   Converts operations into Noria `Modification` packets (using OLD values to generate retractions).
        *   Injects packets into the Noria dataflow graph.

### Component C: The "Magic" Adapter (Client Layer)
*Location: `noria-sqlite/src/lib.rs` (New Crate)*

This is the user-facing API (Rust) and the FFI core for other languages. It implements **Dynamic View Synthesis**.

*   **Logic Flow:**
    1.  **Intercept:** Receive SQL string + Parameters.
    2.  **Normalize:** Identify if this is a **Prepared Statement** (stable shape) or an Ad-Hoc query.
    3.  **Cache Check:**
        *   *Hit:* Retrieve `ViewHandle`. Call `view.lookup(params)`.
        *   *Miss (First time seeing this query):*
            *   **Dynamic Synthesis:** Compile the SQL into a Noria graph segment.
            *   Register it as a parameterized view.
            *   Wait for graph activation.
            *   Perform lookup (triggering initial upquery).
    4.  **Consistency Guard:** If a write to Table T occurred < X ms ago (tracked via Session Observer), bypass Noria and read from SQLite to ensure "Read-Your-Writes" consistency.

---

## 3. Implementation Roadmap

### Phase 1: Core Surgery (Rust)
1.  **Fork Noria:** Create a custom branch.
2.  **Refactor State:** Abstract `PersistentState` usage in `Base` nodes to allow a generic `State` backend.
3.  **Implement `SqliteState`:** Build the `rusqlite` adapter for `State` trait.
4.  **Embedded Controller:** Expose `noria::ControllerHandle` via a simplified thread-safe API that starts the worker pool in-process.

### Phase 2: The Adapter & Bundling
1.  **Bundled SQLite:** create a build script (`cc` crate) to compile `sqlite3.c` with `SQLITE_ENABLE_SESSION`.
2.  **Session Integration:** Implement the Rust FFI bindings for `sqlite3session` to capture changesets.
3.  **Ingestor Loop:** Build the background thread that moves data from Session Changesets -> `Noria Graph`.
4.  **Dynamic Synthesis Logic:** Implement the "Cache-on-First-Sight" logic for prepared statements.

### Phase 3: Language Bindings (FFI & Polyfills)
1.  **Node.js Binding:**
    *   Use `napi-rs`.
    *   **Critical:** Implement the `better-sqlite3` API surface (`prepare`, `run`, `get`, `all`).
2.  **Python Binding:**
    *   Use `PyO3`.
    *   **Critical:** Implement the Python DBAPI 2.0 (PEP 249) interface.

---

## 4. Developer Experience (DX) Goals

*   **Installation:** `npm install noria-sqlite` (Pre-built binaries).
*   **Usage (Node.js example):**

    ```javascript
    // Drop-in replacement for better-sqlite3
    const Database = require('noria-sqlite');
    const db = new Database('app.db');

    // Works with Prisma, Drizzle, etc. because it walks and quacks like a standard driver
    const stmt = db.prepare("SELECT * FROM users WHERE id = ?");
    const user = stmt.get(1); // Accelerates automatically after first run
    ```

*   **Safety:**
    *   If Noria crashes, the adapter seamlessly falls back to raw SQLite.
    *   If a query is unsupported by Noria's parser, it falls back to raw SQLite.
    *   Memory usage is capped (default 100MB).

---

## 5. Risks & Mitigations

| Risk | Mitigation |
| :--- | :--- |
| **Write Overhead** | Session extension is efficient, but we must ensure ingestor is async. |
| **Consistency Lag** | Implement "Hybrid Mode" (Bypass cache for recent writes). |
| **Unsupported SQL** | Fail-open parser: if Noria doesn't understand it, SQLite runs it. |
| **OOM (Memory)** | Strict LRU eviction on the view cache; hard memory limit on Noria worker. |
| **Cache Thrashing** | Only cache **Prepared Statements**, ignore random ad-hoc strings. |
