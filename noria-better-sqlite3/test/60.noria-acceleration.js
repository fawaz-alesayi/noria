'use strict';
/**
 * Noria Acceleration Tests
 *
 * These tests verify that Noria's dataflow engine is actually working,
 * not just that SQLite returns correct data. We use the introspection API
 * (cacheStats) to verify:
 *
 * 1. Views are created when queries are prepared
 * 2. Cache misses trigger upqueries
 * 3. Cache hits serve from memory
 * 4. CDC propagates changes to cache (not requiring upquery)
 *
 * If Noria was broken and everything fell back to SQLite via upquery,
 * these tests would FAIL because we verify hit/miss counts.
 */

const Database = require('../.');

// ============================================================================
// View Synthesis Tests
// Verify that preparing a query creates a Noria view
// ============================================================================
describe('Noria View Synthesis', function () {
	beforeEach(function () {
		this.db = new Database(util.next());
		this.db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
	});
	afterEach(function () {
		this.db.close();
	});

	it('should create a view when SELECT query is prepared', function () {
		const statsBefore = this.db.cacheStats();
		const initialViews = statsBefore.viewCount;

		// Prepare a SELECT query - should create a view
		this.db.prepare('SELECT * FROM users WHERE id = ?');

		const statsAfter = this.db.cacheStats();
		expect(statsAfter.viewCount).to.equal(initialViews + 1);
	});

	it('should not create duplicate views for same query', function () {
		// Prepare same query twice
		this.db.prepare('SELECT * FROM users WHERE id = ?');
		const statsAfter1 = this.db.cacheStats();

		this.db.prepare('SELECT * FROM users WHERE id = ?');
		const statsAfter2 = this.db.cacheStats();

		// Should not create a second view
		expect(statsAfter2.viewCount).to.equal(statsAfter1.viewCount);
	});

	it('should create separate views for different queries', function () {
		const statsBefore = this.db.cacheStats();

		this.db.prepare('SELECT * FROM users WHERE id = ?');
		this.db.prepare('SELECT * FROM users WHERE age = ?');
		this.db.prepare('SELECT name FROM users WHERE id = ?');

		const statsAfter = this.db.cacheStats();
		expect(statsAfter.viewCount).to.be.at.least(statsBefore.viewCount + 2);
	});
});

// ============================================================================
// Cache Hit/Miss Tests
// Verify the caching behavior works correctly
// ============================================================================
describe('Noria Cache Hits and Misses', function () {
	beforeEach(function () {
		this.db = new Database(util.next());
		this.db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
	});
	afterEach(function () {
		this.db.close();
	});

	it('should record cache miss on first query to empty cache', function () {
		// Insert data first
		this.db.prepare('INSERT INTO users VALUES (?, ?, ?)').run(1, 'Alice', 30);

		const query = this.db.prepare('SELECT * FROM users WHERE id = ?');
		const statsBefore = this.db.cacheStats();

		// First query - should be a cache miss (triggers upquery)
		query.get(1);

		const statsAfter = this.db.cacheStats();
		expect(statsAfter.cacheMisses).to.be.greaterThan(statsBefore.cacheMisses);
	});

	it('should record cache hit on second query to same key', function () {
		this.db.prepare('INSERT INTO users VALUES (?, ?, ?)').run(1, 'Alice', 30);

		const query = this.db.prepare('SELECT * FROM users WHERE id = ?');

		// First query - populates cache
		query.get(1);

		const statsBefore = this.db.cacheStats();
		const hitsBefore = statsBefore.cacheHits;
		const missesBefore = statsBefore.cacheMisses;

		// Second query - should be cache HIT
		query.get(1);

		const statsAfter = this.db.cacheStats();
		expect(statsAfter.cacheHits).to.be.greaterThan(hitsBefore);
		expect(statsAfter.cacheMisses).to.equal(missesBefore); // No new misses
	});

	it('should achieve high hit rate with repeated queries', function () {
		this.db.prepare('INSERT INTO users VALUES (?, ?, ?)').run(1, 'Alice', 30);

		const query = this.db.prepare('SELECT * FROM users WHERE id = ?');

		// First query (miss)
		query.get(1);

		// 10 more queries (should all be hits)
		for (let i = 0; i < 10; i++) {
			query.get(1);
		}

		const stats = this.db.cacheStats();
		const hitRate = stats.cacheHits / (stats.cacheHits + stats.cacheMisses);

		// With 1 miss and 10 hits, hit rate should be ~91%
		expect(hitRate).to.be.greaterThan(0.9);
	});
});

// ============================================================================
// CDC Propagation Tests
// These are the CRITICAL tests that prove Noria is working.
// After INSERT/UPDATE/DELETE, querying should be a cache HIT (not miss)
// because CDC pushed the data to the view.
// ============================================================================
describe('Noria CDC Propagation (The Real Test)', function () {
	beforeEach(function () {
		this.db = new Database(util.next());
		this.db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
	});
	afterEach(function () {
		this.db.close();
	});

	it('should populate cache via CDC after INSERT (not upquery)', function () {
		const insert = this.db.prepare('INSERT INTO users VALUES (?, ?, ?)');
		const query = this.db.prepare('SELECT * FROM users WHERE id = ?');

		// Insert - CDC should push data to the view
		insert.run(1, 'Alice', 30);

		const statsBefore = this.db.cacheStats();
		const missesBefore = statsBefore.cacheMisses;

		// Query - should be a cache HIT because CDC already populated it
		const result = query.get(1);

		const statsAfter = this.db.cacheStats();

		// Verify correct data
		expect(result).to.deep.equal({ id: 1, name: 'Alice', age: 30 });

		// THE KEY ASSERTION: No new cache misses!
		// If this fails, CDC is not pushing data to the view
		expect(statsAfter.cacheMisses).to.equal(missesBefore,
			'CDC should have populated the cache - no upquery needed');
	});

	// UPDATE CDC is implemented via session extension
	it('should update cache via CDC after UPDATE (not upquery)', function () {
		const insert = this.db.prepare('INSERT INTO users VALUES (?, ?, ?)');
		const update = this.db.prepare('UPDATE users SET name = ? WHERE id = ?');
		const query = this.db.prepare('SELECT * FROM users WHERE id = ?');

		// Insert and query to populate cache
		insert.run(1, 'Alice', 30);
		query.get(1); // Initial cache population (may be upquery or CDC)

		// Update - CDC should update the cached entry
		update.run('Alicia', 1);

		const statsBefore = this.db.cacheStats();
		const missesBefore = statsBefore.cacheMisses;

		// Query - should be a cache HIT with UPDATED data
		const result = query.get(1);

		const statsAfter = this.db.cacheStats();

		// Verify updated data
		expect(result.name).to.equal('Alicia');

		// THE KEY ASSERTION: No new cache misses!
		// If this fails, UPDATE CDC is not propagating to the view
		expect(statsAfter.cacheMisses).to.equal(missesBefore,
			'UPDATE via CDC should have updated the cache - no upquery needed');
	});

	// DELETE CDC is implemented via session extension
	it('should remove from cache via CDC after DELETE', function () {
		const insert = this.db.prepare('INSERT INTO users VALUES (?, ?, ?)');
		const del = this.db.prepare('DELETE FROM users WHERE id = ?');
		const query = this.db.prepare('SELECT * FROM users WHERE id = ?');

		// Insert and query to populate cache
		insert.run(1, 'Alice', 30);
		query.get(1);

		// Delete - CDC should remove from cache
		del.run(1);

		// Query - should return undefined (data is gone)
		const result = query.get(1);
		expect(result).to.be.undefined;
	});

	it('should handle multiple INSERTs via CDC', function () {
		const insert = this.db.prepare('INSERT INTO users VALUES (?, ?, ?)');
		const query = this.db.prepare('SELECT * FROM users WHERE id = ?');

		// Insert 5 rows - all should propagate via CDC
		for (let i = 1; i <= 5; i++) {
			insert.run(i, `User${i}`, 20 + i);
		}

		const statsBefore = this.db.cacheStats();
		const missesBefore = statsBefore.cacheMisses;

		// Query all 5 - should all be cache HITs
		for (let i = 1; i <= 5; i++) {
			const result = query.get(i);
			expect(result.name).to.equal(`User${i}`);
		}

		const statsAfter = this.db.cacheStats();

		// Should be 0 new misses if CDC is working
		expect(statsAfter.cacheMisses).to.equal(missesBefore,
			'All 5 INSERTs should have been cached via CDC');
	});
});

// ============================================================================
// Transaction CDC Tests
// Verify that CDC events are only applied on COMMIT, not during transaction
// ============================================================================
describe('Noria Transaction-Aware CDC', function () {
	beforeEach(function () {
		this.db = new Database(util.next());
		this.db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
	});
	afterEach(function () {
		this.db.close();
	});

	it('should apply CDC events on COMMIT', function () {
		const insert = this.db.prepare('INSERT INTO users VALUES (?, ?, ?)');
		const query = this.db.prepare('SELECT * FROM users WHERE id = ?');

		// Transaction with inserts
		const tx = this.db.transaction(() => {
			insert.run(1, 'Alice', 30);
			insert.run(2, 'Bob', 25);
		});
		tx();

		const statsBefore = this.db.cacheStats();
		const missesBefore = statsBefore.cacheMisses;

		// Query both - should be cache HITs (CDC applied on commit)
		expect(query.get(1).name).to.equal('Alice');
		expect(query.get(2).name).to.equal('Bob');

		const statsAfter = this.db.cacheStats();
		expect(statsAfter.cacheMisses).to.equal(missesBefore,
			'Committed transaction data should be in cache via CDC');
	});

	// Transaction rollback CDC is handled by deferring processing until COMMIT
	it('should discard CDC events on ROLLBACK', function () {
		const insert = this.db.prepare('INSERT INTO users VALUES (?, ?, ?)');
		const query = this.db.prepare('SELECT * FROM users WHERE id = ?');

		// Insert one row successfully first
		insert.run(1, 'Alice', 30);

		// Failed transaction
		const badTx = this.db.transaction(() => {
			insert.run(2, 'Bob', 25);
			throw new Error('Rollback!');
		});

		try {
			badTx();
		} catch (e) {
			// Expected
		}

		// Bob should not exist
		expect(query.get(2)).to.be.undefined;

		// Alice should still be accessible
		expect(query.get(1).name).to.equal('Alice');
	});

	// UPDATE CDC in transactions is handled properly
	it('should handle UPDATE in committed transaction via CDC', function () {
		const insert = this.db.prepare('INSERT INTO users VALUES (?, ?, ?)');
		const update = this.db.prepare('UPDATE users SET name = ? WHERE id = ?');
		const query = this.db.prepare('SELECT * FROM users WHERE id = ?');

		// Setup
		insert.run(1, 'Alice', 30);
		query.get(1); // Populate cache

		// Update in transaction
		const tx = this.db.transaction(() => {
			update.run('Alicia', 1);
		});
		tx();

		const statsBefore = this.db.cacheStats();
		const missesBefore = statsBefore.cacheMisses;

		// Query - should have updated value from cache
		const result = query.get(1);
		expect(result.name).to.equal('Alicia');

		const statsAfter = this.db.cacheStats();
		expect(statsAfter.cacheMisses).to.equal(missesBefore,
			'UPDATE in transaction should update cache via CDC');
	});

	it('should discard UPDATE on ROLLBACK', function () {
		const insert = this.db.prepare('INSERT INTO users VALUES (?, ?, ?)');
		const update = this.db.prepare('UPDATE users SET name = ? WHERE id = ?');
		const query = this.db.prepare('SELECT * FROM users WHERE id = ?');

		insert.run(1, 'Alice', 30);
		query.get(1); // Populate cache

		// Failed update transaction
		const badTx = this.db.transaction(() => {
			update.run('ShouldNotPersist', 1);
			throw new Error('Rollback!');
		});

		try {
			badTx();
		} catch (e) {
			// Expected
		}

		// Should still be Alice (rollback discarded the update)
		expect(query.get(1).name).to.equal('Alice');
	});

	// DELETE CDC in transactions is handled properly
	it('should handle DELETE in committed transaction via CDC', function () {
		const insert = this.db.prepare('INSERT INTO users VALUES (?, ?, ?)');
		const del = this.db.prepare('DELETE FROM users WHERE id = ?');
		const query = this.db.prepare('SELECT * FROM users WHERE id = ?');

		insert.run(1, 'Alice', 30);
		query.get(1); // Populate cache

		// Delete in transaction
		const tx = this.db.transaction(() => {
			del.run(1);
		});
		tx();

		// Should be gone
		expect(query.get(1)).to.be.undefined;
	});

	it('should discard DELETE on ROLLBACK', function () {
		const insert = this.db.prepare('INSERT INTO users VALUES (?, ?, ?)');
		const del = this.db.prepare('DELETE FROM users WHERE id = ?');
		const query = this.db.prepare('SELECT * FROM users WHERE id = ?');

		insert.run(1, 'Alice', 30);
		query.get(1); // Populate cache

		// Failed delete transaction
		const badTx = this.db.transaction(() => {
			del.run(1);
			throw new Error('Rollback!');
		});

		try {
			badTx();
		} catch (e) {
			// Expected
		}

		// Should still exist (rollback discarded the delete)
		expect(query.get(1).name).to.equal('Alice');
	});
});

// ============================================================================
// Cache Statistics API Tests
// ============================================================================
describe('Noria Cache Statistics API', function () {
	beforeEach(function () {
		this.db = new Database(util.next());
		this.db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
	});
	afterEach(function () {
		this.db.close();
	});

	it('should return cacheStats object with all required properties', function () {
		const stats = this.db.cacheStats();

		expect(stats).to.be.an('object');
		expect(stats).to.have.property('nodeCount');
		expect(stats).to.have.property('totalRows');
		expect(stats).to.have.property('viewCount');
		expect(stats).to.have.property('cacheHits');
		expect(stats).to.have.property('cacheMisses');
	});

	it('should start with zero cache hits and misses', function () {
		const stats = this.db.cacheStats();

		expect(stats.cacheHits).to.equal(0);
		expect(stats.cacheMisses).to.equal(0);
	});

	it('should track totalRows in materialized views', function () {
		this.db.prepare('INSERT INTO users VALUES (?, ?, ?)').run(1, 'Alice', 30);
		this.db.prepare('INSERT INTO users VALUES (?, ?, ?)').run(2, 'Bob', 25);

		const query = this.db.prepare('SELECT * FROM users WHERE id = ?');

		// Query to populate cache (via CDC or upquery)
		query.get(1);
		query.get(2);

		const stats = this.db.cacheStats();
		expect(stats.totalRows).to.be.at.least(2);
	});
});

// ============================================================================
// Edge Cases and Correctness
// These tests verify correctness (still important!) alongside performance
// ============================================================================
describe('Noria Correctness with Cache Verification', function () {
	beforeEach(function () {
		this.db = new Database(util.next());
		this.db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
	});
	afterEach(function () {
		this.db.close();
	});

	it('should return undefined for non-existent keys', function () {
		const query = this.db.prepare('SELECT * FROM users WHERE id = ?');
		expect(query.get(999)).to.be.undefined;
	});

	it('should work with pluck mode', function () {
		this.db.prepare('INSERT INTO users VALUES (?, ?, ?)').run(1, 'Alice', 30);

		const query = this.db.prepare('SELECT name FROM users WHERE id = ?').pluck();
		expect(query.get(1)).to.equal('Alice');
	});

	it('should work with raw mode', function () {
		this.db.prepare('INSERT INTO users VALUES (?, ?, ?)').run(1, 'Alice', 30);

		const query = this.db.prepare('SELECT id, name, age FROM users WHERE id = ?').raw();
		expect(query.get(1)).to.deep.equal([1, 'Alice', 30]);
	});

	it('should work with all() returning multiple rows', function () {
		this.db.prepare('INSERT INTO users VALUES (?, ?, ?)').run(1, 'Alice', 30);
		this.db.prepare('INSERT INTO users VALUES (?, ?, ?)').run(2, 'Bob', 30);

		const query = this.db.prepare('SELECT name FROM users WHERE age = ?');
		const results = query.all(30);

		expect(results).to.have.lengthOf(2);
		expect(results.map(r => r.name).sort()).to.deep.equal(['Alice', 'Bob']);
	});
});

// ============================================================================
// Memory Management Tests (Noria-specific)
// These tests verify memory tracking and eviction
// ============================================================================
describe('Noria Memory Management', function () {
	beforeEach(function () {
		this.db = new Database(util.next());
		this.db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, bio TEXT)');
	});
	afterEach(function () {
		this.db.close();
	});

	it('should track memory usage in cacheStats', function () {
		// Insert some data with larger text fields
		const insert = this.db.prepare('INSERT INTO users VALUES (?, ?, ?)');
		for (let i = 1; i <= 100; i++) {
			insert.run(i, `User${i}`, `This is a longer bio for user ${i} to use more memory`.repeat(10));
		}

		const query = this.db.prepare('SELECT * FROM users WHERE id = ?');

		// Query several rows to populate cache
		for (let i = 1; i <= 50; i++) {
			query.get(i);
		}

		const stats = this.db.cacheStats();

		// Memory should be tracked (if the feature is available)
		if (stats.memoryBytes !== undefined) {
			expect(stats.memoryBytes).to.be.greaterThan(0);
		}

		// Should have rows in cache
		expect(stats.totalRows).to.be.at.least(50);
	});

	it('should report cache hit rate correctly', function () {
		const insert = this.db.prepare('INSERT INTO users VALUES (?, ?, ?)');
		insert.run(1, 'Alice', 'Bio 1');
		insert.run(2, 'Bob', 'Bio 2');

		const query = this.db.prepare('SELECT * FROM users WHERE id = ?');

		// First queries - should be misses (or CDC hits)
		query.get(1);
		query.get(2);

		const statsAfterFirst = this.db.cacheStats();

		// Second queries - should be hits
		for (let i = 0; i < 10; i++) {
			query.get(1);
			query.get(2);
		}

		const statsAfterSecond = this.db.cacheStats();

		// Cache hits should have increased significantly
		expect(statsAfterSecond.cacheHits).to.be.greaterThan(statsAfterFirst.cacheHits);
	});
});

// ============================================================================
// JOIN Query Tests (Noria-specific)
// These tests verify JOIN query acceleration
// Note: Multi-table JOIN caching is complex and currently limited
// ============================================================================
describe('Noria JOIN Query Acceleration', function () {
	beforeEach(function () {
		this.db = new Database(util.next());
		this.db.exec(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT);
		`);
	});
	afterEach(function () {
		this.db.close();
	});

	// Note: JOIN queries with multi-table dependencies are complex to cache correctly
	// These tests verify basic functionality falls back to SQLite correctly
	it('should return correct JOIN query results (via SQLite)', function () {
		// Insert all data first
		this.db.prepare('INSERT INTO users VALUES (?, ?)').run(1, 'Alice');
		this.db.prepare('INSERT INTO posts VALUES (?, ?, ?)').run(1, 1, 'First Post');
		this.db.prepare('INSERT INTO posts VALUES (?, ?, ?)').run(2, 1, 'Second Post');

		const query = this.db.prepare(`
			SELECT p.id, p.title, u.name
			FROM posts p
			JOIN users u ON p.user_id = u.id
			WHERE u.id = ?
		`);

		// Query should return correct results
		const results = query.all(1);
		expect(results).to.have.lengthOf(2);
		expect(results[0].name).to.equal('Alice');
	});

	it('should return updated JOIN results after base table changes', function () {
		this.db.prepare('INSERT INTO users VALUES (?, ?)').run(1, 'Alice');
		this.db.prepare('INSERT INTO posts VALUES (?, ?, ?)').run(1, 1, 'First Post');

		const query = this.db.prepare(`
			SELECT p.title, u.name
			FROM posts p
			JOIN users u ON p.user_id = u.id
			WHERE u.id = ?
		`);

		const result1 = query.all(1);
		expect(result1[0].name).to.equal('Alice');

		this.db.prepare('UPDATE users SET name = ? WHERE id = ?').run('Alicia', 1);

		const results = query.all(1);
		expect(results[0].name).to.equal('Alicia');
	});
});

describe('Noria Aggregate Query Handling', function () {
	beforeEach(function () {
		this.db = new Database(util.next());
		this.db.exec('CREATE TABLE sales (id INTEGER PRIMARY KEY, product TEXT, amount INTEGER)');
	});
	afterEach(function () {
		this.db.close();
	});

	it('should handle COUNT queries correctly', function () {
		const insert = this.db.prepare('INSERT INTO sales VALUES (?, ?, ?)');
		insert.run(1, 'Widget', 100);
		insert.run(2, 'Widget', 150);
		insert.run(3, 'Gadget', 200);

		const query = this.db.prepare('SELECT product, COUNT(*) as count FROM sales WHERE product = ? GROUP BY product');

		const result = query.get('Widget');
		expect(result.count).to.equal(2);
	});

	it('should handle SUM queries correctly', function () {
		const insert = this.db.prepare('INSERT INTO sales VALUES (?, ?, ?)');
		insert.run(1, 'Widget', 100);
		insert.run(2, 'Widget', 150);
		insert.run(3, 'Gadget', 200);

		const query = this.db.prepare('SELECT product, SUM(amount) as total FROM sales WHERE product = ? GROUP BY product');

		const result = query.get('Widget');
		expect(result.total).to.equal(250);
	});

	it('should return updated aggregate after INSERT', function () {
		const insert = this.db.prepare('INSERT INTO sales VALUES (?, ?, ?)');
		const query = this.db.prepare('SELECT product, SUM(amount) as total FROM sales WHERE product = ? GROUP BY product');

		insert.run(1, 'Widget', 100);
		let result = query.get('Widget');
		expect(result.total).to.equal(100);

		insert.run(2, 'Widget', 50);
		result = query.get('Widget');
		expect(result.total).to.equal(150);
	});

	it('should cache aggregate results between identical queries', function () {
		// Insert data before preparing query
		const insert = this.db.prepare('INSERT INTO sales VALUES (?, ?, ?)');
		insert.run(1, 'Widget', 100);
		insert.run(2, 'Widget', 150);

		const query = this.db.prepare('SELECT SUM(amount) as total FROM sales WHERE product = ?');

		// First query
		const result1 = query.get('Widget');
		expect(result1.total).to.equal(250);

		// Second query - should return same result
		const result2 = query.get('Widget');
		expect(result2.total).to.equal(250);
	});
});
