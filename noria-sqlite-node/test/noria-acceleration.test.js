'use strict';
/**
 * Noria Acceleration Tests
 *
 * These tests verify Noria-specific functionality:
 * - Incremental view maintenance
 * - Cache hits/misses
 * - Transaction-aware CDC
 * - Cache statistics API
 */

const fs = require('fs');
const path = require('path');
const { expect } = require('chai');
const Database = require('../index.js');

// Test utilities
const tempDir = path.join(__dirname, 'temp');
let dbId = 1000; // Start at different range to avoid conflicts

const util = {
	current: () => path.join(tempDir, `noria-${dbId}.db`),
	next: () => (++dbId, util.current()),
};

// Setup/teardown
before(function () {
	if (!fs.existsSync(tempDir)) {
		fs.mkdirSync(tempDir, { recursive: true });
	}
});

after(function () {
	// Cleanup is handled by better-sqlite3-compat.test.js
});

// ============================================================================
// Basic Noria Acceleration Tests
// ============================================================================
describe('Noria Acceleration', function () {
	beforeEach(function () {
		this.db = new Database(util.next());
		this.db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
		this.db.exec('CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT)');
	});
	afterEach(function () {
		this.db.close();
	});

	it('should work with parameterized queries', function () {
		this.db.prepare('INSERT INTO users VALUES (?, ?, ?)').run(1, 'Alice', 30);
		this.db.prepare('INSERT INTO users VALUES (?, ?, ?)').run(2, 'Bob', 25);

		const stmt = this.db.prepare('SELECT * FROM users WHERE id = ?');

		const alice = stmt.get(1);
		expect(alice).to.deep.equal({ id: 1, name: 'Alice', age: 30 });

		const bob = stmt.get(2);
		expect(bob).to.deep.equal({ id: 2, name: 'Bob', age: 25 });

		expect(stmt.get(999)).to.be.undefined;
	});

	it('should reflect INSERT changes', function () {
		const stmt = this.db.prepare('SELECT * FROM users WHERE id = ?');

		expect(stmt.get(1)).to.be.undefined;

		this.db.prepare('INSERT INTO users VALUES (?, ?, ?)').run(1, 'Alice', 30);

		const result = stmt.get(1);
		expect(result).to.deep.equal({ id: 1, name: 'Alice', age: 30 });
	});

	it('should reflect UPDATE changes', function () {
		this.db.prepare('INSERT INTO users VALUES (?, ?, ?)').run(1, 'Alice', 30);

		const stmt = this.db.prepare('SELECT * FROM users WHERE id = ?');

		let result = stmt.get(1);
		expect(result.name).to.equal('Alice');

		this.db.prepare('UPDATE users SET name = ? WHERE id = ?').run('Alicia', 1);

		result = stmt.get(1);
		expect(result.name).to.equal('Alicia');
	});

	it('should reflect DELETE changes', function () {
		this.db.prepare('INSERT INTO users VALUES (?, ?, ?)').run(1, 'Alice', 30);

		const stmt = this.db.prepare('SELECT * FROM users WHERE id = ?');

		expect(stmt.get(1)).to.not.be.undefined;

		this.db.prepare('DELETE FROM users WHERE id = ?').run(1);

		expect(stmt.get(1)).to.be.undefined;
	});

	it('should work with all() for multiple rows', function () {
		this.db.prepare('INSERT INTO users VALUES (?, ?, ?)').run(1, 'Alice', 30);
		this.db.prepare('INSERT INTO users VALUES (?, ?, ?)').run(2, 'Bob', 30);
		this.db.prepare('INSERT INTO users VALUES (?, ?, ?)').run(3, 'Charlie', 25);

		const stmt = this.db.prepare('SELECT name FROM users WHERE age = ?');

		const age30 = stmt.all(30);
		expect(age30).to.have.lengthOf(2);
		expect(age30.map(r => r.name).sort()).to.deep.equal(['Alice', 'Bob']);

		const age25 = stmt.all(25);
		expect(age25).to.have.lengthOf(1);
		expect(age25[0].name).to.equal('Charlie');
	});

	it('should work with pluck mode', function () {
		this.db.prepare('INSERT INTO users VALUES (?, ?, ?)').run(1, 'Alice', 30);

		const stmt = this.db.prepare('SELECT name FROM users WHERE id = ?').pluck();
		expect(stmt.get(1)).to.equal('Alice');
	});

	it('should work with raw mode', function () {
		this.db.prepare('INSERT INTO users VALUES (?, ?, ?)').run(1, 'Alice', 30);

		const stmt = this.db.prepare('SELECT id, name, age FROM users WHERE id = ?').raw();
		expect(stmt.get(1)).to.deep.equal([1, 'Alice', 30]);
	});

	it('should work with multiple concurrent queries', function () {
		for (let i = 1; i <= 10; i++) {
			this.db.prepare('INSERT INTO users VALUES (?, ?, ?)').run(i, `User${i}`, 20 + i);
		}

		const byId = this.db.prepare('SELECT * FROM users WHERE id = ?');
		const byAge = this.db.prepare('SELECT * FROM users WHERE age = ?');

		expect(byId.get(1).name).to.equal('User1');
		expect(byAge.get(25).name).to.equal('User5');
		expect(byId.get(3).name).to.equal('User3');
		expect(byAge.get(30).name).to.equal('User10');
	});
});

// ============================================================================
// Transaction-Aware CDC Tests
// These tests comprehensively verify that CDC events are only applied on COMMIT
// ============================================================================
describe('Transaction-Aware CDC', function () {
	beforeEach(function () {
		this.db = new Database(util.next());
		this.db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
	});
	afterEach(function () {
		this.db.close();
	});

	describe('Basic Transaction Behavior', function () {
		it('should apply CDC events on successful transaction commit', function () {
			const insert = this.db.prepare('INSERT INTO users VALUES (?, ?, ?)');
			const query = this.db.prepare('SELECT * FROM users WHERE id = ?');

			const insertMany = this.db.transaction((users) => {
				for (const u of users) {
					insert.run(u.id, u.name, u.age);
				}
			});

			insertMany([
				{ id: 1, name: 'Alice', age: 30 },
				{ id: 2, name: 'Bob', age: 25 },
				{ id: 3, name: 'Charlie', age: 35 },
			]);

			// All data should be visible after commit
			expect(query.get(1).name).to.equal('Alice');
			expect(query.get(2).name).to.equal('Bob');
			expect(query.get(3).name).to.equal('Charlie');
		});

		it('should discard CDC events on transaction rollback', function () {
			const insert = this.db.prepare('INSERT INTO users VALUES (?, ?, ?)');
			const query = this.db.prepare('SELECT * FROM users WHERE id = ?');

			// Insert one row successfully
			insert.run(1, 'Alice', 30);
			expect(query.get(1).name).to.equal('Alice');

			// Try a failing transaction
			const badTransaction = this.db.transaction(() => {
				insert.run(2, 'Bob', 25);
				throw new Error('Rollback!');
			});

			try {
				badTransaction();
			} catch (e) {
				// Expected
			}

			// Bob should not exist (rolled back)
			expect(query.get(2)).to.be.undefined;

			// Alice should still exist
			expect(query.get(1).name).to.equal('Alice');
		});

		it('should handle multiple inserts in rolled-back transaction', function () {
			const insert = this.db.prepare('INSERT INTO users VALUES (?, ?, ?)');
			const query = this.db.prepare('SELECT * FROM users WHERE id = ?');

			const badTransaction = this.db.transaction(() => {
				insert.run(1, 'Alice', 30);
				insert.run(2, 'Bob', 25);
				insert.run(3, 'Charlie', 35);
				throw new Error('Rollback all!');
			});

			try {
				badTransaction();
			} catch (e) {
				// Expected
			}

			// None of the rows should exist
			expect(query.get(1)).to.be.undefined;
			expect(query.get(2)).to.be.undefined;
			expect(query.get(3)).to.be.undefined;
		});
	});

	describe('UPDATE in Transactions', function () {
		it('should apply UPDATE on commit', function () {
			const insert = this.db.prepare('INSERT INTO users VALUES (?, ?, ?)');
			const update = this.db.prepare('UPDATE users SET name = ? WHERE id = ?');
			const query = this.db.prepare('SELECT * FROM users WHERE id = ?');

			insert.run(1, 'Alice', 30);
			expect(query.get(1).name).to.equal('Alice');

			const updateTx = this.db.transaction(() => {
				update.run('Alicia', 1);
			});
			updateTx();

			expect(query.get(1).name).to.equal('Alicia');
		});

		it('should discard UPDATE on rollback', function () {
			const insert = this.db.prepare('INSERT INTO users VALUES (?, ?, ?)');
			const update = this.db.prepare('UPDATE users SET name = ? WHERE id = ?');
			const query = this.db.prepare('SELECT * FROM users WHERE id = ?');

			insert.run(1, 'Alice', 30);
			expect(query.get(1).name).to.equal('Alice');

			const badUpdate = this.db.transaction(() => {
				update.run('ShouldNotPersist', 1);
				throw new Error('Rollback!');
			});

			try {
				badUpdate();
			} catch (e) {
				// Expected
			}

			// Name should still be Alice
			expect(query.get(1).name).to.equal('Alice');
		});

		it('should handle multiple UPDATEs to same row in rollback', function () {
			const insert = this.db.prepare('INSERT INTO users VALUES (?, ?, ?)');
			const update = this.db.prepare('UPDATE users SET name = ? WHERE id = ?');
			const query = this.db.prepare('SELECT * FROM users WHERE id = ?');

			insert.run(1, 'Alice', 30);

			const badTx = this.db.transaction(() => {
				update.run('Bob', 1);
				update.run('Charlie', 1);
				update.run('David', 1);
				throw new Error('Rollback!');
			});

			try {
				badTx();
			} catch (e) {
				// Expected
			}

			// Should still be Alice
			expect(query.get(1).name).to.equal('Alice');
		});
	});

	describe('DELETE in Transactions', function () {
		it('should apply DELETE on commit', function () {
			const insert = this.db.prepare('INSERT INTO users VALUES (?, ?, ?)');
			const del = this.db.prepare('DELETE FROM users WHERE id = ?');
			const query = this.db.prepare('SELECT * FROM users WHERE id = ?');

			insert.run(1, 'Alice', 30);
			expect(query.get(1)).to.not.be.undefined;

			const deleteTx = this.db.transaction(() => {
				del.run(1);
			});
			deleteTx();

			expect(query.get(1)).to.be.undefined;
		});

		it('should discard DELETE on rollback', function () {
			const insert = this.db.prepare('INSERT INTO users VALUES (?, ?, ?)');
			const del = this.db.prepare('DELETE FROM users WHERE id = ?');
			const query = this.db.prepare('SELECT * FROM users WHERE id = ?');

			insert.run(1, 'Alice', 30);
			expect(query.get(1).name).to.equal('Alice');

			const badDelete = this.db.transaction(() => {
				del.run(1);
				throw new Error('Rollback!');
			});

			try {
				badDelete();
			} catch (e) {
				// Expected
			}

			// Alice should still exist
			expect(query.get(1).name).to.equal('Alice');
		});

		it('should handle DELETE of multiple rows in rollback', function () {
			const insert = this.db.prepare('INSERT INTO users VALUES (?, ?, ?)');
			const del = this.db.prepare('DELETE FROM users WHERE id = ?');
			const query = this.db.prepare('SELECT * FROM users WHERE id = ?');

			insert.run(1, 'Alice', 30);
			insert.run(2, 'Bob', 25);
			insert.run(3, 'Charlie', 35);

			const badTx = this.db.transaction(() => {
				del.run(1);
				del.run(2);
				del.run(3);
				throw new Error('Rollback!');
			});

			try {
				badTx();
			} catch (e) {
				// Expected
			}

			// All rows should still exist
			expect(query.get(1).name).to.equal('Alice');
			expect(query.get(2).name).to.equal('Bob');
			expect(query.get(3).name).to.equal('Charlie');
		});
	});

	describe('Mixed Operations in Transactions', function () {
		it('should handle INSERT + UPDATE + DELETE in committed transaction', function () {
			const insert = this.db.prepare('INSERT INTO users VALUES (?, ?, ?)');
			const update = this.db.prepare('UPDATE users SET name = ? WHERE id = ?');
			const del = this.db.prepare('DELETE FROM users WHERE id = ?');
			const query = this.db.prepare('SELECT * FROM users WHERE id = ?');

			// Setup initial data
			insert.run(1, 'Alice', 30);

			const mixedTx = this.db.transaction(() => {
				insert.run(2, 'Bob', 25);
				update.run('Alicia', 1);
				insert.run(3, 'Charlie', 35);
				del.run(2);
			});
			mixedTx();

			expect(query.get(1).name).to.equal('Alicia'); // Updated
			expect(query.get(2)).to.be.undefined; // Inserted then deleted
			expect(query.get(3).name).to.equal('Charlie'); // Inserted
		});

		it('should handle INSERT + UPDATE + DELETE in rolled-back transaction', function () {
			const insert = this.db.prepare('INSERT INTO users VALUES (?, ?, ?)');
			const update = this.db.prepare('UPDATE users SET name = ? WHERE id = ?');
			const del = this.db.prepare('DELETE FROM users WHERE id = ?');
			const query = this.db.prepare('SELECT * FROM users WHERE id = ?');

			// Setup initial data
			insert.run(1, 'Alice', 30);
			insert.run(2, 'Bob', 25);

			const badTx = this.db.transaction(() => {
				insert.run(3, 'Charlie', 35);
				update.run('Alicia', 1);
				del.run(2);
				throw new Error('Rollback!');
			});

			try {
				badTx();
			} catch (e) {
				// Expected
			}

			expect(query.get(1).name).to.equal('Alice'); // Not updated
			expect(query.get(2).name).to.equal('Bob'); // Not deleted
			expect(query.get(3)).to.be.undefined; // Not inserted
		});
	});

	describe('Nested Transactions', function () {
		it('should handle savepoint-like behavior with nested transactions', function () {
			const insert = this.db.prepare('INSERT INTO users VALUES (?, ?, ?)');
			const query = this.db.prepare('SELECT * FROM users WHERE id = ?');

			// better-sqlite3's transaction() doesn't support true nesting,
			// but inner transactions should still work correctly
			const outerTx = this.db.transaction(() => {
				insert.run(1, 'Alice', 30);

				// This is not truly nested - inner transaction commits independently
				// but if outer fails, we want consistent behavior
			});

			outerTx();
			expect(query.get(1).name).to.equal('Alice');
		});
	});

	describe('Autocommit Mode', function () {
		it('should apply changes immediately in autocommit mode', function () {
			const insert = this.db.prepare('INSERT INTO users VALUES (?, ?, ?)');
			const query = this.db.prepare('SELECT * FROM users WHERE id = ?');

			// Each insert is its own transaction in autocommit mode
			insert.run(1, 'Alice', 30);
			expect(query.get(1).name).to.equal('Alice');

			insert.run(2, 'Bob', 25);
			expect(query.get(2).name).to.equal('Bob');
		});

		it('should not affect previously committed data on later rollback', function () {
			const insert = this.db.prepare('INSERT INTO users VALUES (?, ?, ?)');
			const query = this.db.prepare('SELECT * FROM users WHERE id = ?');

			// Autocommit inserts
			insert.run(1, 'Alice', 30);
			insert.run(2, 'Bob', 25);

			// Rolled-back transaction
			const badTx = this.db.transaction(() => {
				insert.run(3, 'Charlie', 35);
				throw new Error('Rollback!');
			});

			try {
				badTx();
			} catch (e) {
				// Expected
			}

			// Previously committed data should be intact
			expect(query.get(1).name).to.equal('Alice');
			expect(query.get(2).name).to.equal('Bob');
			expect(query.get(3)).to.be.undefined;
		});
	});

	describe('Edge Cases', function () {
		it('should handle empty transaction commit', function () {
			const query = this.db.prepare('SELECT * FROM users WHERE id = ?');

			const emptyTx = this.db.transaction(() => {
				// Do nothing
			});
			emptyTx();

			// Should still work
			expect(query.get(1)).to.be.undefined;
		});

		it('should handle empty transaction rollback', function () {
			const insert = this.db.prepare('INSERT INTO users VALUES (?, ?, ?)');
			const query = this.db.prepare('SELECT * FROM users WHERE id = ?');

			insert.run(1, 'Alice', 30);

			const emptyTx = this.db.transaction(() => {
				throw new Error('Rollback empty!');
			});

			try {
				emptyTx();
			} catch (e) {
				// Expected
			}

			// Previous data should be intact
			expect(query.get(1).name).to.equal('Alice');
		});

		it('should handle transaction after previous rollback', function () {
			const insert = this.db.prepare('INSERT INTO users VALUES (?, ?, ?)');
			const query = this.db.prepare('SELECT * FROM users WHERE id = ?');

			// First transaction fails
			const badTx = this.db.transaction(() => {
				insert.run(1, 'Alice', 30);
				throw new Error('Rollback!');
			});

			try {
				badTx();
			} catch (e) {
				// Expected
			}

			expect(query.get(1)).to.be.undefined;

			// Second transaction succeeds
			const goodTx = this.db.transaction(() => {
				insert.run(1, 'Bob', 25);
			});
			goodTx();

			expect(query.get(1).name).to.equal('Bob');
		});

		it('should handle rapid transaction commit/rollback cycles', function () {
			const insert = this.db.prepare('INSERT INTO users VALUES (?, ?, ?)');
			const del = this.db.prepare('DELETE FROM users WHERE id = ?');
			const query = this.db.prepare('SELECT * FROM users WHERE id = ?');

			for (let i = 0; i < 10; i++) {
				if (i % 2 === 0) {
					// Even: commit
					const tx = this.db.transaction(() => {
						insert.run(i, `User${i}`, 20 + i);
					});
					tx();
				} else {
					// Odd: rollback
					const tx = this.db.transaction(() => {
						insert.run(i, `User${i}`, 20 + i);
						throw new Error('Rollback!');
					});
					try {
						tx();
					} catch (e) {
						// Expected
					}
				}
			}

			// Only even IDs should exist
			for (let i = 0; i < 10; i++) {
				if (i % 2 === 0) {
					expect(query.get(i).name).to.equal(`User${i}`);
				} else {
					expect(query.get(i)).to.be.undefined;
				}
			}
		});
	});
});

// ============================================================================
// Cache Statistics API Tests
// ============================================================================
describe('Cache Statistics API', function () {
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
		expect(stats).to.have.property('materializedNodes');
		expect(stats).to.have.property('totalRows');
		expect(stats).to.have.property('viewCount');
		expect(stats).to.have.property('cacheHits');
		expect(stats).to.have.property('cacheMisses');
	});

	it('should have non-negative numeric values', function () {
		const stats = this.db.cacheStats();

		expect(stats.nodeCount).to.be.a('number').and.at.least(0);
		expect(stats.materializedNodes).to.be.a('number').and.at.least(0);
		expect(stats.totalRows).to.be.a('number').and.at.least(0);
		expect(stats.viewCount).to.be.a('number').and.at.least(0);
		expect(stats.cacheHits).to.be.a('number').and.at.least(0);
		expect(stats.cacheMisses).to.be.a('number').and.at.least(0);
	});

	it('should start with zero cache hits and misses', function () {
		const stats = this.db.cacheStats();

		expect(stats.cacheHits).to.equal(0);
		expect(stats.cacheMisses).to.equal(0);
	});

	it('should track cache misses on first query (upquery)', function () {
		this.db.prepare('INSERT INTO users VALUES (?, ?, ?)').run(1, 'Alice', 30);

		const stmt = this.db.prepare('SELECT * FROM users WHERE id = ?');

		const statsBefore = this.db.cacheStats();
		const initialMisses = statsBefore.cacheMisses;

		stmt.get(1);

		const statsAfter = this.db.cacheStats();
		expect(statsAfter.cacheMisses).to.be.greaterThan(initialMisses);
	});

	it('should track cache hits on subsequent queries', function () {
		this.db.prepare('INSERT INTO users VALUES (?, ?, ?)').run(1, 'Alice', 30);

		const stmt = this.db.prepare('SELECT * FROM users WHERE id = ?');

		stmt.get(1);

		const statsBefore = this.db.cacheStats();
		const initialHits = statsBefore.cacheHits;

		stmt.get(1);

		const statsAfter = this.db.cacheStats();
		expect(statsAfter.cacheHits).to.be.greaterThan(initialHits);
	});

	it('should increment viewCount when queries are prepared', function () {
		const statsBefore = this.db.cacheStats();
		const initialViews = statsBefore.viewCount;

		this.db.prepare('SELECT * FROM users WHERE id = ?');

		const statsAfter = this.db.cacheStats();
		expect(statsAfter.viewCount).to.be.greaterThan(initialViews);
	});

	it('should track totalRows in materialized views', function () {
		this.db.prepare('INSERT INTO users VALUES (?, ?, ?)').run(1, 'Alice', 30);
		this.db.prepare('INSERT INTO users VALUES (?, ?, ?)').run(2, 'Bob', 25);

		const stmt = this.db.prepare('SELECT * FROM users WHERE id = ?');
		stmt.get(1);
		stmt.get(2);

		const stats = this.db.cacheStats();
		expect(stats.totalRows).to.be.at.least(2);
	});

	it('should calculate hit rate correctly', function () {
		this.db.prepare('INSERT INTO users VALUES (?, ?, ?)').run(1, 'Alice', 30);

		const stmt = this.db.prepare('SELECT * FROM users WHERE id = ?');

		// First query (miss)
		stmt.get(1);

		// Several more queries (hits)
		for (let i = 0; i < 5; i++) {
			stmt.get(1);
		}

		const stats = this.db.cacheStats();
		const hitRate = stats.cacheHits / (stats.cacheHits + stats.cacheMisses);

		expect(hitRate).to.be.greaterThan(0.8);
	});
});
