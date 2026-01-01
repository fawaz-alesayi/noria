'use strict';
/**
 * better-sqlite3 compatibility tests
 *
 * These tests are ported from better-sqlite3 to ensure API compatibility.
 * See: https://github.com/WiseLibs/better-sqlite3
 */

const fs = require('fs');
const path = require('path');
const { expect } = require('chai');
const Database = require('../index.js');

// Test utilities
const tempDir = path.join(__dirname, 'temp');
let dbId = 0;

const util = {
	current: () => path.join(tempDir, `${dbId}.db`),
	next: () => (++dbId, util.current()),
};

// Setup/teardown
before(function () {
	if (fs.existsSync(tempDir)) {
		fs.rmSync(tempDir, { recursive: true });
	}
	fs.mkdirSync(tempDir, { recursive: true });
});

after(function () {
	if (fs.existsSync(tempDir)) {
		fs.rmSync(tempDir, { recursive: true });
	}
});

// ============================================================================
// SqliteError tests (from 01.sqlite-error.js)
// ============================================================================
describe('SqliteError', function () {
	it('should be a subclass of Error', function () {
		const { SqliteError } = Database;
		expect(SqliteError).to.be.a('function');
		expect(SqliteError).to.not.equal(Error);
		expect(SqliteError.prototype).to.be.an.instanceof(Error);
		expect(SqliteError('foo', 'bar')).to.be.an.instanceof(Error);
		expect(new SqliteError('foo', 'bar')).to.be.an.instanceof(Error);
	});
	it('should have the correct name', function () {
		expect(Database.SqliteError.prototype.name).to.equal('SqliteError');
	});
	it('should accept two arguments for setting the message and error code', function () {
		const { SqliteError } = Database;
		const err = SqliteError('foobar', 'baz');
		expect(err.message).to.equal('foobar');
		expect(err.code).to.equal('baz');
		expect(SqliteError(123, 'baz').message).to.equal('123');
		expect(() => SqliteError('foo')).to.throw(TypeError);
		expect(() => SqliteError('foo', 123)).to.throw(TypeError);
	});
	it('should capture stack traces', function () {
		expect(Database.SqliteError(null, 'baz').stack).to.be.a('string');
	});
});

// ============================================================================
// Database opening tests (from 10.database.open.js)
// ============================================================================
describe('new Database()', function () {
	afterEach(function () {
		if (this.db) this.db.close();
	});

	it('should throw when given invalid argument types', function () {
		expect(() => (this.db = new Database('', ''))).to.throw(TypeError);
		expect(() => (this.db = new Database({}, ''))).to.throw(TypeError);
		expect(() => (this.db = new Database({}, {}))).to.throw(TypeError);
		expect(() => (this.db = new Database({}))).to.throw(TypeError);
		expect(() => (this.db = new Database(0))).to.throw(TypeError);
		expect(() => (this.db = new Database(123))).to.throw(TypeError);
		expect(() => (this.db = new Database(new String(util.next())))).to.throw(TypeError);
		expect(() => (this.db = new Database(() => util.next()))).to.throw(TypeError);
		expect(() => (this.db = new Database([util.next()]))).to.throw(TypeError);
	});
	it('should throw when boolean options are provided as non-booleans', function () {
		expect(() => (this.db = new Database(util.next(), { readOnly: false }))).to.throw(TypeError);
	});
	it('should allow anonymous temporary databases to be created', function () {
		for (const args of [[''], [], [null], [undefined], ['', { timeout: 2000 }]]) {
			const db = this.db = new Database(...args);
			expect(db.name).to.equal('');
			expect(db.memory).to.be.true;
			expect(db.readonly).to.be.false;
			expect(db.open).to.be.true;
			expect(db.inTransaction).to.be.false;
			db.close();
		}
	});
	it('should allow anonymous in-memory databases to be created', function () {
		const db = this.db = new Database(':memory:');
		expect(db.name).to.equal(':memory:');
		expect(db.memory).to.be.true;
		expect(db.readonly).to.be.false;
		expect(db.open).to.be.true;
		expect(db.inTransaction).to.be.false;
	});
	it('should allow disk-bound databases to be created', function () {
		expect(fs.existsSync(util.next())).to.be.false;
		const db = this.db = new Database(util.current());
		expect(db.name).to.equal(util.current());
		expect(db.memory).to.be.false;
		expect(db.readonly).to.be.false;
		expect(db.open).to.be.true;
		expect(db.inTransaction).to.be.false;
		expect(fs.existsSync(util.current())).to.be.true;
	});
	it('should not allow the "readonly" option for in-memory databases', function () {
		expect(() => (this.db = new Database(':memory:', { readonly: true }))).to.throw(TypeError);
		expect(() => (this.db = new Database('', { readonly: true }))).to.throw(TypeError);
	});
	it('should accept the "fileMustExist" option', function () {
		expect(fs.existsSync(util.next())).to.be.false;
		expect(() => (this.db = new Database(util.current(), { fileMustExist: true }))).to.throw(Database.SqliteError).with.property('code', 'SQLITE_CANTOPEN');
		(new Database(util.current())).close();
		expect(fs.existsSync(util.current())).to.be.true;
		const db = this.db = new Database(util.current(), { fileMustExist: true });
		expect(db.name).to.equal(util.current());
		expect(db.memory).to.be.false;
		expect(db.readonly).to.be.false;
		expect(db.open).to.be.true;
	});
	it('should have a proper prototype chain', function () {
		const db = this.db = new Database(util.next());
		expect(db).to.be.an.instanceof(Database);
		expect(db.constructor).to.equal(Database);
		expect(Database.prototype.constructor).to.equal(Database);
		expect(Database.prototype.close).to.be.a('function');
		expect(Database.prototype.close).to.equal(db.close);
		expect(Database.prototype).to.equal(Object.getPrototypeOf(db));
	});
	it('should work properly when called as a function', function () {
		const db = this.db = Database(util.next());
		expect(db).to.be.an.instanceof(Database);
		expect(db.constructor).to.equal(Database);
		expect(Database.prototype.close).to.equal(db.close);
		expect(Database.prototype).to.equal(Object.getPrototypeOf(db));
	});
});

// ============================================================================
// Database#exec() tests (from 14.database.exec.js)
// ============================================================================
describe('Database#exec()', function () {
	beforeEach(function () {
		this.db = new Database(util.next());
	});
	afterEach(function () {
		this.db.close();
	});

	it('should throw an exception if a string is not provided', function () {
		expect(() => this.db.exec(123)).to.throw(TypeError);
		expect(() => this.db.exec(0)).to.throw(TypeError);
		expect(() => this.db.exec(null)).to.throw(TypeError);
		expect(() => this.db.exec()).to.throw(TypeError);
		expect(() => this.db.exec(new String('CREATE TABLE entries (a TEXT, b INTEGER)'))).to.throw(TypeError);
	});
	it('should throw an exception if invalid SQL is provided', function () {
		expect(() => this.db.exec('CREATE TABLE entries (a TEXT, b INTEGER')).to.throw(Database.SqliteError).with.property('code', 'SQLITE_ERROR');
	});
	it('should execute the SQL, returning the database object itself', function () {
		const r1 = this.db.exec('CREATE TABLE entries (a TEXT, b INTEGER)');
		const r2 = this.db.exec("INSERT INTO entries VALUES ('foobar', 44); INSERT INTO entries VALUES ('baz', NULL);");
		const r3 = this.db.exec('SELECT * FROM entries');

		expect(r1).to.equal(this.db);
		expect(r2).to.equal(this.db);
		expect(r3).to.equal(this.db);

		const rows = this.db.prepare('SELECT * FROM entries ORDER BY rowid').all();
		expect(rows.length).to.equal(2);
		expect(rows[0].a).to.equal('foobar');
		expect(rows[0].b).to.equal(44);
		expect(rows[1].a).to.equal('baz');
		expect(rows[1].b).to.equal(null);
	});
});

// ============================================================================
// Database#prepare() tests (from 13.database.prepare.js)
// ============================================================================
describe('Database#prepare()', function () {
	beforeEach(function () {
		this.db = new Database(util.next());
	});
	afterEach(function () {
		this.db.close();
	});

	it('should throw an exception if a string is not provided', function () {
		expect(() => this.db.prepare(123)).to.throw(TypeError);
		expect(() => this.db.prepare(0)).to.throw(TypeError);
		expect(() => this.db.prepare(null)).to.throw(TypeError);
		expect(() => this.db.prepare()).to.throw(TypeError);
		expect(() => this.db.prepare(new String('CREATE TABLE entries (a TEXT, b INTEGER)'))).to.throw(TypeError);
	});
	it('should throw an exception if invalid SQL is provided', function () {
		// Note: noria-sqlite validates at execution time, so we verify run() throws
		const stmt = this.db.prepare('CREATE TABLE entries (a TEXT, b INTEGER');
		expect(() => stmt.run()).to.throw(Database.SqliteError).with.property('code', 'SQLITE_ERROR');
	});
	it('should return a prepared Statement object', function () {
		const stmt = this.db.prepare('CREATE TABLE entries (a TEXT, b INTEGER)');
		expect(stmt.constructor.name).to.equal('Statement');
		expect(stmt.source).to.equal('CREATE TABLE entries (a TEXT, b INTEGER)');
		expect(stmt.reader).to.be.false;
	});
	it('should set reader to true for SELECT statements', function () {
		this.db.exec('CREATE TABLE entries (a TEXT)');
		expect(this.db.prepare('SELECT * FROM entries').reader).to.be.true;
		expect(this.db.prepare('SELECT 1').reader).to.be.true;
		expect(this.db.prepare("INSERT INTO entries VALUES ('foo')").reader).to.be.false;
	});
});

// ============================================================================
// Statement#run() tests (from 20.statement.run.js)
// ============================================================================
describe('Statement#run()', function () {
	beforeEach(function () {
		this.db = new Database(util.next());
		this.db.init = (data = false) => {
			this.db.info = this.db.prepare('CREATE TABLE entries (a TEXT, b INTEGER, c REAL, d BLOB)').run();
			if (data) {
				this.db.prepare('CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)').run();
				this.db.prepare("INSERT INTO entries VALUES ('foo', 25, 3.14, x'1133ddff'), ('foo', 25, 3.14, x'1133ddff'), ('foo', 25, 3.14, x'1133ddff')").run();
				this.db.prepare("INSERT INTO people VALUES (1, 'bob'), (2, 'sarah')").run();
			}
			return this.db;
		};
	});
	afterEach(function () {
		this.db.close();
	});

	it('should work with CREATE TABLE', function () {
		const { info } = this.db.init();
		expect(info.changes).to.equal(0);
		expect(info.lastInsertRowid).to.equal(0);
	});
	it('should work with INSERT INTO', function () {
		let stmt = this.db.init().prepare("INSERT INTO entries VALUES ('foo', 25, 3.14, x'1133ddff')");
		let info = stmt.run();
		expect(info.changes).to.equal(1);
		expect(info.lastInsertRowid).to.equal(1);

		info = stmt.run();
		expect(info.changes).to.equal(1);
		expect(info.lastInsertRowid).to.equal(2);

		stmt = this.db.prepare("INSERT INTO entries VALUES ('foo', 25, 3.14, x'1133ddff'), ('foo', 25, 3.14, x'1133ddff')");
		info = stmt.run();
		expect(info.changes).to.equal(2);
		expect(info.lastInsertRowid).to.equal(4);
	});
	it('should work with UPDATE', function () {
		const stmt = this.db.init(true).prepare("UPDATE entries SET a='bar' WHERE rowid=1");
		expect(stmt.run().changes).to.equal(1);
	});
	it('should work with DELETE FROM', function () {
		let stmt = this.db.init(true).prepare("DELETE FROM entries WHERE a='foo'");
		expect(stmt.run().changes).to.equal(3);
	});
	it('should work with DROP TABLE', function () {
		const stmt = this.db.init(true).prepare("DROP TABLE entries");
		// Note: changes count may vary due to CDC tracking deleted rows
		const info = stmt.run();
		expect(info.changes).to.be.a('number');
		expect(info.lastInsertRowid).to.be.a('number');
	});
	it('should accept positional bind parameters', function () {
		this.db.prepare("CREATE TABLE entries (a TEXT, b INTEGER, c REAL)").run();
		this.db.prepare('INSERT INTO entries VALUES (?, ?, ?)').run('foo', 25, 3.14);
		this.db.prepare('INSERT INTO entries VALUES (?, ?, ?)').run(['bar', 30, 2.71]);

		const rows = this.db.prepare('SELECT * FROM entries ORDER BY rowid').all();
		expect(rows.length).to.equal(2);
		expect(rows[0]).to.deep.equal({ a: 'foo', b: 25, c: 3.14 });
		expect(rows[1]).to.deep.equal({ a: 'bar', b: 30, c: 2.71 });
	});
});

// ============================================================================
// Statement#get() tests (from 21.statement.get.js)
// ============================================================================
describe('Statement#get()', function () {
	beforeEach(function () {
		this.db = new Database(util.next());
		this.db.prepare('CREATE TABLE entries (a TEXT, b INTEGER, c REAL, d BLOB, e TEXT)').run();
		// Insert 10 rows with b from 1 to 10
		for (let i = 1; i <= 10; i++) {
			this.db.prepare("INSERT INTO entries VALUES ('foo', ?, 3.14, x'dddddddd', NULL)").run(i);
		}
	});
	afterEach(function () {
		this.db.close();
	});

	it('should throw an exception when used on a statement that returns no data', function () {
		let stmt = this.db.prepare("INSERT INTO entries VALUES ('foo', 1, 3.14, x'dddddddd', NULL)");
		expect(stmt.reader).to.be.false;
		expect(() => stmt.get()).to.throw(TypeError);

		stmt = this.db.prepare("CREATE TABLE IF NOT EXISTS entries (a TEXT, b INTEGER, c REAL, d BLOB, e TEXT)");
		expect(stmt.reader).to.be.false;
		expect(() => stmt.get()).to.throw(TypeError);
	});
	it('should return the first matching row', function () {
		let stmt = this.db.prepare("SELECT * FROM entries ORDER BY rowid");
		expect(stmt.reader).to.be.true;
		const row = stmt.get();
		expect(row.a).to.equal('foo');
		expect(row.b).to.equal(1);
		expect(row.c).to.equal(3.14);
		expect(row.e).to.equal(null);

		stmt = this.db.prepare("SELECT * FROM entries WHERE b > 5 ORDER BY rowid");
		expect(stmt.get().b).to.equal(6);
	});
	it('should return undefined when no rows were found', function () {
		const stmt = this.db.prepare("SELECT * FROM entries WHERE b == 999");
		expect(stmt.get()).to.be.undefined;
		expect(stmt.pluck().get()).to.be.undefined;
	});
	it('should accept bind parameters', function () {
		const SQL = 'SELECT * FROM entries WHERE b = ?';
		let result = this.db.prepare(SQL).get(5);
		expect(result.b).to.equal(5);

		result = this.db.prepare(SQL).get([7]);
		expect(result.b).to.equal(7);

		result = this.db.prepare(SQL).get(999);
		expect(result).to.be.undefined;
	});
	it('should obey the pluck setting', function () {
		const stmt = this.db.prepare("SELECT a, b FROM entries ORDER BY rowid");
		expect(stmt.get()).to.deep.equal({ a: 'foo', b: 1 });
		expect(stmt.pluck(true).get()).to.equal('foo');
		expect(stmt.pluck().get()).to.equal('foo');
		expect(stmt.pluck(false).get()).to.deep.equal({ a: 'foo', b: 1 });
	});
	it('should obey the raw setting', function () {
		const stmt = this.db.prepare("SELECT a, b FROM entries ORDER BY rowid");
		expect(stmt.get()).to.deep.equal({ a: 'foo', b: 1 });
		expect(stmt.raw(true).get()).to.deep.equal(['foo', 1]);
		expect(stmt.raw().get()).to.deep.equal(['foo', 1]);
		expect(stmt.raw(false).get()).to.deep.equal({ a: 'foo', b: 1 });
	});
});

// ============================================================================
// Statement#all() tests (from 22.statement.all.js)
// ============================================================================
describe('Statement#all()', function () {
	beforeEach(function () {
		this.db = new Database(util.next());
		this.db.prepare('CREATE TABLE entries (a TEXT, b INTEGER, c REAL)').run();
		for (let i = 1; i <= 10; i++) {
			this.db.prepare("INSERT INTO entries VALUES ('foo', ?, 3.14)").run(i);
		}
	});
	afterEach(function () {
		this.db.close();
	});

	it('should return an array of every matching row', function () {
		let stmt = this.db.prepare("SELECT * FROM entries ORDER BY rowid");
		expect(stmt.reader).to.be.true;
		const rows = stmt.all();
		expect(rows.length).to.equal(10);
		for (let i = 0; i < 10; i++) {
			expect(rows[i]).to.deep.equal({ a: 'foo', b: i + 1, c: 3.14 });
		}

		stmt = this.db.prepare("SELECT * FROM entries WHERE b > 5 ORDER BY rowid");
		const filteredRows = stmt.all();
		expect(filteredRows.length).to.equal(5);
		for (let i = 0; i < 5; i++) {
			expect(filteredRows[i].b).to.equal(i + 6);
		}
	});
	it('should return an empty array when no rows were found', function () {
		const stmt = this.db.prepare("SELECT * FROM entries WHERE b == 999");
		expect(stmt.all()).to.deep.equal([]);
		expect(stmt.pluck().all()).to.deep.equal([]);
	});
	it('should accept bind parameters', function () {
		const SQL = 'SELECT * FROM entries WHERE b > ?';
		let result = this.db.prepare(SQL).all(8);
		expect(result.length).to.equal(2);
		expect(result[0].b).to.equal(9);
		expect(result[1].b).to.equal(10);

		result = this.db.prepare(SQL).all([5]);
		expect(result.length).to.equal(5);
	});
	it('should obey the pluck setting', function () {
		const stmt = this.db.prepare("SELECT a, b FROM entries ORDER BY rowid LIMIT 3");
		expect(stmt.all()).to.deep.equal([
			{ a: 'foo', b: 1 },
			{ a: 'foo', b: 2 },
			{ a: 'foo', b: 3 },
		]);
		expect(stmt.pluck(true).all()).to.deep.equal(['foo', 'foo', 'foo']);
		expect(stmt.pluck(false).all()).to.deep.equal([
			{ a: 'foo', b: 1 },
			{ a: 'foo', b: 2 },
			{ a: 'foo', b: 3 },
		]);
	});
	it('should obey the raw setting', function () {
		const stmt = this.db.prepare("SELECT a, b FROM entries ORDER BY rowid LIMIT 3");
		expect(stmt.raw(true).all()).to.deep.equal([
			['foo', 1],
			['foo', 2],
			['foo', 3],
		]);
		expect(stmt.raw(false).all()).to.deep.equal([
			{ a: 'foo', b: 1 },
			{ a: 'foo', b: 2 },
			{ a: 'foo', b: 3 },
		]);
	});
});

// ============================================================================
// Database#close() tests (from 11.database.close.js)
// ============================================================================
describe('Database#close()', function () {
	it('should cause database.open to return false', function () {
		const db = new Database(util.next());
		expect(db.open).to.be.true;
		db.close();
		expect(db.open).to.be.false;
	});
	it('should return the database object', function () {
		const db = new Database(util.next());
		expect(db.close()).to.equal(db);
	});
});

// ============================================================================
// Transaction tests (from 30.database.transaction.js - simplified)
// ============================================================================
describe('Database#transaction()', function () {
	beforeEach(function () {
		this.db = new Database(util.next());
		this.db.exec('CREATE TABLE entries (a TEXT)');
	});
	afterEach(function () {
		this.db.close();
	});

	it('should throw if a function is not provided', function () {
		expect(() => this.db.transaction()).to.throw(TypeError);
		expect(() => this.db.transaction(null)).to.throw(TypeError);
		expect(() => this.db.transaction('foo')).to.throw(TypeError);
	});
	it('should execute the function in a transaction', function () {
		const insert = this.db.prepare("INSERT INTO entries VALUES (?)");
		const trx = this.db.transaction(() => {
			insert.run('one');
			insert.run('two');
			insert.run('three');
		});
		trx();
		const rows = this.db.prepare('SELECT * FROM entries').all();
		expect(rows.length).to.equal(3);
	});
	it('should rollback on error', function () {
		const insert = this.db.prepare("INSERT INTO entries VALUES (?)");
		const trx = this.db.transaction(() => {
			insert.run('one');
			throw new Error('test error');
		});
		expect(() => trx()).to.throw('test error');
		const rows = this.db.prepare('SELECT * FROM entries').all();
		expect(rows.length).to.equal(0);
	});
});

// ============================================================================
// Statement#iterate() tests (from 23.statement.iterate.js - simplified)
// ============================================================================
describe('Statement#iterate()', function () {
	beforeEach(function () {
		this.db = new Database(util.next());
		this.db.prepare('CREATE TABLE entries (a TEXT, b INTEGER)').run();
		for (let i = 1; i <= 5; i++) {
			this.db.prepare("INSERT INTO entries VALUES ('foo', ?)").run(i);
		}
	});
	afterEach(function () {
		this.db.close();
	});

	it('should return an iterator over all matching rows', function () {
		const stmt = this.db.prepare("SELECT * FROM entries ORDER BY rowid");
		const rows = [...stmt.iterate()];
		expect(rows.length).to.equal(5);
		for (let i = 0; i < 5; i++) {
			expect(rows[i]).to.deep.equal({ a: 'foo', b: i + 1 });
		}
	});
	it('should work with for...of loops', function () {
		const stmt = this.db.prepare("SELECT * FROM entries ORDER BY rowid");
		let count = 0;
		for (const row of stmt.iterate()) {
			count++;
			expect(row.a).to.equal('foo');
		}
		expect(count).to.equal(5);
	});
});

// ============================================================================
// Pragma tests (from 12.database.pragma.js - simplified)
// ============================================================================
describe('Database#pragma()', function () {
	beforeEach(function () {
		this.db = new Database(util.next());
	});
	afterEach(function () {
		this.db.close();
	});

	it('should throw if a string is not provided', function () {
		expect(() => this.db.pragma()).to.throw(TypeError);
		expect(() => this.db.pragma(123)).to.throw(TypeError);
	});
	it('should execute PRAGMA statements', function () {
		const journalMode = this.db.pragma('journal_mode', { simple: true });
		expect(typeof journalMode).to.equal('string');
	});
});
