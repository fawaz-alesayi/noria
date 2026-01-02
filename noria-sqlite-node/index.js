'use strict';
/**
 * noria-sqlite - better-sqlite3 compatible SQLite library with Noria acceleration
 *
 * This module provides a drop-in replacement for better-sqlite3 with automatic
 * incremental view maintenance powered by Noria's dataflow engine.
 */

const fs = require('fs');
const path = require('path');

// Load native binding from the generated file
const nativeBinding = require('./native.js');
const NativeDatabase = nativeBinding.Database;

// Symbol for storing native binding
const cppdb = Symbol('cppdb');

// SqliteError - compatible with better-sqlite3
const errorDescriptor = { value: 'SqliteError', writable: true, enumerable: false, configurable: true };

function SqliteError(message, code) {
	if (new.target !== SqliteError) {
		return new SqliteError(message, code);
	}
	if (typeof code !== 'string') {
		throw new TypeError('Expected second argument to be a string');
	}
	Error.call(this, message);
	errorDescriptor.value = '' + message;
	Object.defineProperty(this, 'message', errorDescriptor);
	if (Error.captureStackTrace) {
		Error.captureStackTrace(this, SqliteError);
	}
	this.code = code;
}
Object.setPrototypeOf(SqliteError, Error);
Object.setPrototypeOf(SqliteError.prototype, Error.prototype);
Object.defineProperty(SqliteError.prototype, 'name', { value: 'SqliteError', writable: true, enumerable: false, configurable: true });

// Helper to get boolean option
function getBooleanOption(options, name) {
	const value = options[name];
	if (value == null) return false;
	if (typeof value !== 'boolean') {
		throw new TypeError(`Expected the "${name}" option to be a boolean`);
	}
	return value;
}

// Database wrapper
function Database(filenameGiven, options) {
	if (new.target == null) {
		return new Database(filenameGiven, options);
	}

	// Handle Buffer input (serialize/deserialize feature)
	let buffer;
	if (Buffer.isBuffer(filenameGiven)) {
		buffer = filenameGiven;
		filenameGiven = ':memory:';
	}

	// Apply defaults
	if (filenameGiven == null) filenameGiven = '';
	if (options == null) options = {};

	// Validate arguments
	if (typeof filenameGiven !== 'string') {
		throw new TypeError('Expected first argument to be a string');
	}
	if (typeof options !== 'object') {
		throw new TypeError('Expected second argument to be an options object');
	}
	if ('readOnly' in options) {
		throw new TypeError('Misspelled option "readOnly" should be "readonly"');
	}
	if ('memory' in options) {
		throw new TypeError('Option "memory" was removed in v7.0.0 (use ":memory:" filename instead)');
	}

	// Interpret options
	const filename = filenameGiven.trim();
	const anonymous = filename === '' || filename === ':memory:';
	const readonly = getBooleanOption(options, 'readonly');
	const fileMustExist = getBooleanOption(options, 'fileMustExist');
	const timeout = 'timeout' in options ? options.timeout : 5000;
	const verbose = 'verbose' in options ? options.verbose : null;

	// Validate interpreted options
	if (readonly && anonymous && !buffer) {
		throw new TypeError('In-memory/temporary databases cannot be readonly');
	}
	if (!Number.isInteger(timeout) || timeout < 0) {
		throw new TypeError('Expected the "timeout" option to be a positive integer');
	}
	if (timeout > 0x7fffffff) {
		throw new RangeError('Option "timeout" cannot be greater than 2147483647');
	}
	if (verbose != null && typeof verbose !== 'function') {
		throw new TypeError('Expected the "verbose" option to be a function');
	}

	// Make sure the specified directory exists (for non-anonymous databases)
	if (!anonymous && !filename.startsWith('file:') && !fs.existsSync(path.dirname(filename))) {
		throw new TypeError('Cannot open database because the directory does not exist');
	}

	// Check if file must exist
	if (fileMustExist && !anonymous && !fs.existsSync(filename)) {
		const err = new SqliteError(`unable to open database file`, 'SQLITE_CANTOPEN');
		throw err;
	}

	// Create native database
	try {
		this[cppdb] = new NativeDatabase(filename, {
			readonly: readonly,
			fileMustExist: fileMustExist,
			timeout: timeout,
		});
	} catch (e) {
		// Convert to SqliteError if needed
		if (e.message && e.message.startsWith('SQLITE_')) {
			const match = e.message.match(/^(SQLITE_\w+):\s*(.*)/);
			if (match) {
				throw new SqliteError(match[2] || match[1], match[1]);
			}
		}
		throw e;
	}

	// Store buffer for serialize() if provided
	if (buffer) {
		// TODO: implement database deserialization
	}
}

// Define getters
Object.defineProperties(Database.prototype, {
	name: {
		get: function name() { return this[cppdb].name; },
		enumerable: true,
	},
	open: {
		get: function open() { return this[cppdb].open; },
		enumerable: true,
	},
	inTransaction: {
		get: function inTransaction() { return this[cppdb].inTransaction; },
		enumerable: true,
	},
	readonly: {
		get: function readonly() { return this[cppdb].readonly; },
		enumerable: true,
	},
	memory: {
		get: function memory() { return this[cppdb].memory; },
		enumerable: true,
	},
});

// Database methods
Database.prototype.prepare = function prepare(sql) {
	if (typeof sql !== 'string') {
		throw new TypeError('Expected first argument to be a string');
	}
	try {
		const nativeStmt = this[cppdb].prepare(sql);
		return new Statement(nativeStmt, this);
	} catch (e) {
		// Convert to SqliteError
		if (e.message && e.message.startsWith('SQLITE_')) {
			const match = e.message.match(/^(SQLITE_\w+):\s*(.*)/);
			if (match) {
				throw new SqliteError(match[2] || match[1], match[1]);
			}
		}
		throw e;
	}
};

Database.prototype.exec = function exec(sql) {
	if (typeof sql !== 'string') {
		throw new TypeError('Expected first argument to be a string');
	}
	try {
		this[cppdb]._exec(sql);
	} catch (e) {
		// Convert to SqliteError
		if (e.message && e.message.startsWith('SQLITE_')) {
			const match = e.message.match(/^(SQLITE_\w+):\s*(.*)/);
			if (match) {
				throw new SqliteError(match[2] || match[1], match[1]);
			}
		}
		throw e;
	}
	return this;
};

Database.prototype.close = function close() {
	this[cppdb].close();
	return this;
};

// Transaction helper (basic implementation)
Database.prototype.transaction = function transaction(fn) {
	if (typeof fn !== 'function') {
		throw new TypeError('Expected first argument to be a function');
	}

	const db = this;
	const controller = function (...args) {
		let before, after, undo;
		if (controller.deferred) {
			before = 'BEGIN DEFERRED';
		} else if (controller.immediate) {
			before = 'BEGIN IMMEDIATE';
		} else if (controller.exclusive) {
			before = 'BEGIN EXCLUSIVE';
		} else {
			before = 'BEGIN';
		}
		after = 'COMMIT';
		undo = 'ROLLBACK';

		db.exec(before);
		try {
			const result = fn.apply(this, args);
			db.exec(after);
			return result;
		} catch (e) {
			if (db.open) {
				db.exec(undo);
			}
			throw e;
		}
	};

	controller.deferred = controller;
	controller.immediate = Object.assign((...args) => controller.apply(controller.immediate, args), { immediate: true });
	controller.exclusive = Object.assign((...args) => controller.apply(controller.exclusive, args), { exclusive: true });
	controller.default = controller;

	return controller;
};

// Pragma helper
Database.prototype.pragma = function pragma(source, options) {
	if (typeof source !== 'string') {
		throw new TypeError('Expected first argument to be a string');
	}
	const simple = options && options.simple;
	const sql = `PRAGMA ${source}`;

	// Check if this is a getter or setter pragma
	if (source.includes('=') || source.includes('(')) {
		// Setter pragma - just execute it
		this.exec(sql);
		return;
	}

	// Getter pragma - return results
	try {
		const stmt = this.prepare(sql);
		const rows = stmt.all();
		if (simple) {
			if (rows.length === 0) return undefined;
			const row = rows[0];
			const keys = Object.keys(row);
			return keys.length === 1 ? row[keys[0]] : row;
		}
		return rows;
	} catch (e) {
		// Some pragmas may not return results
		this.exec(sql);
		return;
	}
};

// Load a SQLite extension
Database.prototype.loadExtension = function loadExtension(path, entryPoint) {
	if (typeof path !== 'string') {
		throw new TypeError('Expected first argument to be a string');
	}
	if (path.trim() === '') {
		throw new TypeError('Expected first argument to be a non-empty string');
	}
	try {
		this[cppdb]._loadExtension(path, entryPoint);
	} catch (e) {
		if (e.message && e.message.startsWith('SQLITE_')) {
			const match = e.message.match(/^(SQLITE_\w+):\s*(.*)/);
			if (match) {
				throw new SqliteError(match[2] || match[1], match[1]);
			}
		}
		throw e;
	}
	return this;
};

// Statement wrapper
function Statement(nativeStmt, db) {
	this[cppdb] = nativeStmt;
	this._db = db;
}

// Statement getters
Object.defineProperties(Statement.prototype, {
	reader: {
		get: function reader() { return this[cppdb].reader; },
		enumerable: true,
	},
	readonly: {
		get: function readonly() { return !this.reader; },
		enumerable: true,
	},
	source: {
		get: function source() { return this[cppdb].source; },
		enumerable: true,
	},
});

// Statement methods - all take variadic parameters
Statement.prototype.run = function run(...params) {
	try {
		return this[cppdb].run(params);
	} catch (e) {
		if (e.message && e.message.includes('already has bound parameters')) {
			throw new TypeError('This statement already has bound parameters');
		}
		if (e.message && e.message.startsWith('SQLITE_')) {
			const match = e.message.match(/^(SQLITE_\w+):\s*(.*)/);
			if (match) {
				throw new SqliteError(match[2] || match[1], match[1]);
			}
		}
		throw e;
	}
};

Statement.prototype.get = function get(...params) {
	// Throw TypeError if this is not a reader statement (better-sqlite3 compat)
	if (!this.reader) {
		throw new TypeError('This statement does not return data. Use run() instead');
	}
	try {
		const result = this[cppdb].get(params);
		return result === null ? undefined : result;
	} catch (e) {
		if (e.message && e.message.startsWith('SQLITE_')) {
			const match = e.message.match(/^(SQLITE_\w+):\s*(.*)/);
			if (match) {
				throw new SqliteError(match[2] || match[1], match[1]);
			}
		}
		throw e;
	}
};

Statement.prototype.all = function all(...params) {
	// Throw TypeError if this is not a reader statement (better-sqlite3 compat)
	if (!this.reader) {
		throw new TypeError('This statement does not return data. Use run() instead');
	}
	try {
		return this[cppdb].all(params);
	} catch (e) {
		if (e.message && e.message.startsWith('SQLITE_')) {
			const match = e.message.match(/^(SQLITE_\w+):\s*(.*)/);
			if (match) {
				throw new SqliteError(match[2] || match[1], match[1]);
			}
		}
		throw e;
	}
};

Statement.prototype.pluck = function pluck(enabled) {
	this[cppdb].pluck(enabled);
	return this;
};

Statement.prototype.expand = function expand(enabled) {
	this[cppdb].expand(enabled);
	return this;
};

Statement.prototype.raw = function raw(enabled) {
	this[cppdb].raw(enabled);
	return this;
};

// Convert params for native binding (handle Buffers specially)
function convertParams(params) {
	return params.map(p => {
		if (Buffer.isBuffer(p)) {
			return { type: 'Buffer', data: Array.from(p) };
		}
		return p;
	});
}

Statement.prototype.bind = function bind(...params) {
	try {
		this[cppdb].bind(convertParams(params));
	} catch (e) {
		// Convert to TypeError for better-sqlite3 compat
		if (e.message && e.message.includes('already has bound parameters')) {
			throw new TypeError('This statement already has bound parameters');
		}
		if (e.message && e.message.includes('can only be invoked once')) {
			throw new TypeError('The bind() method can only be invoked once per statement object');
		}
		throw e;
	}
	return this;
};

// Iterate support (basic)
Statement.prototype.iterate = function* iterate(...params) {
	const rows = this.all(...params);
	for (const row of rows) {
		yield row;
	}
};

// Columns info
Statement.prototype.columns = function columns() {
	if (!this.reader) {
		throw new TypeError('This statement does not return data. Use run() instead');
	}
	try {
		return this[cppdb].columns();
	} catch (e) {
		if (e.message && e.message.startsWith('SQLITE_')) {
			const match = e.message.match(/^(SQLITE_\w+):\s*(.*)/);
			if (match) {
				throw new SqliteError(match[2] || match[1], match[1]);
			}
		}
		throw e;
	}
};

// Attach SqliteError to Database
Database.SqliteError = SqliteError;

// Export
module.exports = Database;
module.exports.Database = Database;
module.exports.SqliteError = SqliteError;
module.exports.default = Database;
