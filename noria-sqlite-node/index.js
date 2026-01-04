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

// Get cache statistics from the Noria dataflow engine
Database.prototype.cacheStats = function cacheStats() {
	return this[cppdb].cacheStats();
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

	// Pragmas return results unless they have an '=' (setter)
	// Pragmas with parentheses like table_info(name) are getters and return results
	const returnsResults = !source.includes('=');

	if (!returnsResults) {
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

// Toggle unsafe mode - allows operations during iteration
Database.prototype.unsafeMode = function unsafeMode(enabled) {
	if (enabled !== undefined && typeof enabled !== 'boolean') {
		throw new TypeError('Expected argument to be a boolean');
	}
	return this[cppdb]._unsafeMode(enabled);
};

// Toggle default safe integers mode - new statements will return integers as BigInt
Database.prototype.defaultSafeIntegers = function defaultSafeIntegers(enabled) {
	if (enabled !== undefined && typeof enabled !== 'boolean') {
		throw new TypeError('Expected argument to be a boolean');
	}
	return this[cppdb]._defaultSafeIntegers(enabled);
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

// Register a user-defined SQL function
Database.prototype.function = function defineFunction(name, options, fn) {
	// Apply defaults
	if (options == null) options = {};
	if (typeof options === 'function') { fn = options; options = {}; }

	// Validate arguments
	if (typeof name !== 'string') throw new TypeError('Expected first argument to be a string');
	if (typeof fn !== 'function') throw new TypeError('Expected last argument to be a function');
	if (typeof options !== 'object') throw new TypeError('Expected second argument to be an options object');
	if (!name) throw new TypeError('User-defined function name cannot be an empty string');

	// Interpret options
	const safeIntegers = 'safeIntegers' in options ? (getBooleanOption(options, 'safeIntegers') ? 1 : 0) : 2;
	const deterministic = getBooleanOption(options, 'deterministic');
	const directOnly = getBooleanOption(options, 'directOnly');
	const varargs = getBooleanOption(options, 'varargs');
	let argCount = -1;

	// Determine argument count
	if (!varargs) {
		argCount = fn.length;
		if (!Number.isInteger(argCount) || argCount < 0) throw new TypeError('Expected function.length to be a positive integer');
		if (argCount > 100) throw new RangeError('User-defined functions cannot have more than 100 arguments');
	}

	// Create wrapper function that handles BigInt conversion
	const db = this;
	const wrapperFn = function(...args) {
		// Convert BigInt markers to actual BigInt
		const convertedArgs = args.map(convertBigInts);
		const result = fn.apply(this, convertedArgs);
		// Convert BigInt results back to marker objects for transport
		if (typeof result === 'bigint') {
			return { $bigint: result.toString() };
		}
		if (Buffer.isBuffer(result)) {
			return { type: 'Buffer', data: Array.from(result) };
		}
		return result;
	};

	try {
		this[cppdb]._registerFunction(wrapperFn, name, argCount, safeIntegers, deterministic, directOnly);
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

// Register a user-defined aggregate function
Database.prototype.aggregate = function defineAggregate(name, options) {
	// Validate arguments
	if (typeof name !== 'string') throw new TypeError('Expected first argument to be a string');
	if (typeof options !== 'object' || options === null) throw new TypeError('Expected second argument to be an options object');
	if (!name) throw new TypeError('User-defined function name cannot be an empty string');

	// Interpret options
	const start = 'start' in options ? options.start : null;
	const step = getFunctionOption(options, 'step', true);
	const inverse = getFunctionOption(options, 'inverse', false);
	const result = getFunctionOption(options, 'result', false);
	const safeIntegers = 'safeIntegers' in options ? (getBooleanOption(options, 'safeIntegers') ? 1 : 0) : 2;
	const deterministic = getBooleanOption(options, 'deterministic');
	const directOnly = getBooleanOption(options, 'directOnly');
	const varargs = getBooleanOption(options, 'varargs');
	let argCount = -1;

	// Determine argument count
	if (!varargs) {
		argCount = Math.max(getLength(step), inverse ? getLength(inverse) : 0);
		if (argCount > 0) argCount -= 1;
		if (argCount > 100) throw new RangeError('User-defined functions cannot have more than 100 arguments');
	}

	// Wrap step function to handle BigInt conversion
	const wrapStep = function(acc, ...args) {
		const convertedArgs = args.map(convertBigInts);
		const newAcc = step.call(this, acc, ...convertedArgs);
		return newAcc !== undefined ? newAcc : acc;
	};

	// Wrap inverse function if provided
	const wrapInverse = inverse ? function(acc, ...args) {
		const convertedArgs = args.map(convertBigInts);
		const newAcc = inverse.call(this, acc, ...convertedArgs);
		return newAcc !== undefined ? newAcc : acc;
	} : null;

	// Wrap result function if provided
	const wrapResult = result ? function(acc) {
		const res = result.call(this, acc);
		if (typeof res === 'bigint') {
			return { $bigint: res.toString() };
		}
		return res;
	} : null;

	try {
		this[cppdb]._registerAggregate(
			start,
			wrapStep,
			wrapInverse,
			wrapResult,
			name,
			argCount,
			safeIntegers,
			deterministic,
			directOnly
		);
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

// Register a virtual table
Database.prototype.table = function defineTable(name, definition) {
	// Validate name
	if (typeof name !== 'string') {
		throw new TypeError('Expected first argument to be a string');
	}
	if (!name) {
		throw new TypeError('Virtual table name cannot be an empty string');
	}

	// Validate definition
	if (definition == null || typeof definition !== 'object') {
		throw new TypeError('Expected second argument to be an options object');
	}

	// Validate columns
	if (!Array.isArray(definition.columns)) {
		throw new TypeError('Expected the "columns" option to be an array');
	}
	if (definition.columns.length === 0) {
		throw new TypeError('Virtual tables must have at least one column');
	}

	// Validate rows - must be a generator function
	const rows = definition.rows;
	if (typeof rows !== 'function') {
		throw new TypeError('Expected the "rows" option to be a generator function');
	}
	// Check if it's a generator function by checking for the GeneratorFunction constructor
	const GeneratorFunction = Object.getPrototypeOf(function* () {}).constructor;
	if (!(rows instanceof GeneratorFunction)) {
		throw new TypeError('Expected the "rows" option to be a generator function');
	}

	// Get optional parameters
	const parameters = definition.parameters || [];
	if (!Array.isArray(parameters)) {
		throw new TypeError('Expected the "parameters" option to be an array');
	}

	// Virtual tables require complex lifetime management in rusqlite
	// that is difficult to implement with JS callbacks. For now, throw an error.
	throw new Error('Virtual tables are not yet fully implemented. This feature requires complex native code integration with JavaScript generators.');

	// TODO: Implement virtual table registration
	// try {
	// 	this[cppdb]._registerVirtualTable(name, definition.columns, parameters, rows);
	// } catch (e) {
	// 	if (e.message && e.message.startsWith('SQLITE_')) {
	// 		const match = e.message.match(/^(SQLITE_\w+):\s*(.*)/);
	// 		if (match) {
	// 			throw new SqliteError(match[2] || match[1], match[1]);
	// 		}
	// 	}
	// 	throw e;
	// }
	// return this;
};

// Helper function to get a function option
function getFunctionOption(options, key, required) {
	const value = key in options ? options[key] : null;
	if (typeof value === 'function') return value;
	if (value != null) throw new TypeError(`Expected the "${key}" option to be a function`);
	if (required) throw new TypeError(`Missing required option "${key}"`);
	return null;
}

// Helper function to get function length
function getLength({ length }) {
	if (Number.isInteger(length) && length >= 0) return length;
	throw new TypeError('Expected function.length to be a positive integer');
}

// Serialize the database to a Buffer
Database.prototype.serialize = function serialize(options) {
	if (options == null) options = {};

	// Validate arguments
	if (typeof options !== 'object') throw new TypeError('Expected first argument to be an options object');

	// Interpret and validate options
	const attachedName = 'attached' in options ? options.attached : 'main';
	if (typeof attachedName !== 'string') throw new TypeError('Expected the "attached" option to be a string');
	if (!attachedName) throw new TypeError('The "attached" option cannot be an empty string');

	try {
		return this[cppdb]._serialize(attachedName);
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

// Backup the database to a file
Database.prototype.backup = async function backup(filename, options) {
	if (options == null) options = {};

	// Validate arguments
	if (typeof filename !== 'string') throw new TypeError('Expected first argument to be a string');
	if (typeof options !== 'object') throw new TypeError('Expected second argument to be an options object');

	// Interpret options
	filename = filename.trim();
	const attachedName = 'attached' in options ? options.attached : 'main';
	const handler = 'progress' in options ? options.progress : null;

	// Validate interpreted options
	if (!filename) throw new TypeError('Backup filename cannot be an empty string');
	if (filename === ':memory:') throw new TypeError('Invalid backup filename ":memory:"');
	if (typeof attachedName !== 'string') throw new TypeError('Expected the "attached" option to be a string');
	if (!attachedName) throw new TypeError('The "attached" option cannot be an empty string');
	if (handler != null && typeof handler !== 'function') throw new TypeError('Expected the "progress" option to be a function');

	// Make sure the specified directory exists
	const dirname = path.dirname(filename);
	if (dirname && dirname !== '.' && !fs.existsSync(dirname)) {
		throw new TypeError('Cannot save backup because the directory does not exist');
	}

	try {
		const progress = this[cppdb]._backup(filename, attachedName);
		// Call progress handler if provided
		if (handler) {
			handler(progress);
		}
		return progress;
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
		// Fast path: pass raw params array directly (no JSON serialization)
		let rawParams = params;
		if (params.length === 1 && Array.isArray(params[0]) && !Buffer.isBuffer(params[0])) {
			rawParams = params[0];
		}
		return this[cppdb]._runFast(rawParams);
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
		// Check if expand mode is enabled - need to use slow path for expand
		// because the fast path doesn't support nested objects
		if (this._expandMode) {
			// Fall back to slow path for expand mode
			const result = this[cppdb].get(convertParams(params));
			if (result === null) return undefined;
			return convertBigInts(result);
		}
		// Use fast path (direct NAPI object creation) - bypass JSON serialization
		// The fast method creates JS objects/arrays/values directly
		const result = this[cppdb]._getFast(convertParams(params));
		// _getFast returns undefined for no rows, otherwise the row object
		return result;
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
		// Check if expand mode is enabled - need to use slow path for expand
		if (this._expandMode) {
			// Fall back to slow path for expand mode
			const results = this[cppdb].all(convertParams(params));
			return results.map(convertBigInts);
		}

		// Handle bind([array]) case
		let rawParams = params;
		if (params.length === 1 && Array.isArray(params[0]) && !Buffer.isBuffer(params[0])) {
			rawParams = params[0];
		}

		// Check for pluck mode - use existing fast path
		if (this._pluckMode) {
			return this[cppdb]._allFast(rawParams);
		}

		// Check for raw mode - use existing fast path
		if (this._rawMode) {
			return this[cppdb]._allFast(rawParams);
		}

		// Ultra-fast path: get flat array from native, convert to objects in JS
		// V8's JIT is highly optimized for object creation
		const flat = this[cppdb]._allRaw(rawParams);

		// Cache column names on first call
		if (!this._colNames) {
			this._colNames = this[cppdb].columns().map(c => c.name);
			// Pre-create an object factory function for this shape
			// V8 optimizes objects with the same shape (hidden class)
			const names = this._colNames;
			const colCount = names.length;
			// Generate optimized factory code
			let factoryCode = 'return function(f,i){return{';
			for (let c = 0; c < colCount; c++) {
				if (c > 0) factoryCode += ',';
				factoryCode += JSON.stringify(names[c]) + ':f[i+' + c + ']';
			}
			factoryCode += '};}';
			this._rowFactory = new Function(factoryCode)();
		}

		const colCount = this._colNames.length;
		const len = flat.length;
		const rowCount = (len / colCount) | 0;
		const results = new Array(rowCount);
		const factory = this._rowFactory;

		for (let r = 0, i = 0; r < rowCount; r++, i += colCount) {
			results[r] = factory(flat, i);
		}

		return results;
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
	this._pluckMode = enabled !== false;
	return this;
};

Statement.prototype.expand = function expand(enabled) {
	this[cppdb].expand(enabled);
	this._expandMode = enabled !== false;
	return this;
};

Statement.prototype.raw = function raw(enabled) {
	this[cppdb].raw(enabled);
	this._rawMode = enabled !== false;
	return this;
};

Statement.prototype.safeIntegers = function safeIntegers(enabled) {
	this[cppdb]._safeIntegers(enabled);
	return this;
};

// Fast path check for primitive values (no conversion needed)
function isPrimitive(v) {
	const t = typeof v;
	return v === null || t === 'number' || t === 'string' || t === 'boolean' || v === undefined;
}

// Convert params for native binding (handle Buffers and BigInt specially)
function convertValue(p) {
	if (Buffer.isBuffer(p)) {
		return { type: 'Buffer', data: Array.from(p) };
	}
	if (typeof p === 'bigint') {
		// Convert BigInt to number if it fits safely
		if (p >= Number.MIN_SAFE_INTEGER && p <= Number.MAX_SAFE_INTEGER) {
			return Number(p);
		}
		// For large BigInts, use a special marker so Rust can parse as i64
		return { $bigint: p.toString() };
	}
	// Recursively convert object properties (for named params like { col: Buffer })
	if (p !== null && typeof p === 'object' && !Array.isArray(p)) {
		const result = {};
		for (const key of Object.keys(p)) {
			result[key] = convertValue(p[key]);
		}
		return result;
	}
	return p;
}

function convertParams(params) {
	// Handle bind([array]) case - unwrap single array argument
	if (params.length === 1 && Array.isArray(params[0]) && !Buffer.isBuffer(params[0])) {
		params = params[0];
	}
	// Fast path: if all params are primitives, return as-is (no .map allocation)
	let allPrimitive = true;
	for (let i = 0; i < params.length; i++) {
		if (!isPrimitive(params[i])) {
			allPrimitive = false;
			break;
		}
	}
	if (allPrimitive) return params;
	// Slow path: need to convert some values
	return params.map(convertValue);
}

// Convert BigInt markers in result back to actual BigInt
function convertBigInts(value) {
	if (value === null || value === undefined) {
		return value;
	}
	if (typeof value === 'object') {
		// Return actual Buffers as-is
		if (Buffer.isBuffer(value)) {
			return value;
		}
		if (value.$bigint !== undefined) {
			return BigInt(value.$bigint);
		}
		if (Array.isArray(value)) {
			return value.map(convertBigInts);
		}
		if (value.type === 'Buffer' && Array.isArray(value.data)) {
			return Buffer.from(value.data);
		}
		const result = {};
		for (const key of Object.keys(value)) {
			result[key] = convertBigInts(value[key]);
		}
		return result;
	}
	return value;
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
