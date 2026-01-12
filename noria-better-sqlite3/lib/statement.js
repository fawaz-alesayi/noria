'use strict';
const { cppdb } = require('./util');

// Fast check if value is a { fresh: true } options object
// Optimized for the common case where there's no options object
function isFreshOption(value) {
	return value !== null &&
		typeof value === 'object' &&
		value.fresh === true &&
		!Array.isArray(value) &&
		!Buffer.isBuffer(value);
}

// Wrap a native statement to add { fresh: true } support
function wrapStatement(nativeStmt, db) {
	const originalGet = nativeStmt.get.bind(nativeStmt);
	const originalAll = nativeStmt.all.bind(nativeStmt);
	const originalGetMany = nativeStmt.getMany.bind(nativeStmt);
	const flushFn = db[cppdb].flushCache.bind(db[cppdb]);

	nativeStmt.get = function get() {
		const len = arguments.length;
		if (len > 0 && isFreshOption(arguments[len - 1])) {
			flushFn();
			// Call with all args except the last one
			switch (len) {
				case 1: return originalGet();
				case 2: return originalGet(arguments[0]);
				case 3: return originalGet(arguments[0], arguments[1]);
				default:
					const args = new Array(len - 1);
					for (let i = 0; i < len - 1; i++) args[i] = arguments[i];
					return originalGet.apply(null, args);
			}
		}
		// Fast path: no options, pass through directly
		switch (len) {
			case 0: return originalGet();
			case 1: return originalGet(arguments[0]);
			case 2: return originalGet(arguments[0], arguments[1]);
			case 3: return originalGet(arguments[0], arguments[1], arguments[2]);
			default: return originalGet.apply(null, arguments);
		}
	};

	nativeStmt.all = function all() {
		const len = arguments.length;
		if (len > 0 && isFreshOption(arguments[len - 1])) {
			flushFn();
			switch (len) {
				case 1: return originalAll();
				case 2: return originalAll(arguments[0]);
				case 3: return originalAll(arguments[0], arguments[1]);
				default:
					const args = new Array(len - 1);
					for (let i = 0; i < len - 1; i++) args[i] = arguments[i];
					return originalAll.apply(null, args);
			}
		}
		switch (len) {
			case 0: return originalAll();
			case 1: return originalAll(arguments[0]);
			case 2: return originalAll(arguments[0], arguments[1]);
			case 3: return originalAll(arguments[0], arguments[1], arguments[2]);
			default: return originalAll.apply(null, arguments);
		}
	};

	// getMany: batch lookup for multiple keys in a single call
	// Usage: stmt.getMany([key1, key2, key3]) -> [row1, row2, row3]
	// With fresh option: stmt.getMany([key1, key2], { fresh: true })
	nativeStmt.getMany = function getMany(keys, options) {
		if (options && isFreshOption(options)) {
			flushFn();
		}
		return originalGetMany(keys);
	};

	return nativeStmt;
}

module.exports = { wrapStatement };
