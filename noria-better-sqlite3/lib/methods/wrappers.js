'use strict';
const { cppdb } = require('../util');

// Lazy-loaded statement wrapper (only loaded if { fresh: true } is actually used)
let wrapStatement = null;

exports.prepare = function prepare(sql) {
	const stmt = this[cppdb].prepare(sql, this, false);
	// Don't wrap by default - add fresh support on-demand
	// This keeps the fast path zero-cost
	return stmt;
};

// Enable fresh reads support (call once to activate the feature)
exports.enableFreshReads = function enableFreshReads() {
	if (!wrapStatement) {
		wrapStatement = require('../statement').wrapStatement;
	}
	// Patch prepare to wrap statements
	const db = this;
	const originalPrepare = this[cppdb].prepare.bind(this[cppdb]);
	this.prepare = function prepare(sql) {
		const stmt = originalPrepare(sql, db, false);
		return wrapStatement(stmt, db);
	};
	return this;
};

exports.exec = function exec(sql) {
	this[cppdb].exec(sql);
	return this;
};

exports.close = function close() {
	this[cppdb].close();
	return this;
};

exports.loadExtension = function loadExtension(...args) {
	this[cppdb].loadExtension(...args);
	return this;
};

exports.defaultSafeIntegers = function defaultSafeIntegers(...args) {
	this[cppdb].defaultSafeIntegers(...args);
	return this;
};

exports.unsafeMode = function unsafeMode(...args) {
	this[cppdb].unsafeMode(...args);
	return this;
};

// Noria-specific methods
exports.cacheStats = function cacheStats() {
	return this[cppdb].cacheStats();
};

exports.flushCache = function flushCache() {
	this[cppdb].flushCache();
	return this;
};

exports.getters = {
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
};
