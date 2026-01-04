'use strict';

/*
	Every benchmark trial will be executed once for each SQLite driver listed
	below. Each driver has a function to open a new database connection on a
	given filename and a list of PRAGMA statements.
 */

module.exports = new Map([
	// Raw SQLite mode (no Noria acceleration) - for apples-to-apples comparison
	['noria-sqlite (raw)', async (filename, pragma) => {
		const db = require('../.')(filename, { noria: { accelerationDisabled: true } });
		for (const str of pragma) db.pragma(str);
		return db;
	}],
	// With Noria acceleration enabled - to see caching benefits
	['noria-sqlite (accel)', async (filename, pragma) => {
		const db = require('../.')(filename);
		for (const str of pragma) db.pragma(str);
		return db;
	}],
	['better-sqlite3', async (filename, pragma) => {
		const db = require('../../better-sqlite3')(filename);
		for (const str of pragma) db.pragma(str);
		return db;
	}],
	...!moduleExists('sqlite3') ? [] : [
		['node-sqlite3', async (filename, pragma) => {
			const driver = require('sqlite3').Database;
			const db = await (require('sqlite').open)({ filename, driver });
			for (const str of pragma) await db.run(`PRAGMA ${str}`);
			return db;
		}]
	],
	...!moduleExists('node:sqlite') ? [] : [
		['node:sqlite', async (filename, pragma) => {
			const db = new (require('node:sqlite').DatabaseSync)(filename);
			for (const str of pragma) db.exec(`PRAGMA ${str}`);
			return db;
		}]
	],
]);

function moduleExists(moduleName) {
	try {
		return !!(require.resolve(moduleName));
	} catch (_) {
		return false;
	}
};
