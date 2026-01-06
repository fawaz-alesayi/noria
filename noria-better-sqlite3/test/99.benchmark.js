'use strict';
const NoriaDatabase = require('../.');
const path = require('path');

let OriginalDatabase;
try {
	OriginalDatabase = require(path.join(__dirname, '../../better-sqlite3'));
} catch (e) {
	try {
		OriginalDatabase = require('better-sqlite3');
	} catch (e2) {
		OriginalDatabase = null;
	}
}

describe('Performance Comparison: noria-better-sqlite3 vs better-sqlite3', function() {
	this.slow(30000);
	this.timeout(120000);

	const results = {
		original: {},
		noria: {}
	};

	function benchmark(fn, iterations) {
		for (let i = 0; i < Math.min(100, iterations / 10); i++) {
			fn();
		}

		const start = process.hrtime.bigint();
		for (let i = 0; i < iterations; i++) {
			fn();
		}
		const elapsed = Number(process.hrtime.bigint() - start) / 1e6;
		return Math.round(iterations / (elapsed / 1000));
	}

	function formatOps(ops) {
		return ops.toLocaleString().padStart(12);
	}

	function formatDiff(noria, original) {
		if (!original) return '    N/A';
		const diff = ((noria - original) / original) * 100;
		const sign = diff >= 0 ? '+' : '';
		const color = diff >= -5 ? '' : ' (!)';
		return (sign + diff.toFixed(1) + '%').padStart(7) + color;
	}

	before(function() {
		if (!OriginalDatabase) {
			console.log('');
			console.log('      NOTE: Original better-sqlite3 not found.');
			console.log('      Install it to enable comparison benchmarks.');
			console.log('');
		}
	});

	it('benchmark: reading rows individually', function() {
		const iterations = 50000;

		const noriaDb = new NoriaDatabase(':memory:');
		noriaDb.exec('CREATE TABLE test (id INTEGER PRIMARY KEY, a TEXT, b REAL, c INTEGER)');
		const noriaInsert = noriaDb.prepare('INSERT INTO test VALUES (?, ?, ?, ?)');
		for (let i = 1; i <= 1000; i++) {
			noriaInsert.run(i, 'text' + i, i * 1.5, i * 10);
		}
		const noriaSelect = noriaDb.prepare('SELECT * FROM test WHERE id = ?');
		results.noria.selectOne = benchmark(() => {
			noriaSelect.get((Math.random() * 1000 | 0) + 1);
		}, iterations);
		noriaDb.close();

		if (OriginalDatabase) {
			const origDb = new OriginalDatabase(':memory:');
			origDb.exec('CREATE TABLE test (id INTEGER PRIMARY KEY, a TEXT, b REAL, c INTEGER)');
			const origInsert = origDb.prepare('INSERT INTO test VALUES (?, ?, ?, ?)');
			for (let i = 1; i <= 1000; i++) {
				origInsert.run(i, 'text' + i, i * 1.5, i * 10);
			}
			const origSelect = origDb.prepare('SELECT * FROM test WHERE id = ?');
			results.original.selectOne = benchmark(() => {
				origSelect.get((Math.random() * 1000 | 0) + 1);
			}, iterations);
			origDb.close();
		}

		console.log('');
		console.log('      Reading rows individually (' + iterations.toLocaleString() + ' ops):');
		console.log('        better-sqlite3:       ' + formatOps(results.original.selectOne || 0) + ' ops/sec');
		console.log('        noria-better-sqlite3: ' + formatOps(results.noria.selectOne) + ' ops/sec  ' + formatDiff(results.noria.selectOne, results.original.selectOne));
	});

	it('benchmark: reading 100 rows into array', function() {
		const iterations = 5000;

		const noriaDb = new NoriaDatabase(':memory:');
		noriaDb.exec('CREATE TABLE test (id INTEGER PRIMARY KEY, a TEXT, b REAL, c INTEGER)');
		const noriaInsert = noriaDb.prepare('INSERT INTO test VALUES (?, ?, ?, ?)');
		for (let i = 1; i <= 10000; i++) {
			noriaInsert.run(i, 'text' + i, i * 1.5, i * 10);
		}
		const noriaSelect = noriaDb.prepare('SELECT * FROM test WHERE id >= ? AND id < ?');
		results.noria.selectAll = benchmark(() => {
			const start = (Math.random() * 9900 | 0) + 1;
			noriaSelect.all(start, start + 100);
		}, iterations);
		noriaDb.close();

		if (OriginalDatabase) {
			const origDb = new OriginalDatabase(':memory:');
			origDb.exec('CREATE TABLE test (id INTEGER PRIMARY KEY, a TEXT, b REAL, c INTEGER)');
			const origInsert = origDb.prepare('INSERT INTO test VALUES (?, ?, ?, ?)');
			for (let i = 1; i <= 10000; i++) {
				origInsert.run(i, 'text' + i, i * 1.5, i * 10);
			}
			const origSelect = origDb.prepare('SELECT * FROM test WHERE id >= ? AND id < ?');
			results.original.selectAll = benchmark(() => {
				const start = (Math.random() * 9900 | 0) + 1;
				origSelect.all(start, start + 100);
			}, iterations);
			origDb.close();
		}

		console.log('');
		console.log('      Reading 100 rows into array (' + iterations.toLocaleString() + ' ops):');
		console.log('        better-sqlite3:       ' + formatOps(results.original.selectAll || 0) + ' ops/sec');
		console.log('        noria-better-sqlite3: ' + formatOps(results.noria.selectAll) + ' ops/sec  ' + formatDiff(results.noria.selectAll, results.original.selectAll));
	});

	it('benchmark: inserting rows individually', function() {
		const iterations = 10000;

		const noriaDb = new NoriaDatabase(':memory:');
		noriaDb.exec('CREATE TABLE test (id INTEGER PRIMARY KEY, a TEXT, b REAL, c INTEGER)');
		const noriaInsert = noriaDb.prepare('INSERT INTO test VALUES (?, ?, ?, ?)');
		let noriaId = 1;
		results.noria.insert = benchmark(() => {
			noriaInsert.run(noriaId++, 'text', 1.5, 10);
		}, iterations);
		noriaDb.close();

		if (OriginalDatabase) {
			const origDb = new OriginalDatabase(':memory:');
			origDb.exec('CREATE TABLE test (id INTEGER PRIMARY KEY, a TEXT, b REAL, c INTEGER)');
			const origInsert = origDb.prepare('INSERT INTO test VALUES (?, ?, ?, ?)');
			let origId = 1;
			results.original.insert = benchmark(() => {
				origInsert.run(origId++, 'text', 1.5, 10);
			}, iterations);
			origDb.close();
		}

		console.log('');
		console.log('      Inserting rows individually (' + iterations.toLocaleString() + ' ops):');
		console.log('        better-sqlite3:       ' + formatOps(results.original.insert || 0) + ' ops/sec');
		console.log('        noria-better-sqlite3: ' + formatOps(results.noria.insert) + ' ops/sec  ' + formatDiff(results.noria.insert, results.original.insert));
	});

	it('benchmark: inserting 100 rows in transaction', function() {
		const iterations = 500;

		const noriaDb = new NoriaDatabase(':memory:');
		noriaDb.exec('CREATE TABLE test (id INTEGER PRIMARY KEY, a TEXT, b REAL, c INTEGER)');
		const noriaInsert = noriaDb.prepare('INSERT INTO test VALUES (?, ?, ?, ?)');
		let noriaId = 1;
		const noriaTxn = noriaDb.transaction(() => {
			for (let i = 0; i < 100; i++) {
				noriaInsert.run(noriaId++, 'text', 1.5, 10);
			}
		});

		results.noria.transaction = benchmark(() => {
			noriaTxn();
		}, iterations);

		noriaDb.close();

		if (OriginalDatabase) {
			const origDb = new OriginalDatabase(':memory:');
			origDb.exec('CREATE TABLE test (id INTEGER PRIMARY KEY, a TEXT, b REAL, c INTEGER)');
			const origInsert = origDb.prepare('INSERT INTO test VALUES (?, ?, ?, ?)');
			let origId = 1;
			const origTxn = origDb.transaction(() => {
				for (let i = 0; i < 100; i++) {
					origInsert.run(origId++, 'text', 1.5, 10);
				}
			});

			results.original.transaction = benchmark(() => {
				origTxn();
			}, iterations);

			origDb.close();
		}

		console.log('');
		console.log('      Inserting 100 rows in transaction (' + iterations.toLocaleString() + ' ops):');
		console.log('        better-sqlite3:       ' + formatOps(results.original.transaction || 0) + ' ops/sec');
		console.log('        noria-better-sqlite3: ' + formatOps(results.noria.transaction) + ' ops/sec  ' + formatDiff(results.noria.transaction, results.original.transaction));
	});

	it('benchmark: Noria cache benefits', function() {
		const iterations = 10000;

		const db = new NoriaDatabase(':memory:');
		db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)');

		const select = db.prepare('SELECT * FROM users WHERE id = ?');
		const insert = db.prepare('INSERT INTO users VALUES (?, ?, ?)');

		for (let i = 1; i <= 100; i++) {
			insert.run(i, 'User ' + i, i * 10);
		}

		const cacheHitOps = benchmark(() => {
			select.get(42);
		}, iterations);

		const stats = db.cacheStats();
		db.close();

		console.log('');
		console.log('      Noria cache performance:');
		console.log('        Cache hit throughput: ' + formatOps(cacheHitOps) + ' ops/sec');
		console.log('        Cache hit rate:       ' + ((stats.cacheHits / (stats.cacheHits + stats.cacheMisses)) * 100).toFixed(1) + '%');
	});

	it('benchmark: summary', function() {
		console.log('');
		console.log('      ┌─────────────────────────────────────────────────────────────────┐');
		console.log('      │                    PERFORMANCE SUMMARY                          │');
		console.log('      ├─────────────────────────────────────────────────────────────────┤');

		if (OriginalDatabase) {
			const tests = [
				['Read single row', 'selectOne'],
				['Read 100 rows', 'selectAll'],
				['Insert single row', 'insert'],
				['Insert 100 (txn)', 'transaction']
			];

			for (const [name, key] of tests) {
				const orig = results.original[key] || 0;
				const noria = results.noria[key] || 0;
				const diff = orig ? ((noria - orig) / orig) * 100 : 0;
				const status = diff >= -5 ? '✓' : '!';
				console.log('      │  ' + name.padEnd(18) + ' │ ' + formatOps(orig) + ' → ' + formatOps(noria) + ' │ ' + formatDiff(noria, orig) + ' ' + status + ' │');
			}
		} else {
			console.log('      │  (Original better-sqlite3 not available for comparison)      │');
		}

		console.log('      └─────────────────────────────────────────────────────────────────┘');
		console.log('');
		console.log('      Legend: ✓ = within acceptable range, ! = potential regression');
		console.log('');
	});
});
