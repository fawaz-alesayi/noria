'use strict';
// Simple profiling script for all() method

const Database = require('../');
const db = new Database(':memory:');

// Setup
db.exec(`
  CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, value REAL, data TEXT);
`);

const insert = db.prepare('INSERT INTO test (name, value, data) VALUES (?, ?, ?)');
for (let i = 0; i < 10000; i++) {
  insert.run(`name${i}`, i * 1.5, `data${i}`);
}

const stmt = db.prepare('SELECT id, name, value, data FROM test WHERE id >= ? LIMIT 100');

// Warmup
for (let i = 0; i < 1000; i++) {
  stmt.all(i % 9900 + 1);
}

console.log('Starting profiled run...');
const start = Date.now();
const iterations = 50000;

for (let i = 0; i < iterations; i++) {
  stmt.all(i % 9900 + 1);
}

const elapsed = Date.now() - start;
console.log(`Completed ${iterations} iterations in ${elapsed}ms`);
console.log(`${(iterations / elapsed * 1000).toFixed(0)} ops/sec`);

db.close();
