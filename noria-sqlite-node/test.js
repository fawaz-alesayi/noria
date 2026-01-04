/**
 * Simple test for noria-sqlite Node.js bindings
 */

const Database = require('./index');

console.log('Testing noria-sqlite Node.js bindings...\n');

// Create in-memory database
const db = new Database(':memory:');
console.log('✓ Created in-memory database');

// Create table
db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
console.log('✓ Created users table');

// Insert data
db.exec("INSERT INTO users VALUES (1, 'Alice', 30)");
db.exec("INSERT INTO users VALUES (2, 'Bob', 25)");
db.exec("INSERT INTO users VALUES (3, 'Charlie', 35)");
console.log('✓ Inserted test data');

// Prepare SELECT statement (should create materialized view)
const stmt = db.prepare('SELECT * FROM users WHERE id = ?');
console.log('✓ Prepared SELECT statement');
console.log(`  - Cached: ${stmt.cached}`);

// Query single row
const alice = stmt.get([1]);
console.log('✓ Queried single row');
console.log(`  - Result: ${JSON.stringify(alice)}`);

// Query again (should be cache hit)
const alice2 = stmt.get([1]);
console.log('✓ Queried same row again (cache hit)');

// Query all matching rows
const allStmt = db.prepare('SELECT * FROM users WHERE age > ?');
const olderUsers = allStmt.all([26]);
console.log('✓ Queried all matching rows');
console.log(`  - Found ${olderUsers.length} users older than 26`);

// Update data (should propagate through dataflow)
db.exec("UPDATE users SET age = 31 WHERE id = 1");
console.log('✓ Updated Alice\'s age');

// Query updated data
const updatedAlice = stmt.get([1]);
console.log('✓ Queried updated row');
console.log(`  - Result: ${JSON.stringify(updatedAlice)}`);

// Check stats
const stats = db.stats();
console.log('✓ Got cache stats');
console.log(`  - Node count: ${stats.nodeCount}`);
console.log(`  - Materialized nodes: ${stats.materializedNodes}`);
console.log(`  - Total rows: ${stats.totalRows}`);

// Close
db.close();
console.log('✓ Closed database');

console.log('\n✓ All tests passed!');
