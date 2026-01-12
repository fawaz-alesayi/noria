/**
 * Minimal benchmark for perf profiling.
 * Run with: sudo perf record -g node benchmark/perf-target.js
 */
const Database = require('../');

const db = new Database(':memory:');
db.exec(`
    CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT);
    CREATE TABLE stories (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT);
    CREATE TABLE votes (id INTEGER PRIMARY KEY, story_id INTEGER, user_id INTEGER);
    CREATE INDEX idx_votes_story ON votes(story_id);
`);

// Setup data
const insertUser = db.prepare('INSERT INTO users VALUES (?, ?, ?)');
const insertStory = db.prepare('INSERT INTO stories VALUES (?, ?, ?)');
const insertVote = db.prepare('INSERT INTO votes VALUES (?, ?, ?)');

db.exec('BEGIN');
for (let i = 1; i <= 100; i++) {
    insertUser.run(i, `user${i}`, `user${i}@example.com`);
    insertStory.run(i, i, `Story ${i}`);
}
for (let i = 1; i <= 5000; i++) {
    insertVote.run(i, (i % 100) + 1, (i % 100) + 1);
}
db.exec('COMMIT');

// Prepare queries (creates Noria views)
const getUser = db.prepare('SELECT * FROM users WHERE id = ?');
const getStory = db.prepare('SELECT * FROM stories WHERE id = ?');
const getVoteCount = db.prepare('SELECT story_id, COUNT(*) as cnt FROM votes WHERE story_id = ? GROUP BY story_id');

// Warmup
for (let i = 0; i < 1000; i++) {
    getUser.get((i % 100) + 1);
    getStory.get((i % 100) + 1);
    getVoteCount.get((i % 100) + 1);
}

// Signal ready for profiling
console.log('Starting benchmark...');

const OPS = 200000;
const start = Date.now();

for (let i = 0; i < OPS; i++) {
    const id = (i % 100) + 1;
    getUser.get(id);
    getStory.get(id);
    getVoteCount.get(id);
}

const elapsed = Date.now() - start;
const opsPerSec = Math.round(OPS * 3 / (elapsed / 1000)); // 3 ops per iteration

console.log(`Completed ${OPS * 3} lookups in ${elapsed}ms`);
console.log(`Throughput: ${opsPerSec.toLocaleString()} ops/sec`);

db.close();
