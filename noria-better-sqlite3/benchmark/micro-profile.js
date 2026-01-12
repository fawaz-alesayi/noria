#!/usr/bin/env node
'use strict';

const Database = require('../.');

const db = new Database(':memory:');
db.exec(`
    CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
    CREATE TABLE votes (id INTEGER PRIMARY KEY, user_id INTEGER, story_id INTEGER);
    CREATE INDEX idx_votes ON votes(story_id);
`);

// Seed some data
for (let i = 1; i <= 1000; i++) {
    db.exec(`INSERT INTO users VALUES (${i}, 'user${i}')`);
}
for (let i = 1; i <= 10000; i++) {
    db.exec(`INSERT INTO votes VALUES (${i}, ${Math.floor(Math.random() * 1000) + 1}, ${Math.floor(Math.random() * 100) + 1})`);
}

const getUser = db.prepare('SELECT * FROM users WHERE id = ?');
const getVoteCount = db.prepare('SELECT story_id, COUNT(*) as cnt FROM votes WHERE story_id = ? GROUP BY story_id');
const addVote = db.prepare('INSERT INTO votes (user_id, story_id) VALUES (?, ?)');

// Warmup
for (let i = 1; i <= 100; i++) {
    getUser.get(i);
    getVoteCount.get(i);
}

const ITERATIONS = 100000;

// Benchmark 1: Pure reads (simple)
console.log('\n=== Simple read (no aggregate) ===');
let start = process.hrtime.bigint();
for (let i = 0; i < ITERATIONS; i++) {
    getUser.get((i % 100) + 1);
}
let elapsed = Number(process.hrtime.bigint() - start) / 1e6;
console.log(`Simple read: ${Math.round(ITERATIONS / (elapsed / 1000))} ops/sec`);

// Benchmark 2: Aggregate reads
console.log('\n=== Aggregate read (COUNT) ===');
start = process.hrtime.bigint();
for (let i = 0; i < ITERATIONS; i++) {
    getVoteCount.get((i % 100) + 1);
}
elapsed = Number(process.hrtime.bigint() - start) / 1e6;
console.log(`Aggregate read: ${Math.round(ITERATIONS / (elapsed / 1000))} ops/sec`);

// Benchmark 3: Writes (no aggregate views registered)
console.log('\n=== Write without aggregate view ===');
const db2 = new Database(':memory:');
db2.exec('CREATE TABLE votes2 (id INTEGER PRIMARY KEY, user_id INTEGER, story_id INTEGER)');
const addVote2 = db2.prepare('INSERT INTO votes2 (user_id, story_id) VALUES (?, ?)');
start = process.hrtime.bigint();
for (let i = 0; i < ITERATIONS; i++) {
    addVote2.run(i, i % 100);
}
elapsed = Number(process.hrtime.bigint() - start) / 1e6;
console.log(`Write (no agg view): ${Math.round(ITERATIONS / (elapsed / 1000))} ops/sec`);

// Benchmark 4: Writes WITH aggregate view
console.log('\n=== Write WITH aggregate view ===');
const db3 = new Database(':memory:');
db3.exec('CREATE TABLE votes3 (id INTEGER PRIMARY KEY, user_id INTEGER, story_id INTEGER)');
// Register the aggregate view first
const countView = db3.prepare('SELECT story_id, COUNT(*) FROM votes3 WHERE story_id = ? GROUP BY story_id');
countView.get(1); // Force view creation
const addVote3 = db3.prepare('INSERT INTO votes3 (user_id, story_id) VALUES (?, ?)');
start = process.hrtime.bigint();
for (let i = 0; i < ITERATIONS; i++) {
    addVote3.run(i, i % 100);
}
elapsed = Number(process.hrtime.bigint() - start) / 1e6;
console.log(`Write (with agg view): ${Math.round(ITERATIONS / (elapsed / 1000))} ops/sec`);

// Benchmark 5: Mixed (95/5) with aggregate
console.log('\n=== Mixed 95/5 with aggregate ===');
start = process.hrtime.bigint();
for (let i = 0; i < ITERATIONS; i++) {
    if (Math.random() < 0.95) {
        getVoteCount.get((i % 100) + 1);
    } else {
        addVote.run(i, i % 100);
    }
}
elapsed = Number(process.hrtime.bigint() - start) / 1e6;
console.log(`Mixed 95/5: ${Math.round(ITERATIONS / (elapsed / 1000))} ops/sec`);

console.log('\n=== Cache Stats ===');
console.log(db.cacheStats());
