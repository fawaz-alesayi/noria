#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');

const NoriaDatabase = require('../.');
let OriginalDatabase;
try {
    OriginalDatabase = require(path.join(__dirname, '../../better-sqlite3'));
} catch (e) {
    try {
        OriginalDatabase = require('better-sqlite3');
    } catch (e2) {
        process.exit(1);
    }
}

const CONFIG = {
    numUsers: 1000,
    numStories: 5000,
    numVotes: 50000,
    numComments: 20000,
    hotStories: 500,
    iterations: 50000,
    benchRuns: 5,  // Number of runs per scenario for statistical significance
    dbPath: '/tmp/lobsters-bench.db',
};

function pickStoryId() {
    return Math.random() < 0.9
        ? Math.floor(Math.random() * CONFIG.hotStories) + 1
        : Math.floor(Math.random() * CONFIG.numStories) + 1;
}

const SCHEMA = `
    CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, username TEXT NOT NULL, karma INTEGER DEFAULT 0, created_at INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS stories (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, title TEXT NOT NULL, url TEXT, body TEXT, created_at INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS votes (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, story_id INTEGER NOT NULL, vote INTEGER NOT NULL DEFAULT 1, created_at INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS comments (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, story_id INTEGER NOT NULL, parent_id INTEGER, body TEXT NOT NULL, created_at INTEGER NOT NULL);
    CREATE INDEX IF NOT EXISTS idx_stories_user ON stories(user_id);
    CREATE INDEX IF NOT EXISTS idx_votes_story ON votes(story_id);
    CREATE INDEX IF NOT EXISTS idx_votes_user ON votes(user_id);
    CREATE INDEX IF NOT EXISTS idx_comments_story ON comments(story_id);
    CREATE INDEX IF NOT EXISTS idx_comments_user ON comments(user_id);
`;

function seedDatabase(db) {
    const now = Date.now();
    const insertUser = db.prepare('INSERT INTO users (id, username, karma, created_at) VALUES (?, ?, ?, ?)');
    for (let i = 1; i <= CONFIG.numUsers; i++) {
        insertUser.run(i, `user${i}`, Math.floor(Math.random() * 1000), now - Math.random() * 86400000 * 365);
    }
    const insertStory = db.prepare('INSERT INTO stories (id, user_id, title, url, body, created_at) VALUES (?, ?, ?, ?, ?, ?)');
    for (let i = 1; i <= CONFIG.numStories; i++) {
        insertStory.run(i, Math.floor(Math.random() * CONFIG.numUsers) + 1, `Story Title ${i}`, `https://example.com/${i}`, `Body of story ${i}`, now - Math.random() * 86400000 * 30);
    }
    const insertVote = db.prepare('INSERT INTO votes (user_id, story_id, vote, created_at) VALUES (?, ?, ?, ?)');
    const votedPairs = new Set();
    for (let i = 0; i < CONFIG.numVotes; i++) {
        const userId = Math.floor(Math.random() * CONFIG.numUsers) + 1;
        const storyId = Math.floor(Math.random() * CONFIG.numStories) + 1;
        const key = `${userId}-${storyId}`;
        if (!votedPairs.has(key)) {
            votedPairs.add(key);
            insertVote.run(userId, storyId, 1, now - Math.random() * 86400000 * 30);
        }
    }
    const insertComment = db.prepare('INSERT INTO comments (user_id, story_id, parent_id, body, created_at) VALUES (?, ?, ?, ?, ?)');
    for (let i = 1; i <= CONFIG.numComments; i++) {
        insertComment.run(Math.floor(Math.random() * CONFIG.numUsers) + 1, Math.floor(Math.random() * CONFIG.numStories) + 1, null, `Comment ${i}`, now - Math.random() * 86400000 * 30);
    }
}

function setupDatabase(DatabaseClass, dbPath) {
    if (fs.existsSync(dbPath)) {
        fs.unlinkSync(dbPath);
        if (fs.existsSync(dbPath + '-wal')) fs.unlinkSync(dbPath + '-wal');
        if (fs.existsSync(dbPath + '-shm')) fs.unlinkSync(dbPath + '-shm');
    }
    const db = new DatabaseClass(dbPath);
    db.pragma('journal_mode = WAL');
    db.pragma('synchronous = NORMAL');
    db.pragma('cache_size = -64000');
    db.exec(SCHEMA);
    db.transaction(() => seedDatabase(db))();
    return db;
}

function prepareStatements(db) {
    return {
        getStory: db.prepare('SELECT * FROM stories WHERE id = ?'),
        getStoryVotes: db.prepare('SELECT story_id, COUNT(*) as count FROM votes WHERE story_id = ? GROUP BY story_id'),
        getUser: db.prepare('SELECT * FROM users WHERE id = ?'),
        getStoryWithAuthor: db.prepare('SELECT s.id, s.title, s.url, s.created_at, u.username as author FROM stories s JOIN users u ON u.id = s.user_id WHERE s.id = ?'),
        addVote: db.prepare('INSERT OR IGNORE INTO votes (user_id, story_id, vote, created_at) VALUES (?, ?, 1, ?)'),
        addComment: db.prepare('INSERT INTO comments (user_id, story_id, body, created_at) VALUES (?, ?, ?, ?)'),
    };
}

function runWorkload(stmts, iterations, readRatio) {
    const now = Date.now();
    for (let i = 0; i < iterations; i++) {
        if (Math.random() < readRatio) {
            const op = Math.random();
            const storyId = pickStoryId();
            const userId = Math.floor(Math.random() * 100) + 1;
            if (op < 0.40) stmts.getStory.get(storyId);
            else if (op < 0.60) stmts.getStoryWithAuthor.get(storyId);
            else if (op < 0.80) stmts.getStoryVotes.get(storyId);
            else stmts.getUser.get(userId);
        } else {
            const userId = Math.floor(Math.random() * CONFIG.numUsers) + 1;
            const storyId = pickStoryId();
            if (Math.random() < 0.7) stmts.addVote.run(userId, storyId, now);
            else stmts.addComment.run(userId, storyId, `Comment at ${now}`, now);
        }
    }
}

function warmupCache(stmts) {
    for (let i = 1; i <= CONFIG.hotStories; i++) {
        stmts.getStory.get(i);
        stmts.getStoryWithAuthor.get(i);
        stmts.getStoryVotes.get(i);
    }
    for (let i = 1; i <= 100; i++) {
        stmts.getUser.get(i);
    }
}

function median(arr) {
    const sorted = [...arr].sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

function benchmarkMultiple(fn, runs) {
    const times = [];
    for (let i = 0; i < runs; i++) {
        const start = process.hrtime.bigint();
        fn();
        times.push(Number(process.hrtime.bigint() - start) / 1e6);
    }
    return median(times);
}

function runScenario(name, readRatio, origStmts, noriaStmts) {
    warmupCache(origStmts);
    warmupCache(noriaStmts);
    const origTime = benchmarkMultiple(() => runWorkload(origStmts, CONFIG.iterations, readRatio), CONFIG.benchRuns);
    const noriaTime = benchmarkMultiple(() => runWorkload(noriaStmts, CONFIG.iterations, readRatio), CONFIG.benchRuns);
    const origOps = Math.round(CONFIG.iterations / (origTime / 1000));
    const noriaOps = Math.round(CONFIG.iterations / (noriaTime / 1000));
    return { name, origOps, noriaOps, speedup: noriaOps / origOps };
}

async function main() {
    const origDbPath = CONFIG.dbPath.replace('.db', '-orig.db');
    const noriaDbPath = CONFIG.dbPath.replace('.db', '-noria.db');
    const origDb = setupDatabase(OriginalDatabase, origDbPath);
    const noriaDb = setupDatabase(NoriaDatabase, noriaDbPath);
    const origStmts = prepareStatements(origDb);
    const noriaStmts = prepareStatements(noriaDb);

    const results = [];

    // Hot path benchmark (100% cache hit on same key)
    origStmts.getStory.get(1);
    noriaStmts.getStory.get(1);
    const hotIterations = 100000;
    const origHotTime = benchmarkMultiple(() => { for (let i = 0; i < hotIterations; i++) origStmts.getStory.get(1); }, CONFIG.benchRuns);
    const noriaHotTime = benchmarkMultiple(() => { for (let i = 0; i < hotIterations; i++) noriaStmts.getStory.get(1); }, CONFIG.benchRuns);
    results.push({
        name: 'Single-key read',
        origOps: Math.round(hotIterations / (origHotTime / 1000)),
        noriaOps: Math.round(hotIterations / (noriaHotTime / 1000)),
        speedup: origHotTime / noriaHotTime
    });

    // Different read/write ratios
    results.push(runScenario('Read-only', 1.0, origStmts, noriaStmts));
    results.push(runScenario('Read 99/1', 0.99, origStmts, noriaStmts));
    results.push(runScenario('Read 95/5', 0.95, origStmts, noriaStmts));
    results.push(runScenario('Read 90/10', 0.90, origStmts, noriaStmts));

    // Output results
    process.stdout.write('\n');
    process.stdout.write('LOBSTERS BENCHMARK\n');
    process.stdout.write('==================\n');
    process.stdout.write('Simulates a link aggregator (HN/Lobsters) with stories, users, votes, comments.\n');
    process.stdout.write('Reads: story lookup, vote count (aggregate), user profile, story+author (join)\n');
    process.stdout.write('Writes: add vote, add comment (triggers aggregate view updates)\n\n');
    process.stdout.write(`${CONFIG.benchRuns} runs per scenario, median time reported\n\n`);
    process.stdout.write('Scenario             | better-sqlite3  | noria           | Speedup\n');
    process.stdout.write('---------------------|-----------------|-----------------|--------\n');
    for (const r of results) {
        const speedStr = r.speedup >= 1 ? `${r.speedup.toFixed(2)}x` : `${r.speedup.toFixed(2)}x`;
        process.stdout.write(`${r.name.padEnd(20)} | ${r.origOps.toLocaleString().padStart(15)} | ${r.noriaOps.toLocaleString().padStart(15)} | ${speedStr}\n`);
    }
    process.stdout.write('\n');

    origDb.close();
    noriaDb.close();
    fs.unlinkSync(origDbPath);
    fs.unlinkSync(noriaDbPath);
    ['-wal', '-shm'].forEach(ext => {
        if (fs.existsSync(origDbPath + ext)) fs.unlinkSync(origDbPath + ext);
        if (fs.existsSync(noriaDbPath + ext)) fs.unlinkSync(noriaDbPath + ext);
    });
}

main().catch(err => { process.stderr.write(err.stack + '\n'); process.exit(1); });
