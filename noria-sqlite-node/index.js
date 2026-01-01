/**
 * noria-sqlite - Transparent caching layer for SQLite using Noria's incremental dataflow engine
 *
 * This module provides a better-sqlite3-compatible API with automatic
 * incremental view maintenance powered by Noria's dataflow engine.
 */

const { existsSync, readFileSync } = require('fs');
const { join } = require('path');

const { platform, arch } = process;

let nativeBinding = null;
let localFileExisted = false;
let loadError = null;

function isMusl() {
  // For Node.js on musl-based systems (like Alpine)
  if (!process.report || typeof process.report.getReport !== 'function') {
    try {
      const lddPath = require('child_process')
        .execSync('which ldd')
        .toString()
        .trim();
      return readFileSync(lddPath, 'utf8').includes('musl');
    } catch {
      return true;
    }
  }
  const { glibcVersionRuntime } = process.report.getReport().header;
  return !glibcVersionRuntime;
}

switch (platform) {
  case 'darwin':
    switch (arch) {
      case 'x64':
        localFileExisted = existsSync(
          join(__dirname, 'noria-sqlite.darwin-x64.node')
        );
        try {
          if (localFileExisted) {
            nativeBinding = require('./noria-sqlite.darwin-x64.node');
          } else {
            nativeBinding = require('@noria-sqlite/darwin-x64');
          }
        } catch (e) {
          loadError = e;
        }
        break;
      case 'arm64':
        localFileExisted = existsSync(
          join(__dirname, 'noria-sqlite.darwin-arm64.node')
        );
        try {
          if (localFileExisted) {
            nativeBinding = require('./noria-sqlite.darwin-arm64.node');
          } else {
            nativeBinding = require('@noria-sqlite/darwin-arm64');
          }
        } catch (e) {
          loadError = e;
        }
        break;
      default:
        throw new Error(`Unsupported architecture on macOS: ${arch}`);
    }
    break;
  case 'linux':
    switch (arch) {
      case 'x64':
        if (isMusl()) {
          localFileExisted = existsSync(
            join(__dirname, 'noria-sqlite.linux-x64-musl.node')
          );
          try {
            if (localFileExisted) {
              nativeBinding = require('./noria-sqlite.linux-x64-musl.node');
            } else {
              nativeBinding = require('@noria-sqlite/linux-x64-musl');
            }
          } catch (e) {
            loadError = e;
          }
        } else {
          localFileExisted = existsSync(
            join(__dirname, 'noria-sqlite.linux-x64-gnu.node')
          );
          try {
            if (localFileExisted) {
              nativeBinding = require('./noria-sqlite.linux-x64-gnu.node');
            } else {
              nativeBinding = require('@noria-sqlite/linux-x64-gnu');
            }
          } catch (e) {
            loadError = e;
          }
        }
        break;
      case 'arm64':
        if (isMusl()) {
          localFileExisted = existsSync(
            join(__dirname, 'noria-sqlite.linux-arm64-musl.node')
          );
          try {
            if (localFileExisted) {
              nativeBinding = require('./noria-sqlite.linux-arm64-musl.node');
            } else {
              nativeBinding = require('@noria-sqlite/linux-arm64-musl');
            }
          } catch (e) {
            loadError = e;
          }
        } else {
          localFileExisted = existsSync(
            join(__dirname, 'noria-sqlite.linux-arm64-gnu.node')
          );
          try {
            if (localFileExisted) {
              nativeBinding = require('./noria-sqlite.linux-arm64-gnu.node');
            } else {
              nativeBinding = require('@noria-sqlite/linux-arm64-gnu');
            }
          } catch (e) {
            loadError = e;
          }
        }
        break;
      default:
        throw new Error(`Unsupported architecture on Linux: ${arch}`);
    }
    break;
  case 'win32':
    switch (arch) {
      case 'x64':
        localFileExisted = existsSync(
          join(__dirname, 'noria-sqlite.win32-x64-msvc.node')
        );
        try {
          if (localFileExisted) {
            nativeBinding = require('./noria-sqlite.win32-x64-msvc.node');
          } else {
            nativeBinding = require('@noria-sqlite/win32-x64-msvc');
          }
        } catch (e) {
          loadError = e;
        }
        break;
      default:
        throw new Error(`Unsupported architecture on Windows: ${arch}`);
    }
    break;
  default:
    throw new Error(`Unsupported OS: ${platform}, architecture: ${arch}`);
}

if (!nativeBinding) {
  if (loadError) {
    throw loadError;
  }
  throw new Error(`Failed to load native binding`);
}

const { Database, Statement, CacheStats, RunResult } = nativeBinding;

module.exports = Database;
module.exports.Database = Database;
module.exports.Statement = Statement;
module.exports.default = Database;
