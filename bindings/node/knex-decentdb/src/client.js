'use strict';

const { positionBindings } = require('./positionBindings');

// This is intentionally minimal: a small custom Knex client that uses the
// DecentDB N-API addon. It should be treated as a scaffold.
//
// Knex does not provide a stable public API for custom dialects, so most
// third-party clients import from internal paths.
let Client;
try {
  // Knex v2 internal path (common pattern in ecosystem)
  Client = require('knex/lib/client');
} catch {
  Client = null;
}

const { Database } = require('decentdb-native');

class Client_DecentDB extends (Client ?? class {}) {
  constructor(config = {}) {
    super(config);
    this.dialect = 'decentdb';
    this.driverName = 'decentdb-native';
  }

  _driver() {
    // Knex calls this to get the underlying driver.
    return { Database };
  }

  // Knex calls this to open a new connection.
  async acquireRawConnection() {
    const conn = this.config && this.config.connection ? this.config.connection : {};
    const filename = conn.filename || conn.database || conn.path;
    if (!filename) {
      throw new Error('DecentDB connection requires { filename }');
    }
    const options = conn.options || null;
    return new Database({ path: filename, options });
  }

  async destroyRawConnection(connection) {
    if (connection && typeof connection.close === 'function') {
      connection.close();
    }
  }

  // Convert Knex-style `?` placeholders into `$N`.
  positionBindings(sql) {
    return positionBindings(sql);
  }

  // Run a single query.
  async _query(connection, obj) {
    const sql = this.positionBindings(obj.sql);
    const bindings = obj.bindings || [];

    const res = connection.exec(sql, bindings);

    // Knex expects obj.response to be set.
    // For SELECT: response is typically rows.
    // For DML: response may be rowCount / rowsAffected.
    obj.response = res.rows;
    obj.rowsAffected = res.rowsAffected;

    return obj;
  }

  // Minimal processing: return raw rows.
  processResponse(obj /*, runner */) {
    return obj.response;
  }
}

module.exports = {
  Client_DecentDB
};
