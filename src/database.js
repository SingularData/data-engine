import pgp from 'pg-promise';
import Promise from 'bluebird';
import config from 'config';

let pg = pgp({ promiseLib: Promise });
let db = null;

/**
 * Get the current database connection.
 * @return {Object} pgp database connection.
 */
export function getDB() {
  return db ? db : initialize();
}

/**
 * Initialize database connection.
 * @return {Object} pgp database connection.
 */
export function initialize() {
  if (db) {
    pg.end();
  }

  db = pg(config.get('database.connStr'));

  return db;
}
