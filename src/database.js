import pgp from 'pg-promise';
import Promise from 'bluebird';
import config from 'config';
import _ from 'lodash';

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

/**
 * Save an array of dataset metadata into the database.
 * @param  {Metadata[]}     metadatas an array of dataset metadata
 * @return {Promise<null>}            no return
 */
export function save(metadatas) {
  let db = getDB();

  let columnSet = new pg.helpers.ColumnSet([
    'portal_id',
    'portal_dataset_id',
    'name',
    'description',
    'created_time',
    'updated_time',
    'portal_link',
    'data_link',
    'publisher',
    'tags',
    'categories',
    'raw'], {
      table: 'dataset'
    });

  let values = _.map(metadatas, metadata => {
    return {
      portal_id: metadata.portalID,
      portal_dataset_id: metadata.portalDatasetID,
      name: metadata.name,
      description: metadata.description,
      created_time: metadata.createdTime,
      updated_time: metadata.updatedTime,
      portal_link: metadata.portalLink,
      data_link: metadata.dataLink,
      publisher: metadata.publisher,
      tags: metadata.tags,
      categories: metadata.categories,
      raw: metadata.raw
    };
  });

  var query = pg.helpers.insert(values, columnSet);

  return db.none(query);
}
