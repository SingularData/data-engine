import pgrx from 'pg-reactive';
import config from 'config';
import _ from 'lodash';
import Rx from 'rxjs';
import log4js from 'log4js';
import { valueToString } from './utils/pg-util';

const logger = log4js.getLogger('database');

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
    db.end();
  }

  db = new pgrx(config.get('database.url'));

  return db;
}

/**
 * Save an array of dataset metadata into the database.
 * @param  {Metadata[]}     metadatas an array of dataset metadata
 * @return {Promise<null>}            no return
 */
export function save(metadatas) {

  if (metadatas.length === 0) {
    return Rx.Observable.empty();
  }

  let db = getDB();
  let query = `
    INSERT INTO view_latest_dataset (
      portal_id,
      portal_dataset_id,
      name,
      description,
      created_time,
      updated_time,
      portal_link,
      data_link,
      publisher,
      tags,
      categories,
      raw
    ) VALUES
  `;
  let values = _.map(metadatas, (metadata) => {
    return `(
      ${valueToString(metadata.portalID)},
      ${valueToString(metadata.portalDatasetID)},
      ${valueToString(metadata.name || 'Untitled Dataset')},
      ${valueToString(metadata.description)},
      ${valueToString(metadata.createdTime)},
      ${valueToString(metadata.updatedTime)},
      ${valueToString(metadata.portalLink)},
      ${valueToString(metadata.dataLink)},
      ${valueToString(metadata.publisher || 'Unknown')},
      ${valueToString(_.uniq(metadata.tags))}::text[],
      ${valueToString(_.uniq(metadata.categories))}::text[],
      ${valueToString(metadata.raw)}
    )`;
  });

  return db.query(query + values.join(','))
    .catch((error) => {
      logger.error('Unable to save data: ', error);
      return Rx.Observable.empty();
    });
}
