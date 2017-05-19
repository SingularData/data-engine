import pgrx from 'pg-reactive';
import config from 'config';
import _ from 'lodash';
import log4js from 'log4js';
import { Observable } from 'rxjs';
import { toCamelCase, valueToString } from './utils/pg-util';

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
    return Observable.empty();
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
      publisher,
      tags,
      categories,
      raw,
      version_number,
      version_period,
      region,
      data
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
      ${valueToString(metadata.publisher || 'Unknown')},
      ${valueToString(_.uniq(metadata.tags))}::text[],
      ${valueToString(_.uniq(metadata.categories))}::text[],
      ${valueToString(metadata.raw)},
      ${valueToString(metadata.versionNumber)},
      ${valueToString(metadata.versionPeriod)},
      ST_SetSRID(ST_Force2D(ST_GeomFromGeoJSON(${valueToString(metadata.region)})), 4326),
      ${valueToString(metadata.data)}::json[]
    )`;
  });

  return db.tx((t) => t.query(query + values.join(',')))
    .catch((error) => {
      logger.error('Unable to save data: ', error);
      return Observable.empty();
    });
}

/**
 * Get a check list for latest datasets in a platform.
 * @param   {String} platform platform name
 * @returns {Observable<any>} a checklist keyed by portal id and portal dataset id
 */
export function getLatestCheckList(platform) {
  let db = getDB();
  let sql = `
    SELECT DISTINCT ON (portal_id, portal_dataset_id)
      portal_id,
      portal_dataset_id,
      version_number,
      updated_time
    FROM dataset AS d
    LEFT JOIN portal AS p ON p.id = d.portal_id
    WHERE p.platform_id = (
      SELECT id FROM platform WHERE name = $1::text LIMIT 1
    )
    ORDER BY portal_id, portal_dataset_id, version_number DESC
  `;

  return db.query(sql, [platform])
    .map((row) => toCamelCase(row))
    .reduce((collection, dataset) => {
      collection[`${dataset.portalId}:${dataset.portalDatasetId}`] = {
        updated: dataset.updatedTime.getTime(),
        version: dataset.versionNumber
      };

      return collection;
    }, {});
}
