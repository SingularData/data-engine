import pgrx from 'pg-reactive';
import config from 'config';
import _ from 'lodash';
import log4js from 'log4js';
import { Observable } from 'rxjs';
import { toCamelCase, valueToString } from './utils/pg-util';

const logger = log4js.getLogger('database');

let db;

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
      uuid,
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
      ${valueToString(metadata.uuid)},
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

  return db.query(query + values.join(','))
    .catch((error) => {
      logger.error('Unable to save data: ', error);
      return Observable.empty();
    });
}

/**
 * Get a check list for latest datasets in a platform.
 * @param   {String} platform platform name
 * @param   {String} [portal] portal name (optional)
 * @returns {Observable<any>} a checklist keyed by portal id and portal dataset id
 */
export function getLatestCheckList(platform, portal) {
  let db = getDB();
  let sql, task;

  if (portal) {
    sql = `
      SELECT DISTINCT ON (uuid)
        portal_id,
        portal_dataset_id,
        uuid,
        version_number,
        raw_md5
      FROM dataset AS d
      LEFT JOIN portal AS p ON p.id = d.portal_id
      WHERE p.name = $1::text AND p.platform_id = (
        SELECT id FROM platform WHERE name = $2::text LIMIT 1
      )
      ORDER BY uuid, version_number DESC
    `;

    task = db.query(sql, [portal, platform]);
  } else {
    sql = `
      SELECT DISTINCT ON (uuid)
        portal_id,
        portal_dataset_id,
        uuid,
        version_number,
        raw_md5
      FROM dataset AS d
      LEFT JOIN portal AS p ON p.id = d.portal_id
      WHERE p.platform_id = (
        SELECT id FROM platform WHERE name = $1::text LIMIT 1
      )
      ORDER BY uuid, version_number DESC
    `;

    task = db.query(sql, [platform]);
  }

  return task
    .map((row) => toCamelCase(row))
    .reduce((collection, dataset) => {
      collection[`${dataset.portal_id}:${dataset.portal_dataset_id}`] = {
        uuid: dataset.uuid,
        md5: dataset.rawMd5,
        version: dataset.versionNumber
      };

      return collection;
    }, {});
}

/**
 * Refresh database views.
 * @returns {Observable} empty Observable
 */
export function refreshDatabase() {
  let db = getDB();
  let sql = 'REFRESH MATERIALIZED VIEW public.view_portal;';
  return db.query(sql);
}

/**
 * Clear all metadata data from the database
 * @returns {Observable} empty observable
 */
export function clear() {
  let db = getDB();
  let sql = `
    DELETE FROM dataset_tag_xref;
    DELETE FROM dataset_category_xref;
    DELETE FROM dataset_tag;
    DELETE FROM dataset_category;
    DELETE FROM dataset_data;
    DELETE FROM dataset;
    DELETE FROM dataset_region;
    DELETE FROM dataset_publisher;
  `;

  return db.query(sql);
}
