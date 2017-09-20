const Pool = require('pg-pool');

import pgrx from 'pg-reactive';
import config from 'config';
import _ from 'lodash';
import log4js from 'log4js';
import { parse } from 'url';
import { Observable } from 'rxjs';
import { toCamelCase, valueToString } from './utils/pg-util';

const logger = log4js.getLogger('database');

let db, dbType;

/**
 * Get the current database connection.
 * @param {string} type database object type: pg-reactive or pg
 * @return {Object} pgp database connection.
 */
export function getDB(type = 'pg-reactive') {
  if (dbType === type && db) {
    return db;
  }

  return initialize(type);
}

/**
 * Initialize database connection.
 * @param {string} type database object type: pg-reactive or pg
 * @return {Object} pgp database connection.
 */
export function initialize(type = 'pg-reactive') {
  if (db) {
    db.end();
  }

  if (type === 'pg-reactive') {
    db = new pgrx(config.get('database.url'));
  } else {
    let params = parse(config.get('database.url'));
    let auth = params.auth.split(':');
    let connConfig = {
      user: auth[0],
      password: auth[1],
      host: params.hostname,
      port: params.port,
      database: params.pathname.split('/')[1]
    };

    db = new Pool(connConfig);
  }

  dbType = type;

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
      created,
      updated,
      url,
      publisher,
      tags,
      categories,
      raw,
      version,
      version_period,
      spatial,
      files
    ) VALUES
  `;
  let values = _.map(metadatas, (metadata) => {
    return `(
      ${valueToString(metadata.portalId)},
      ${valueToString(metadata.portalDatasetId)},
      ${valueToString(metadata.uuid)},
      ${valueToString(metadata.name || 'Untitled Dataset')},
      ${valueToString(metadata.description)},
      ${valueToString(metadata.created)},
      ${valueToString(metadata.updated)},
      ${valueToString(metadata.url)},
      ${valueToString(metadata.publisher || 'Unknown')},
      ${valueToString(_.uniq(metadata.tags))}::text[],
      ${valueToString(_.uniq(metadata.categories))}::text[],
      ${valueToString(metadata.raw)},
      ${valueToString(metadata.version)},
      ${valueToString(metadata.versionPeriod)},
      ${valueToString(metadata.spatial)},
      ${valueToString(metadata.files)}::json[]
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
        version,
        raw_md5
      FROM dataset AS d
      LEFT JOIN portal AS p ON p.id = d.portal_id
      WHERE p.name = $1::text AND p.platform_id = (
        SELECT id FROM platform WHERE name = $2::text LIMIT 1
      )
      ORDER BY uuid, version DESC
    `;

    task = db.query(sql, [portal, platform]);
  } else {
    sql = `
      SELECT DISTINCT ON (uuid)
        portal_id,
        portal_dataset_id,
        uuid,
        version,
        raw_md5
      FROM dataset AS d
      LEFT JOIN portal AS p ON p.id = d.portal_id
      WHERE p.platform_id = (
        SELECT id FROM platform WHERE name = $1::text LIMIT 1
      )
      ORDER BY uuid, version DESC
    `;

    task = db.query(sql, [platform]);
  }

  return task
    .map((row) => toCamelCase(row))
    .reduce((collection, dataset) => {
      collection[`${dataset.portal_id}:${dataset.portal_dataset_id}`] = {
        uuid: dataset.uuid,
        md5: dataset.rawMd5,
        version: dataset.version
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

  return db.tx((t) => Observable.concat(
    t.query('REFRESH MATERIALIZED VIEW public.mview_latest_dataset'),
    t.query('REFRESH MATERIALIZED VIEW public.mview_portal')
  ));
}

/**
 * Clear all metadata data from the database
 * @returns {Observable} empty observable
 */
export function clear() {
  let db = getDB();

  return db.tx((t) => Observable.concat(
    t.query('DELETE FROM dataset_tag_xref'),
    t.query('DELETE FROM dataset_category_xref'),
    t.query('DELETE FROM dataset_tag'),
    t.query('DELETE FROM dataset_category'),
    t.query('DELETE FROM dataset_file'),
    t.query('DELETE FROM dataset'),
    t.query('DELETE FROM dataset_region'),
    t.query('DELETE FROM dataset_publisher')
  ));
}
