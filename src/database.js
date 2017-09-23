const Pool = require('pg-pool');

import pgrx from 'pg-reactive';
import config from 'config';
import _ from 'lodash';
import log4js from 'log4js';
import { parse } from 'url';
import { Observable } from 'rxjs';
import { valueToString } from './utils/pg-util';

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
      identifier,
      title,
      description,
      issued,
      modified,
      landing_page,
      publisher,
      keyword,
      theme,
      raw,
      version,
      version_period,
      spatial,
      distribution
    ) VALUES
  `;

  let values = _.map(metadatas, (metadata) => {
    return `(
      ${valueToString(metadata.portalId)},
      ${valueToString(metadata.identifier)},
      ${valueToString(metadata.title || 'Untitled Dataset')},
      ${valueToString(metadata.description)},
      ${valueToString(metadata.issued)},
      ${valueToString(metadata.modified)},
      ${valueToString(metadata.landingPage)},
      ${valueToString(metadata.publisher || 'Unknown')},
      ${valueToString(cleanItems(metadata.keyword))}::text[],
      ${valueToString(cleanItems(metadata.theme))}::text[],
      ${valueToString(metadata.raw)},
      ${valueToString(metadata.version)},
      ${valueToString(metadata.versionPeriod)},
      ${valueToString(metadata.spatial)},
      ${valueToString(metadata.distribution)}::json[]
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
 * @returns {Observable<any>} a checklist keyed by portal, dataset title, and
 *                            metadata content hash
 */
export function getLatestCheckList(platform, portal) {
  let db = getDB();
  let sql = `
    SELECT
      p.name || ':' || title || ':' || identifier AS key,
      version
    FROM dataset AS ds
    LEFT JOIN dataset_portal_xref AS dpx ON dpx.dataset_id = d.id
    LEFT JOIN portal AS p ON p.id = dpx.portal_id
    WHERE p.platform_id = (
      SELECT id FROM platform WHERE name = $2::text LIMIT 1
    )
  `;
  let values = [platform];

  if (portal) {
    sql += ' AND p.name = $1::text';
    values.push(portal);
  }

  return db.query(sql, values)
    .reduce((collection, dataset) => {
      collection[dataset.key] = dataset.version;
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

function cleanItems(items) {
  return _.chain(items).filter().forEach((item) => item.trim()).uniq().value();
}
