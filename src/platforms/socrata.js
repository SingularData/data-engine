import _ from 'lodash';
import config from 'config';
import Rx from 'rxjs';
import log4js from 'log4js';
import { readFileSync } from 'fs';
import { RxHR } from "@akanass/rx-http-request";
import { getDB } from '../database';
import { toUTC } from '../utils/pg-util';
import { getOptions } from '../utils/request-util';

const cocurrency = config.get('cocurrency');
const limit = config.get('platforms.Socrata.limit');
const logger = log4js.getLogger('Socrata');

/**
 * Download all Socrata data.
 * @return {Rx.Observable}        harvest job
 */
export function downloadAll() {

  let sql = readFileSync(__dirname + '/../queries/get_platform_portals.sql', 'utf-8');

  return getDB()
    .query(sql, ['Socrata', 'Socrata-EU'])
    .map((portal) => {
      portal.region = portal.platform === 'Socrata' ? 'us' : 'eu';

      return portal;
    })
    .mergeMap((portal) => download(portal), cocurrency);
}

export function downloadAllUS() {

  let sql = readFileSync(__dirname + '/../queries/get_platform_portals.sql', 'utf-8');

  return getDB()
    .query(sql, ['Socrata'])
    .map((portal) => {
      portal.region = 'us';

      return portal;
    })
    .mergeMap((portal) => download(portal), cocurrency);
}

export function downloadAllEU() {

  let sql = readFileSync(__dirname + '/../queries/get_platform_portals', 'utf-8');

  return getDB()
    .query(sql, ['Socrata-EU'])
    .map((portal) => {
      portal.region = 'us';

      return portal;
    })
    .mergeMap((portal) => download(portal), cocurrency);
}

/**
 * Harvest a Socrata portal.
 * @param   {String}      name    portal name
 * @return  {Observable}          a stream of dataset metadata
 */
export function downloadPortal(name) {

  let sql = `
    SELECT
      p.id,
      p.name,
      p.url,
      p.description,
      pl.name AS platform,
      l.name AS location
    FROM portal AS p
    LEFT JOIN platform AS pl ON pl.id = p.platform_id
    LEFT JOIN location AS l ON l.id = p.location_id
    WHERE p.name = $1::text AND (
      pl.name = $2::text OR pl.name = $3::text
    )
    LIMIT 1
  `;

  return getDB()
    .query(sql, [name, 'Socrata', 'Socrata-EU'])
    .map((portal) => {
      portal.region = 'us';

      return portal;
    })
    .concatMap((portal) => download(portal));
}

/**
 * Harvest the given Socrata portal.
 * @param  {Portal}      portal    portal information
 * @return {Rx.Observable}         dataset stream
 */
export function download(portal) {
  return RxHR.get(`http://api.${portal.region}.socrata.com/api/catalog/v1?domains=${portal.url}&offset=0&limit=0`, getOptions())
    .concatMap((result) => {
      if (result.body.error) {
        throw new Error(result.body.error);
      }

      let totalCount = Math.ceil(result.body.resultSetSize / limit);

      return Rx.Observable.range(0, totalCount)
        .concatMap((i) => RxHR.get(`http://api.${portal.region}.socrata.com/api/catalog/v1?domains=${portal.url}&limit=${limit}&offset=${i * limit}`, getOptions()));
    })
    .concatMap((result) => {
      if (result.body.error) {
        throw new Error(result.body.error);
      }

      return Rx.Observable.of(...result.body.results);
    })
    .map((dataset) =>{
      let resource = dataset.resource;

      return {
        portalId: portal.id,
        portal: _.omit(portal, 'region'),
        name: resource.name,
        portalDatasetId: resource.id,
        created: toUTC(new Date(resource.createdAt)),
        updated: toUTC(new Date(resource.updatedAt)),
        description: resource.description,
        url: dataset.permalink || `${portal.url}/d/${dataset.id}`,
        license: _.get(dataset.metadata, 'license'),
        publisher: portal.name,
        tags: _.concat(_.get(dataset.classification, 'tags'), _.get(dataset.classificatio, 'domain_tags')),
        categories: _.concat(_.get(dataset.classification, 'categories'), _.get(dataset.classification, 'domain_category')),
        raw: dataset,
        files: [],
        region: null
      };
    })
    .catch((error) => {
      logger.error(`Unable to download data from ${portal.url}. Message: ${error.message}.`);
      return Rx.Observable.empty();
    });
}
