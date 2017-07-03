import _ from 'lodash';
import config from 'config';
import Rx from 'rxjs';
import log4js from 'log4js';
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
  let sql = `
    SELECT
      portal.id,
      portal.name,
      portal.url,
      CASE WHEN platform.name = 'Socrata' THEN 'us' ELSE 'eu' END as region
    FROM portal
    LEFT JOIN platform ON platform.id = portal.platform_id
    WHERE platform.name = $1::text OR platform.name = $2::text
  `;

  return getDB()
    .query(sql, ['Socrata', 'Socrata-EU'])
    .mergeMap((portal) => download(portal.id, portal.name, portal.url, portal.region), cocurrency);
}

export function downloadAllUS() {
  let sql = `
    SELECT
      portal.id,
      portal.name,
      portal.url,
      'us' as region
    FROM portal
    LEFT JOIN platform ON platform.id = portal.platform_id
    WHERE platform.name = $1::text
  `;

  return getDB()
    .query(sql, ['Socrata'])
    .mergeMap((portal) => download(portal.id, portal.name, portal.url, portal.region), cocurrency);
}

export function downloadAllEU() {
  let sql = `
    SELECT
      portal.id,
      portal.name,
      portal.url,
      'EU' as region
    FROM portal
    LEFT JOIN platform ON platform.id = portal.platform_id
    WHERE platform.name = $1::text
  `;

  return getDB()
    .query(sql, ['Socrata-EU'])
    .mergeMap((portal) => download(portal.id, portal.name, portal.url, portal.region), cocurrency);
}

/**
 * Harvest a Socrata portal.
 * @param   {String}      name    portal name
 * @param   {String}      region  portal region (us or eu)
 * @return  {Observable}          a stream of dataset metadata
 */
export function downloadPortal(name, region) {
  let sql = `
    SELECT p.id, p.name, p.url FROM portal AS p
    LEFT JOIN platform AS pl ON pl.id = p.platform_id
    WHERE p.name = $1::text AND pl.name = $2::text
    LIMIT 1
  `;

  return getDB()
    .query(sql, [name, 'Socrata'])
    .concatMap((row) => download(row.id, row.name, row.url, region));
}

/**
 * Harvest the given Socrata portal.
 * @param  {Number}             portalId    portal ID
 * @param  {String}             portalName  portal name
 * @param  {String}             portalUrl   portal url
 * @param  {String}             region      portal region (us or eu)
 * @return {Rx.Observable}                  harvest job
 */
export function download(portalId, portalName, portalUrl, region) {
  return RxHR.get(`http://api.${region}.socrata.com/api/catalog/v1?domains=${portalUrl}&offset=0&limit=0`, getOptions())
  .concatMap((result) => {
    if (result.body.error) {
      throw new Error(result.body.error);
    }

    let totalCount = Math.ceil(result.body.resultSetSize / limit);

    return Rx.Observable.range(0, totalCount)
      .concatMap((i) => RxHR.get(`http://api.${region}.socrata.com/api/catalog/v1?domains=${portalUrl}&limit=${limit}&offset=${i * limit}`, getOptions()));
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
      portalId,
      name: resource.name,
      portalDatasetId: resource.id,
      created: toUTC(new Date(resource.createdAt)),
      updated: toUTC(new Date(resource.updatedAt)),
      description: resource.description,
      url: dataset.permalink || `${portalUrl}/d/${dataset.id}`,
      license: _.get(dataset.metadata, 'license'),
      publisher: portalName,
      tags: _.concat(_.get(dataset.classification, 'tags'), _.get(dataset.classificatio, 'domain_tags')),
      categories: _.concat(_.get(dataset.classification, 'categories'), _.get(dataset.classification, 'domain_category')),
      raw: dataset,
      files: [],
      region: null
    };
  })
  .catch((error) => {
    logger.error(`Unable to download data from ${portalUrl}. Message: ${error.message}.`);
    return Rx.Observable.empty();
  });
}
