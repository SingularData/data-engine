import _ from 'lodash';
import config from 'config';
import Rx from 'rxjs';
import log4js from 'log4js';
import { RxHR } from "@akanass/rx-http-request";
import { getDB } from '../database';
import { toUTC } from '../utils/pg-util';
import { getOptions } from '../utils/request-util';

const cocurrency = config.get('cocurrency');
const rows = config.get('platforms.OpenDataSoft.rows');
const logger = log4js.getLogger('OpenDataSoft');

/**
 * Get a list of harvesting Jobs.
 * @return {Rx.Observable}        harvest job
 */
export function downloadAll() {

  let sql = `
    SELECT po.id, po.name FROM portal po
    LEFT JOIN platform pl ON pl.id = po.platform_id
    WHERE pl.name = $1::text
  `;

  return getDB()
    .query(sql, ['OpenDataSoft'])
    .reduce((collection, portal) => {
      collection[portal.name] = portal.id;
      return collection;
    }, {})
    .concatMap((portalIDs) => {
      return RxHR.get('https://data.opendatasoft.com/api/v2/catalog/datasets?rows=0&start=0', getOptions())
      .concatMap((result) => {
        let totalCount = Math.ceil(result.body.total_count / rows);

        return Rx.Observable.range(0, totalCount)
          .mergeMap((i) => download(`https://data.opendatasoft.com/api/v2/catalog/datasets?rows=${rows}&start=${i * rows}`, portalIDs), cocurrency);
      });
    });
}

/**
 * Harvest a OpenDataSoft portal.
 * @param   {String}      name  portal name
 * @return  {Observable}        a stream of dataset metadata
 */
export function downloadPortal(name) {
  let sql = `
    SELECT p.id, p.name, p.url FROM portal AS p
    LEFT JOIN platform AS pl ON pl.id = p.platform_id
    WHERE p.name = $1::text AND pl.name = $2::text
    LIMIT 1
  `;

  return getDB()
    .query(sql, [name, 'OpenDataSoft'])
    .concatMap((row) => {
      return RxHR.get(`${row.url}/api/v2/catalog/datasets?rows=0&start=0`, getOptions())
      .concatMap((result) => {
        let totalCount = Math.ceil(result.body.total_count / rows);
        let idMap = {};
        idMap[row.name] = row.id;

        return Rx.Observable.range(0, totalCount)
          .mergeMap((i) => download(`${row.url}/api/v2/catalog/datasets?rows=${rows}&start=${i * rows}`, idMap), cocurrency);
      });
    });
}

/**
 * Harvest the open data network of OpenDataSoft with a given url.
 * @param  {String}             url         OpenDataSoft data network API url
 * @param  {Object}             portalIDs   portal IDs indexed by portal name
 * @return {Rx.Observable}                  harvest job
 */
export function download(url, portalIDs) {

  return RxHR.get(url, getOptions())
  .concatMap((result) => {
    if (result.body.status === 500) {
      throw new Error(`Unable to download data from ${url}. Message: ${result.body.message}.`);
    }

    return Rx.Observable.of(...result.body.datasets);
  })
  .map((dataset) => {
    let metas = dataset.dataset.metas.default;

    if (!portalIDs[metas.source_domain_title]) {
      return null;
    }

    return {
      portalId: portalIDs[metas.source_domain_title],
      name: metas.title,
      portalDatasetId: dataset.dataset.dataset_id,
      created: null,
      updated: toUTC(metas.modified ? new Date(metas.modified) : new Date()),
      description: metas.description,
      url: createLink(metas.source_domain_address, metas.source_dataset),
      license: metas.license,
      publisher: metas.publisher,
      tags: getValidArray(metas.keyword),
      categories: getValidArray(metas.theme),
      raw: dataset,
      region: null,
      files: []
    };
  })
  .catch((error) => {
    logger.error(`Unable to download data from ${url}. Message: ${error.message}.`);
    return Rx.Observable.empty();
  })
  .filter((dataset) => dataset !== null);
}

/**
 * Get the site of the dataset
 * @param  {String} domain  portal domain
 * @param  {String} dataset dataset id
 * @return {String}         site url
 */
function createLink(domain, dataset) {
  return `https://${domain}/explore/dataset/${dataset}`;
}

function getValidArray(array) {
  if (_.isArray(array)) {
    return array;
  } else if (array && _.isString(array)) {
    return [array];
  }

  return [];
}
