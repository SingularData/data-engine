import _ from 'lodash';
import config from 'config';
import Rx from 'rxjs';
import log4js from 'log4js';
import { RxHR } from "@akanass/rx-http-request";
import { getDB } from '../database';
import { toUTC } from '../utils/pg-util';

const rows = config.get('platforms.OpenDataSoft.rows');
const userAgents = config.get('harvester.user_agents');
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
      return RxHR.get('https://data.opendatasoft.com/api/v2/catalog/datasets?rows=0&start=0', {
        json: true,
        headers: {
          'User-Agent': _.sample(userAgents)
        }
      })
      .concatMap((result) => {
        let totalCount = Math.ceil(result.body.total_count / rows);

        return Rx.Observable.range(0, totalCount)
          .concatMap((i) => download(`https://data.opendatasoft.com/api/v2/catalog/datasets?rows=${rows}&start=${i * rows}`, portalIDs));
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
      return RxHR.get(`${row.url}/api/v2/catalog/datasets?rows=0&start=0`, {
        json: true,
        headers: {
          'User-Agent': _.sample(userAgents)
        }
      })
      .concatMap((result) => {
        let totalCount = Math.ceil(result.body.total_count / rows);
        let idMap = {};
        idMap[row.name] = row.id;

        return Rx.Observable.range(0, totalCount)
          .concatMap((i) => download(`${row.url}/api/v2/catalog/datasets?rows=${rows}&start=${i * rows}`, idMap));
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

  return RxHR.get(url, {
    json: true,
    headers: {
      'User-Agent': _.sample(userAgents)
    }
  })
  .concatMap((result) => {
    let datasets = [];
    let data = result.body;

    if (data.status === 500) {
      throw new Error(`Unable to download data from ${url}. Message: ${data.message}.`);
    }

    for (let item of data.datasets) {
      let metas = item.dataset.metas.default;

      if (!portalIDs[metas.source_domain_title]) {
        continue;
      }

      let dataset = {
        portalID: portalIDs[metas.source_domain_title],
        name: metas.title,
        portalDatasetID: item.dataset.dataset_id,
        createdTime: null,
        updatedTime: toUTC(metas.modified ? new Date(metas.modified) : new Date()),
        description: metas.description,
        portalLink: createLink(metas.source_domain_address, metas.source_dataset),
        license: metas.license,
        publisher: metas.publisher,
        tags: getValidArray(metas.keyword),
        categories: getValidArray(metas.theme),
        raw: item,
        region: null,
        data: []
      };

      datasets.push(dataset);
    }

    return Rx.Observable.of(...datasets);
  })
  .catch((error) => {
    logger.error(`Unable to download data from ${url}. Message: ${error.message}.`);
    return Rx.Observable.empty();
  });
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
