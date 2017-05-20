import _ from 'lodash';
import config from 'config';
import Rx from 'rxjs';
import log4js from 'log4js';
import { RxHR } from "@akanass/rx-http-request";
import { getDB } from '../database';
import { toUTC } from '../utils/pg-util';

const limit = config.get('platforms.Socrata.limit');
const userAgents = config.get('harvester.user_agents');
const logger = log4js.getLogger('Socrata');

/**
 * Get a list of harvesting Jobs.
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
    .mergeMap((portal) => download(portal.id, portal.name, portal.url, portal.region));
}

/**
 * Harvest the given ArcGIS Open Data portal.
 * @param  {Number}             portalID    portal ID
 * @param  {String}             portalName  portal name
 * @param  {String}             portalUrl   portal url
 * @param  {String}             region      portal region (us or eu)
 * @return {Rx.Observable}                  harvest job
 */
export function download(portalID, portalName, portalUrl, region) {
  return RxHR.get(`http://api.${region}.socrata.com/api/catalog/v1?domains=${portalUrl}&offset=0&limit=0`, {
    json: true,
    headers: {
      'User-Agent': _.sample(userAgents)
    }
  })
  .mergeMap((result) => {
    if (result.body.error) {
      throw new Error(result.body.error);
    }

    let totalCount = Math.ceil(result.body.resultSetSize / limit);

    return Rx.Observable.range(0, totalCount)
      .mergeMap((i) => RxHR.get(`http://api.${region}.socrata.com/api/catalog/v1?domains=${portalUrl}&limit=${limit}&offset=${i * limit}`, {
        json: true,
        headers: {
          'User-Agent': _.sample(userAgents)
        }
      }));
  })
  .mergeMap((result) => {
    if (result.body.error) {
      throw new Error(result.body.error);
    }

    let datasets = [];
    let data = result.body.results;

    for (let j = 0, m = data.length; j < m; j++) {
      let dataset = data[j].resource;

      datasets.push({
        portalID: portalID,
        name: dataset.name,
        portalDatasetID: dataset.id,
        createdTime: toUTC(new Date(dataset.createdAt)),
        updatedTime: toUTC(new Date(dataset.updatedAt)),
        description: dataset.description,
        portalLink: dataset.permalink || `${portalUrl}/d/${dataset.id}`,
        license: _.get(dataset.metadata, 'license'),
        publisher: portalName,
        tags: _.concat(_.get(dataset.classification, 'tags'), _.get(dataset.classificatio, 'domain_tags')),
        categories: _.concat(_.get(dataset.classification, 'categories'), _.get(dataset.classification, 'domain_category')),
        raw: dataset,
        data: [],
        region: null
      });
    }

    return Rx.Observable.of(...datasets);
  })
  .catch((error) => {
    logger.error(`Unable to download data from ${portalUrl}. Message: ${error.message}.`);
    return Rx.Observable.empty();
  });
}
