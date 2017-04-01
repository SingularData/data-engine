import _ from 'lodash';
import config from 'config';
import Rx from 'rxjs';
import log4js from 'log4js';
import { RxHR } from "@akanass/rx-http-request";
import { getDB } from '../database';

const perPage = config.get('platforms.ArcGIS.per_page');
const userAgents = config.get('harvester.user_agents');
const logger = log4js.getLogger('ArcGIS Open Data');

/**
 * Get a list of harvesting Jobs.
 * @return {Rx.Observable}        harvest job
 */
export function downloadAll() {

  let sql = `
    SELECT portal.id, portal.url FROM portal
    LEFT JOIN platform ON platform.id = portal.platform_id
    WHERE platform.name = $1::text
  `;

  return getDB()
    .query(sql, ['ArcGIS Open Data'])
    .mergeMap((portal) => download(portal.id, portal.url));
}

/**
 * Harvest the given ArcGIS Open Data portal.
 * @param  {Number}             portalID    portal ID
 * @param  {String}             portalUrl   portal Url
 * @return {Rx.Observable}                  harvest job
 */
export function download(portalID, portalUrl) {
  return RxHR.get(`${portalUrl}/datasets?page=1&per_page=0`, {
    json: true,
    headers: {
      'User-Agent': _.sample(userAgents)
    }
  })
  .mergeMap(result => {
    if (_.isString(result.body)) {
      return Rx.Observable.throw(new Error(`The target portal doesn't provide APIs: ${portalUrl}`));
    }

    let totalCount = Math.ceil(result.body.metadata.stats.total_count / perPage);

    return Rx.Observable.range(1, totalCount)
      .map((i) => RxHR.get(`${portalUrl}/datasets?page=${i}&per_page=${perPage}`, {
        json: true,
        headers: {
          'User-Agent': _.sample(userAgents)
        }
      }))
      .mergeAll(1);
  }, 1)
  .map((result) => {
    let datasets = [];
    let data = result.body.data;

    for (let j = 0, m = data.length; j < m; j++) {
      let dataset = data[j];

      datasets.push({
        portalID: portalID,
        name: dataset.name,
        portalDatasetID: dataset.id,
        createdTime: dataset.created_at ? new Date(dataset.created_at) : null,
        updatedTime: new Date(dataset.updated_at),
        description: dataset.description,
        dataLink: null,
        portalLink: `${portalUrl}/datasets/${dataset.id}`,
        license: dataset.license,
        publisher: _.get(dataset.sites[0], 'title'),
        tags: dataset.tags,
        categories: [],
        raw: dataset
      });
    }

    return datasets;
  })
  .catch((error) => {
    logger.error(`Unable to download data from ${portalUrl}. Message: ${error.message}.`);
    return Rx.Observable.of([]);
  });
}
