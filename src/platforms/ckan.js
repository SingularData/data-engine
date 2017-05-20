import _ from 'lodash';
import config from 'config';
import Rx from 'rxjs';
import log4js from 'log4js';
import { RxHR } from "@akanass/rx-http-request";
import { getDB } from '../database';
import { toUTC } from '../utils/pg-util';

const rows = config.get('platforms.CKAN.rows');
const userAgents = config.get('harvester.user_agents');
const logger = log4js.getLogger('CKAN');

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
    .query(sql, ['CKAN'])
    .mergeMap((portal) => download(portal.id, portal.url));
}

/**
 * Harvest the given CKAN portal.
 * @param  {Number}             portalID    portal ID
 * @param  {String}             portalUrl   portal Url
 * @return {Rx.Observable}                  harvest job
 */
export function download(portalID, portalUrl) {
  return RxHR.get(`${portalUrl}/api/3/action/package_search?start=0&rows=0`, {
    json: true,
    headers: {
      'User-Agent': _.sample(userAgents)
    }
  })
  .mergeMap((result) => {

    if (_.isString(result.body)) {
      throw new Error('Invalid API response.');
    }

    let totalCount = Math.ceil(result.body.result.count / rows);

    return Rx.Observable.range(0, totalCount)
      .mergeMap((i) => RxHR.get(`${portalUrl}/api/3/action/package_search?rows=${rows}&start=${i * rows}`, {
        json: true,
        headers: {
          'User-Agent': _.sample(userAgents)
        }
      }));
  })
  .mergeMap((result) => {

    if (_.isString(result.body)) {
      throw new Error('Invalid API response.');
    }

    let data = result.body.result.results;
    let datasets = [];

    for (let j = 0, m = data.length; j < m; j++) {
      let dataset = data[j];
      let dataFiles = _.map(dataset.resources, (file) => {
        return {
          name: file.title || file.name || file.format,
          description: file.description,
          link: file.url,
          format: file.format
        };
      });

      datasets.push({
        portalID: portalID,
        name: dataset.title,
        portalDatasetID: dataset.id,
        createdTime: toUTC(dataset.__extras ? new Date(dataset.__extras.metadata_created) : new Date(dataset.metadata_created)),
        updatedTime: toUTC(dataset.__extras ? new Date(dataset.__extras.metadata_modified) : new Date(dataset.metadata_modified)),
        description: dataset.notes,
        portalLink: `${portalUrl}/dataset/${dataset.package_id || dataset.id}`,
        license: dataset.license_title,
        publisher: _.get(dataset.organization, 'name'),
        tags: _.map(dataset.tags, 'display_name'),
        categories: _.map(dataset.groups, 'display_name'),
        raw: dataset,
        region: null,
        data: dataFiles
      });
    }

    return Rx.Observable.of(...datasets);
  })
  .catch((error) => {
    logger.error(`Unable to download data from ${portalUrl}. Message: ${error.message}.`);
    return Rx.Observable.empty();
  });
}
