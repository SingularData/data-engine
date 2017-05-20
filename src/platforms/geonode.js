import _ from 'lodash';
import config from 'config';
import Rx from 'rxjs';
import log4js from 'log4js';
import { RxHR } from "@akanass/rx-http-request";
import { getDB } from '../database';
import { toUTC } from '../utils/pg-util';
import { wktToGeoJSON } from '../utils/geom-util';

const userAgents = config.get('harvester.user_agents');
const logger = log4js.getLogger('GeoNode');

/**
 * Get a list of harvesting jobs.
 * @return {Rx.Observable}        harvest job
 */
export function downloadAll() {

  let sql = `
    SELECT portal.id, portal.name, portal.url FROM portal
    LEFT JOIN platform ON platform.id = portal.platform_id
    WHERE platform.name = $1::text
  `;

  return getDB()
    .query(sql, ['GeoNode'])
    .mergeMap((portal) => download(portal.id, portal.name, portal.url));
}

/**
 * Harvest the given GeoNode portal.
 * @param  {Number}             portalID    portal ID
 * @param  {String}             portalName  portal name
 * @param  {String}             portalUrl   portal URL
 * @return {Rx.Observable}                  harvest job
 */
export function download(portalID, portalName, portalUrl) {
  return RxHR.get(`${portalUrl}/api/base`, {
    json: true,
    headers: {
      'User-Agent': _.sample(userAgents)
    }
  })
  .mergeMap((result) => {
    let datasets = [];

    for (let j = 0, m = result.body.objects.length; j < m; j++) {
      let dataset = result.body.objects[j];
      let dataFiles = [];

      if (dataset.distribution_description && dataset.distribution_url) {
        dataFiles.push({ description: dataset.distribution_description, link: dataset.distribution_url });
      }

      datasets.push({
        portalID: portalID,
        name: dataset.title,
        portalDatasetID: dataset.uuid,
        createdTime: null,
        updatedTime: toUTC(new Date(dataset.date)),
        description: dataset.abstract,
        portalLink: dataset.distribution_url || `${portalUrl}${dataset.detail_url}`,
        license: null,
        publisher: portalName,
        tags: [],
        categories: [dataset.category__gn_description],
        raw: dataset,
        region: wktToGeoJSON(dataset.csw_wkt_geometry),
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
