import _ from 'lodash';
import config from 'config';
import Rx from 'rxjs';
import { RxHR } from "@akanass/rx-http-request";
import { getDB } from '../database';

const limit = config.get('platforms.Junar.limit');
const userAgents = config.get('harvester.user_agents');

/**
 * Get a list of harvesting Jobs.
 * @return {Rx.Observable}        harvest job
 */
export function downloadAll() {

  let sql = `
    SELECT portal.id, portal.name, jpi.api_url AS url, jpi.api_key AS key FROM portal
    LEFT JOIN platform ON platform.id = portal.platform_id
    LEFT JOIN junar_portal_info AS jpi ON jpi.portal_id = portal.id
    WHERE platform.name = $1::text
  `;

  return getDB()
    .query(sql, ['Junar'])
    .mergeMap((portal) => download(portal.id, portal.name, portal.url, portal.key));
}

/**
 * Harvest the given ArcGIS Open Data portal.
 * @param  {Number}             portalID    portal ID
 * @param  {String}             portalName  portal Name
 * @param  {String}             apiUrl      portal API url
 * @param  {String}             apiKey      portal API key
 * @return {Rx.Observable}                  harvest job
 */
export function download(portalID, portalName, apiUrl, apiKey) {
  return RxHR.get(`${apiUrl}/api/v2/datasets/?auth_key=${apiKey}&offset=0&limit=1`, {
    json: true,
    headers: {
      'User-Agent': _.sample(userAgents)
    }
  })
  .mergeMap(result => {
    let totalCount = Math.ceil(result.body.count / limit);

    return Rx.Observable.range(0, totalCount)
      .mergeMap((i) => RxHR.get(`${apiUrl}/api/v2/datasets/?auth_key=${apiKey}&offset=${i * limit}&limit=${limit}`, {
        json: true,
        headers: {
          'User-Agent': _.sample(userAgents)
        }
      }), 1);
  })
  .map(result => {
    let datasets = [];
    let data = result.body.results;

    for (let j = 0, m = data.length; j < m; j++) {
      let dataset = data[j];
      let createdTime = new Date();
      let updatedTime = new Date();

      createdTime.setTime(dataset.created_at);
      updatedTime.setTime(dataset.modified_at);

      datasets.push({
        portalID: portalID,
        name: dataset.title,
        portalDatasetID: dataset.guid,
        createdTime: createdTime,
        updatedTime: updatedTime,
        description: dataset.description,
        dataLink: null,
        license: 'Unknown',
        portalLink: dataset.link,
        publisher: portalName,
        tags: dataset.tags,
        categories: [dataset.category_name],
        raw: dataset
      });
    }

    return datasets;
  });
}
