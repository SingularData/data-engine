import _ from 'lodash';
import rp from 'request-promise';
import config from 'config';
import Promise from 'bluebird';
import { getDB } from '../database';

const limit = config.get('platforms.Junar.limit');
const userAgents = config.get('harvester.user_agents');

/**
 * Get a list of harvesting Jobs.
 * @return {Function[]}    An array of harvesting jobs.
 */
export function downloadAll() {

  let sql = `
    SELECT portal.id, portal.name, jpi.api_url AS url, jpi.api_key AS key FROM portal
    LEFT JOIN platform ON platform.id = portal.platform_id
    LEFT JOIN junar_portal_info AS jpi ON jpi.portal_id = portal.id
    WHERE platform.name = $1
  `;

  return getDB()
    .any(sql, 'Junar')
    .then(results => {
      let tasks = _.map(results, portal => {
        return download(portal.id, portal.name, portal.url, portal.key);
      });

      return tasks;
    });
}

/**
 * Harvest the given ArcGIS Open Data portal.
 * @param  {Number}             portalID    portal ID
 * @param  {String}             portalName  portal Name
 * @param  {String}             apiUrl      portal API url
 * @param  {String}             apiKey      portal API key
 * @return {Function}                       harvest job
 */
export function download(portalID, portalName, apiUrl, apiKey) {
  return function() {
    return rp({
      uri: `${apiUrl}/api/v2/datasets/?auth_key=${apiKey}&offset=0&limit=1`,
      method: 'GET',
      json: true,
      headers: {
        'User-Agent': _.sample(userAgents)
      }
    })
    .then(result => {
      let totalCount = result.count;
      let tasks = [];

      for (let i = 0; i < totalCount; i += limit) {
        let request = rp({
          uri: `${apiUrl}/api/v2/datasets/?auth_key=${apiKey}&offset=${i}&limit=${limit}`,
          method: 'GET',
          json: true,
          headers: {
            'User-Agent': _.sample(userAgents)
          }
        });

        tasks.push(request);
      }

      return Promise.all(tasks);
    })
    .then(results => {
      let datasets = [];

      for (let i = 0, n = results.length; i < n; i++) {
        let data = results[i].results;

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
      }

      return datasets;
    });
  };
}
