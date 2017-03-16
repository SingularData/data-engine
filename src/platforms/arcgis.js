import _ from 'lodash';
import rp from 'request-promise';
import config from 'config';
import Promise from 'bluebird';
import { getDB } from '../database';

const perPage = config.get('platforms.ArcGISOpenData.per_page');
const userAgents = config.get('harvester.user_agents');

/**
 * Get a list of harvesting Jobs.
 * @return {Function[]}    An array of harvesting jobs.
 */
export function downloadAll() {

  let sql = `
    SELECT portal.id, portal.url FROM portal
    LEFT JOIN platform ON platform.id = portal.platform_id
    WHERE platform.name = $1
  `;

  return getDB()
    .any(sql, 'ArcGIS Open Data')
    .then(results => {
      let tasks = _.map(results, portal => {
        return download(portal.id, portal.url);
      });

      return tasks;
    });
}

/**
 * Harvest the given ArcGIS Open Data portal.
 * @param  {Number}             portalID    portal ID
 * @param  {String}             portalUrl   portal Url
 * @return {Function}                       harvest job
 */
export function download(portalID, portalUrl) {

  return function() {
    return rp({
      uri: `${portalUrl}/datasets?page=1&per_page=0`,
      method: 'GET',
      json: true,
      headers: {
        'User-Agent': _.sample(userAgents)
      }
    })
    .then(result => {
      if (_.isString(result)) {
        return Promise.reject(new Error(`The target portal doesn't provide APIs: ${portalUrl}`));
      }

      let totalCount = result.metadata.stats.total_count;
      let tasks = [];

      for (let i = 0, j = 1; i < totalCount; i += perPage, j++) {
        let request = rp({
          uri: `${portalUrl}/datasets?page=${j}&per_page=${perPage}`,
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
        let data = results[i].data;

        for (let j = 0, m = data.length; j < m; j++) {
          let dataset = data[j];

          datasets.push({
            portalID: portalID,
            name: dataset.name,
            portalDatasetID: dataset.id,
            createdTime: new Date(dataset.created_at),
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
      }

      return datasets;
    });
  };
}
