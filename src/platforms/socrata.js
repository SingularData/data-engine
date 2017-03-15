import _ from 'lodash';
import rp from 'request-promise';
import config from 'config';
import Promise from 'bluebird';
import { getDB } from '../database';

const limit = config.get('platforms.Socrata.limit');
const userAgents = config.get('harvester.user_agents');

/**
 * Get a list harvesting Jobs.
 * @return {Function[]}    An array of harvesting jobs.
 */
export function downloadAll() {

  let sql = `
    SELECT
      portal.id,
      portal.url,
      CASE WHEN platform.name = 'Socrata' THEN 'us' ELSE 'eu' END as region
    FROM portal
    LEFT JOIN platform ON platform.id = portal.platform_id
    WHERE platform.name = $1 OR platform.name = $2
  `;

  return getDB()
    .any(sql, ['Socrata', 'Socrata-EU'])
    .then(results => {
      let tasks = _.map(results, portal => {
        return download(portal.id, portal.url, portal.region);
      });

      return tasks;
    });
}

/**
 * Harvest the given ArcGIS Open Data portal.
 * @param  {Number}             portalID    portal ID
 * @param  {String}             portalUrl   portal Url
 * @param  {String}             region      portal region (us or eu)
 * @return {Function}                       harvest job
 */
export function download(portalID, portalUrl, region) {

  return function() {
    return rp({
      uri: `http://api.${region}.socrata.com/api/catalog/v1?domains=${portalUrl}&offset=0&limit=0`,
      method: 'GET',
      json: true,
      headers: {
        'User-Agent': _.sample(userAgents)
      }
    })
    .then(result => {
      let totalCount = result.resultSetSize;
      let tasks = [];

      for (let i = 0; i < totalCount; i += limit) {
        let request = rp({
          uri: `http://api.${region}.socrata.com/api/catalog/v1?domains=${portalUrl}&limit=${limit}&offset=${i}`,
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
          let dataset = data[j].resource;

          datasets.push({
            portalID: portalID,
            name: dataset.name,
            portalDatasetID: dataset.id,
            createdTime: new Date(dataset.createdAt),
            updatedTime: new Date(dataset.updatedAt),
            description: dataset.description,
            dataLink: null,
            portalLink: dataset.permalink || `${portalUrl}/d/${dataset.id}`,
            license: _.get(dataset.metadata, 'license'),
            publisher: portalUrl,
            tags: _.concat(_.get(dataset.classification, 'tags'), _.get(dataset.classificatio, 'domain_tags')),
            categories: _.concat(_.get(dataset.classification, 'categories'), _.get(dataset.classification, 'domain_category')),
            raw: dataset
          });
        }
      }

      return datasets;
    });
  };
}
