import _ from 'lodash';
import rp from 'request-promise';
import config from 'config';
import Promise from 'bluebird';
import { getDB } from '../database';

const rows = config.get('platforms.CKAN.rows');
const userAgents = config.get('harvester.user_agents');

/**
 * Get a list harvesting Jobs.
 * @param  {Boolean}     chuckDonwload  A boolean value indicates whether to download in chuck.
 * @return {Function[]}                 An array of harvesting jobs.
 */
export function downloadAll(chuckDonwload) {

  let sql = `
    SELECT portal.id, portal.url FROM portal
    LEFT JOIN platform ON platform.id = portal.platform_id
    WHERE platform.name = $1
  `;

  return getDB()
    .any(sql, 'CKAN')
    .then(results => {

      if (chuckDonwload) {
        let tasks = _.map(results, portal => {
          return chunkDonwload(portal.id, portal.url);
        });

        return Promise.all(tasks);
      }

      return _.map(results, portal => {
        return download(portal.id, portal.url);
      });
    })
    .then(results => {
      return chunkDonwload ? _.flatten(results) : results;
    });
}

/**
 * Harvest the given CKAN portals in a chunk mode.
 * @param  {Number}             portalID    portal ID
 * @param  {String}             portalUrl   portal Url
 * @return {Promise<Function[]>}            harvest jobs
 */
export function chunkDonwload(portalID, portalUrl) {
  return rp({
    uri: `${portalUrl}/api/3/action/package_search?start=0&rows=0`,
    method: 'GET',
    json: true,
    headers: {
      'User-Agent': _.sample(userAgents)
    }
  })
  .then(result => {

    let totalCount = result.result.count;
    let tasks = [];

    for (let i = 0; i < totalCount; i += rows) {
      let request = () => {
        return rp({
          uri: `${portalUrl}/api/3/action/package_search?rows=${rows}&start=${i}`,
          method: 'GET',
          json: true,
          headers: {
            'User-Agent': _.sample(userAgents)
          }
        })
        .then(result => {
          let data = result.result.results;
          let datasets = [];

          for (let j = 0, m = data.length; j < m; j++) {
            let dataset = data[j];

            datasets.push({
              portalID: portalID,
              name: dataset.title,
              portalDatasetID: dataset.id,
              createdTime: new Date(dataset.metadata_created),
              updatedTime: new Date(dataset.metadata_modified),
              description: dataset.notes,
              dataLink: dataset.url,
              portalLink: `${portalUrl}/dataset/${dataset.package_id || dataset.id}`,
              license: dataset.license_title,
              publisher: _.get(dataset.organization, 'name'),
              tags: _.map(dataset.tags, 'display_name'),
              categories: _.map(dataset.groups, 'display_name'),
              raw: dataset
            });
          }

          return datasets;
        });
      };

      tasks.push(request);
    }

    return tasks;
  })
  .catch(error => {
    console.error(error);
    return [];
  });
}

/**
 * Harvest the given CKAN portal.
 * @param  {Number}             portalID    portal ID
 * @param  {String}             portalUrl   portal Url
 * @return {Function}                       harvest job
 */
export function download(portalID, portalUrl) {
  return function() {
    return rp({
      uri: `${portalUrl}/api/3/action/package_search?start=0&rows=0`,
      method: 'GET',
      json: true,
      headers: {
        'User-Agent': _.sample(userAgents)
      }
    })
    .then(result => {

      let totalCount = result.result.count;
      let tasks = [];

      for (let i = 0; i < totalCount; i += rows) {
        let request = rp({
          uri: `${portalUrl}/api/3/action/package_search?rows=${rows}&start=${i}`,
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
        let data = results[i].result.results;

        for (let j = 0, m = data.length; j < m; j++) {
          let dataset = data[j];

          datasets.push({
            portalID: portalID,
            name: dataset.title,
            portalDatasetID: dataset.id,
            createdTime: new Date(dataset.metadata_created),
            updatedTime: new Date(dataset.metadata_modified),
            description: dataset.notes,
            dataLink: dataset.url,
            portalLink: `${portalUrl}/dataset/${dataset.package_id || dataset.id}`,
            license: dataset.license_title,
            publisher: _.get(dataset.organization, 'name'),
            tags: _.map(dataset.tags, 'display_name'),
            categories: _.map(dataset.groups, 'display_name'),
            raw: dataset
          });
        }
      }

      return datasets;
    });
  };
}
