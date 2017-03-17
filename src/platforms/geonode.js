import _ from 'lodash';
import rp from 'request-promise';
import config from 'config';
import { getDB } from '../database';

const userAgents = config.get('harvester.user_agents');

/**
 * Get a list of harvesting jobs.
 * @return {Function[]}    An array of harvesting jobs.
 */
export function downloadAll() {

  let sql = `
    SELECT portal.id, portal.name, portal.url FROM portal
    LEFT JOIN platform ON platform.id = portal.platform_id
    WHERE platform.name = $1
  `;

  return getDB()
    .any(sql, 'GeoNode')
    .then(results => {
      let tasks = _.map(results, portal => {
        return download(portal.id, portal.name, portal.url);
      });

      return tasks;
    });
}

/**
 * Harvest the given GeoNode portal.
 * @param  {Number}             portalID    portal ID
 * @param  {String}             portalName  portal name
 * @param  {String}             portalUrl   portal URL
 * @return {Function}                       harvest job
 */
export function download(portalID, portalName, portalUrl) {

  return function() {
    return rp({
      uri: `${portalUrl}/api/base`,
      method: 'GET',
      json: true,
      headers: {
        'User-Agent': _.sample(userAgents)
      }
    })
    .then(result => {
      let datasets = [];

      for (let j = 0, m = result.objects.length; j < m; j++) {
        let dataset = result.objects[j];

        datasets.push({
          portalID: portalID,
          name: dataset.title,
          portalDatasetID: dataset.uuid,
          createdTime: null,
          updatedTime: new Date(dataset.date),
          description: dataset.abstract,
          dataLink: null,
          portalLink: dataset.distribution_url || `${portalUrl}${dataset.detail_url}`,
          license: null,
          publisher: portalName,
          tags: [],
          categories: [dataset.category__gn_description],
          raw: dataset
        });
      }

      return datasets;
    });
  };
}
