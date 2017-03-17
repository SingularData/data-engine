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
    .any(sql, 'DKAN')
    .then(results => {
      let tasks = _.map(results, portal => {
        return download(portal.id, portal.name, portal.url);
      });

      return tasks;
    });
}

/**
 * Harvest the given DKAN portal.
 * @param  {Number}             portalID    portal ID
 * @param  {String}             portalName  portal name
 * @param  {String}             portalUrl   portal URL
 * @return {Function}                       harvest job
 */
export function download(portalID, portalName, portalUrl) {

  return function() {
    return rp({
      uri: `${portalUrl}/data.json`,
      method: 'GET',
      json: true,
      headers: {
        'User-Agent': _.sample(userAgents)
      }
    })
    .then(result => {

      if (_.isString(result)) {
        throw new Error(`Invalid API response: ${portalUrl}`);
      }

      let data = _.isArray(result) ? result : result.dataset;
      let datasets = [];

      for (let j = 0, m = data.length; j < m; j++) {
        let dataset = data[j];

        if (!dataset.title) {
          continue;
        }

        datasets.push({
          portalID: portalID,
          name: dataset.title,
          portalDatasetID: dataset.identifier,
          createdTime: dataset.issued ? new Date(getDateString(dataset.issued)) : null,
          updatedTime: dataset.modified ? new Date(getDateString(dataset.modified)) : new Date(),
          description: dataset.description,
          dataLink: null,
          portalLink: `${portalUrl}/search/type/dataset?query=${_.escape(dataset.title.replace(/ /g, '+'))}`,
          license: dataset.license,
          publisher: dataset.publisher ? dataset.publisher.name : portalName,
          tags: dataset.keyword || [],
          categories: [],
          raw: dataset
        });
      }

      return datasets;
    });
  };
}

function getDateString(raw) {
  if (raw.match(/\d{4}-\d{2}-\d{2}/)) {
    return raw;
  }

  let match = raw.search(/\d{2}\/\d{2}\/\d{4}/);

  if (match > -1) {
    return raw.substr(match, 10);
  }
}
