import _ from 'lodash';
import rp from 'request-promise';
import config from 'config';
import Promise from 'bluebird';
import { getDB } from '../database';

const rows = config.get('platforms.OpenDataSoft.rows');
const userAgents = config.get('harvester.user_agents');

/**
 * Get a list harvesting Jobs.
 * @return {Promise<[]>}    An array of harvesting jobs.
 */
export function downloadAll() {

  let getDatasetCount = rp({
    uri: 'https://data.opendatasoft.com/api/v2/catalog/datasets?rows=0&start=0',
    method: 'GET',
    json: true,
    headers: {
      'User-Agent': _.sample(userAgents)
    }
  })
  .then(result => result.total_count);

  return Promise.all([getDatasetCount, getPortalIDs()])
    .then(results => {
      let datasetCount = results[0];
      let portalIDs = results[1];
      let tasks = [];

      for (let i = 0; i <= datasetCount; i += rows) {
        let request = download(`https://data.opendatasoft.com/api/v2/catalog/datasets?rows=${rows}&start=${i}`, portalIDs);
        tasks.push(request);
      }

      return tasks;
    });
}

/**
 * Harvest the open data network of OpenDataSoft with a given url.
 * @param  {String}             url         OpenDataSoft data network API url
 * @param  {Object}             portalIDs   portal IDs indexed by portal name
 * @return {Promise<Object[]>}              an array of dataset metadata
 */
export function download(url, portalIDs) {

  let promise = portalIDs ? Promise.resolve(portalIDs) : getPortalIDs();

  return promise.then(portals => {
    return rp({
      uri: url,
      method: 'GET',
      json: true,
      headers: {
        'User-Agent': _.sample(userAgents)
      }
    })
    .then(data => {
      let datasets = [];

      for (let i = 0, n = data.datasets.length; i < n; i++) {
        let item = data.datasets[i];
        let metas = item.dataset.metas.default;

        if (!portals[metas.source_domain_title]) {
          continue;
        }

        let dataset = {
          portalID: portals[metas.source_domain_title],
          name: metas.title,
          portalDatasetID: item.dataset.dataset_id,
          createdTime: null,
          updatedTime: metas.modified ? new Date(metas.modified) : new Date(),
          description: metas.description,
          portalLink: createLink(metas.source_domain_address, metas.source_dataset),
          dataLink: null,
          license: metas.license,
          publisher: metas.publisher,
          tags: getValidArray(metas.keyword),
          categories: getValidArray(metas.theme),
          raw: item
        };

        datasets.push(dataset);
      }

      return datasets;
    });
  });
}

/**
 * Get the site of the dataset
 * @param  {String} domain  portal domain
 * @param  {String} dataset dataset id
 * @return {String}         site url
 */
function createLink(domain, dataset) {
  return `https://${domain}/explore/dataset/${dataset}`;
}

/**
 * Get portal IDs, indexed by portal name.
 * @return {Promise<Object>} a collection of portals indexed by name
 */
function getPortalIDs() {
  let sql = `
    SELECT po.id, po.name FROM portal po
    LEFT JOIN platform pl ON pl.id = po.platform_id
    WHERE pl.name = $1
  `;

  return getDB().any(sql, 'OpenDataSoft')
    .then(results => {
      return _.reduce(results, (collection, portal) => {
        collection[portal.name] = portal.id;
        return collection;
      }, {});
    });
}

function getValidArray(array) {
  if (_.isArray(array)) {
    return array;
  } else if (array && _.isString(array)) {
    return [array];
  }

  return [];
}
