import _ from 'lodash';
import config from 'config';
import Rx from 'rxjs';
import { RxHR } from "@akanass/rx-http-request";
import { getDB } from '../database';

const rows = config.get('platforms.OpenDataSoft.rows');
const userAgents = config.get('harvester.user_agents');

/**
 * Get a list of harvesting Jobs.
 * @return {Rx.Observable}        harvest job
 */
export function downloadAll() {

  let sql = `
    SELECT po.id, po.name FROM portal po
    LEFT JOIN platform pl ON pl.id = po.platform_id
    WHERE pl.name = $1::text
  `;

  return getDB()
    .query(sql, ['OpenDataSoft'])
    .reduce((collection, portal) => {
      collection[portal.name] = portal.id;
      return collection;
    }, {})
    .mergeMap((portalIDs) => {
      return RxHR.get('https://data.opendatasoft.com/api/v2/catalog/datasets?rows=0&start=0', {
        json: true,
        headers: {
          'User-Agent': _.sample(userAgents)
        }
      })
      .mergeMap((result) => {
        let totalCount = Math.ceil(result.body.total_count / rows);

        return Rx.Observable.range(0, totalCount)
          .mergeMap((i) => download(`https://data.opendatasoft.com/api/v2/catalog/datasets?rows=${rows}&start=${i * rows}`, portalIDs), 1);
      })
      .mergeAll(1);
    });
}

/**
 * Harvest the open data network of OpenDataSoft with a given url.
 * @param  {String}             url         OpenDataSoft data network API url
 * @param  {Object}             portalIDs   portal IDs indexed by portal name
 * @return {Rx.Observable}                  harvest job
 */
export function download(url, portalIDs) {

  return RxHR.get(url, {
    json: true,
    headers: {
      'User-Agent': _.sample(userAgents)
    }
  })
  .map((result) => {
    let datasets = [];
    let data = result.body;

    for (let i = 0, n = data.datasets.length; i < n; i++) {
      let item = data.datasets[i];
      let metas = item.dataset.metas.default;

      if (!portalIDs[metas.source_domain_title]) {
        continue;
      }

      let dataset = {
        portalID: portalIDs[metas.source_domain_title],
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

function getValidArray(array) {
  if (_.isArray(array)) {
    return array;
  } else if (array && _.isString(array)) {
    return [array];
  }

  return [];
}
