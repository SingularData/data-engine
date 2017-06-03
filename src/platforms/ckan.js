import _ from 'lodash';
import config from 'config';
import Rx from 'rxjs';
import log4js from 'log4js';
import { RxHR } from "@akanass/rx-http-request";
import { getDB } from '../database';
import { toUTC } from '../utils/pg-util';
import { getOptions } from '../utils/request-util';

const cocurrency = config.get('cocurrency');
const rows = config.get('platforms.CKAN.rows');
const logger = log4js.getLogger('CKAN');

/**
 * Get a list of harvesting Jobs.
 * @return {Rx.Observable}        harvest job
 */
export function downloadAll() {

  let sql = `
    SELECT portal.id, portal.url FROM portal
    LEFT JOIN platform ON platform.id = portal.platform_id
    WHERE platform.name = $1::text
  `;

  return getDB()
    .query(sql, ['CKAN'])
    .concatMap((portal) => download(portal.id, portal.url));
}

/**
 * Harvest a CAKN portal.
 * @param   {String}      name  portal name
 * @return  {Observable}        a stream of dataset metadata
 */
export function downloadPortal(name) {
  let sql = `
    SELECT p.id, p.url FROM portal AS p
    LEFT JOIN platform AS pl ON pl.id = p.platform_id
    WHERE p.name = $1::text AND pl.name = $2::text
    LIMIT 1
  `;

  return getDB()
    .query(sql, [name, 'CKAN'])
    .concatMap((row) => download(row.id, row.url));
}

/**
 * Harvest the given CKAN portal.
 * @param  {Number}             portalID    portal ID
 * @param  {String}             portalUrl   portal Url
 * @return {Rx.Observable}                  harvest job
 */
export function download(portalID, portalUrl) {
  return RxHR.get(`${portalUrl}/api/3/action/package_search?start=0&rows=0`, getOptions())
  .concatMap((result) => {

    if (_.isString(result.body)) {
      throw new Error('Invalid API response.');
    }

    let totalCount = Math.ceil(result.body.result.count / rows);

    return Rx.Observable.range(0, totalCount)
      .mergeMap((i) => RxHR.get(`${portalUrl}/api/3/action/package_search?rows=${rows}&start=${i * rows}`, getOptions()), cocurrency);
  })
  .concatMap((result) => {

    if (_.isString(result.body)) {
      throw new Error('Invalid API response.');
    }

    let data = result.body.result.results;
    let datasets = [];

    for (let j = 0, m = data.length; j < m; j++) {
      let dataset = data[j];
      let dataFiles = _.map(dataset.resources, (file) => {
        return {
          name: file.title || file.name || file.format,
          description: file.description,
          link: file.url,
          format: file.format
        };
      });

      datasets.push({
        portalID: portalID,
        name: dataset.title,
        portalDatasetID: dataset.id,
        createdTime: toUTC(dataset.__extras ? new Date(dataset.__extras.metadata_created) : new Date(dataset.metadata_created)),
        updatedTime: toUTC(dataset.__extras ? new Date(dataset.__extras.metadata_modified) : new Date(dataset.metadata_modified)),
        description: dataset.notes,
        portalLink: `${portalUrl}/dataset/${dataset.package_id || dataset.id}`,
        license: dataset.license_title,
        publisher: _.get(dataset.organization, 'name'),
        tags: _.map(dataset.tags, 'display_name'),
        categories: _.map(dataset.groups, 'display_name'),
        raw: dataset,
        region: null,
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
