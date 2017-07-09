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
    SELECT portal.id, portal.name, portal.url FROM portal
    LEFT JOIN platform ON platform.id = portal.platform_id
    WHERE platform.name = $1::text
  `;

  return getDB()
    .query(sql, ['CKAN'])
    .concatMap((portal) => download(portal.id, portal.name, portal.url));
}

/**
 * Harvest a CAKN portal.
 * @param   {String}      name  portal name
 * @return  {Observable}        a stream of dataset metadata
 */
export function downloadPortal(name) {
  let sql = `
    SELECT p.id, p.name, p.url FROM portal AS p
    LEFT JOIN platform AS pl ON pl.id = p.platform_id
    WHERE p.name = $1::text AND pl.name = $2::text
    LIMIT 1
  `;

  return getDB()
    .query(sql, [name, 'CKAN'])
    .concatMap((row) => download(row.id, row.name, row.url));
}

/**
 * Harvest the given CKAN portal.
 * @param  {Number}             portalId    portal ID
 * @param  {String}             portalName  portal Name
 * @param  {String}             portalUrl   portal Url
 * @return {Rx.Observable}                  harvest job
 */
export function download(portalId, portalName, portalUrl) {
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

      return Rx.Observable.of(...result.body.result.results);
    })
    .map((dataset) => {
      let dataFiles = _.map(dataset.resources, (file) => {
        return {
          name: file.title || file.name || file.format,
          description: file.description,
          url: file.url,
          format: file.format
        };
      });

      return {
        portalId: portalId,
        portal: portalName,
        platform: 'CKAN',
        name: dataset.title,
        portalDatasetId: dataset.id,
        created: toUTC(dataset.__extras ? new Date(dataset.__extras.metadata_created) : new Date(dataset.metadata_created)),
        updated: toUTC(dataset.__extras ? new Date(dataset.__extras.metadata_modified) : new Date(dataset.metadata_modified)),
        description: dataset.notes,
        url: `${portalUrl}/dataset/${dataset.package_id || dataset.id}`,
        license: dataset.license_title,
        publisher: _.get(dataset.organization, 'name') || portalName,
        tags: _.map(dataset.tags, 'display_name'),
        categories: _.map(dataset.groups, 'display_name'),
        raw: dataset,
        region: null,
        files: dataFiles
      };
    })
    .catch((error) => {
      logger.error(`Unable to download data from ${portalUrl}. Message: ${error.message}.`);
      return Rx.Observable.empty();
    });
}
