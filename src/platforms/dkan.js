import _ from 'lodash';
import config from 'config';
import Rx from 'rxjs';
import log4js from 'log4js';
import { RxHR } from '@akanass/rx-http-request';
import { getDB } from '../database';
import { toUTC } from '../utils/pg-util';
import { wktToGeoJSON } from '../utils/geom-util';
import { getOptions } from '../utils/request-util';

const cocurrency = config.get('cocurrency');
const logger = log4js.getLogger('DKAN');

/**
 * Get a list of harvesting jobs.
 * @return {Rx.Observable}        harvest job
 */
export function downloadAll() {

  let sql = `
    SELECT portal.id, portal.name, portal.url FROM portal
    LEFT JOIN platform ON platform.id = portal.platform_id
    WHERE platform.name = $1::text
  `;

  return getDB()
    .query(sql, ['DKAN'])
    .concatMap((portal) => download(portal.id, portal.name, portal.url));
}

/**
 * Harvest a DKAN portal.
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
    .query(sql, [name, 'DKAN'])
    .mergeMap((row) => download(row.id, row.name, row.url), cocurrency);
}

/**
 * Harvest the given DKAN portal.
 * @param  {Number}             portalId    portal ID
 * @param  {String}             portalName  portal name
 * @param  {String}             portalUrl   portal URL
 * @return {Rx.Observable}                  harvest job
 */
export function download(portalId, portalName, portalUrl) {
  return RxHR.get(`${portalUrl}/data.json`, getOptions())
    .concatMap((result) => {

      if (_.isString(result.body)) {
        throw new Error(`Invalid API response: ${portalUrl}/data.json`);
      }

      let data = _.isArray(result.body) ? result.body : result.body.dataset;
      return Rx.Observable.of(...data);
    })
    .map((dataset) => {
      let dataFiles = _.map(dataset.distribution, (file) => {
        return {
          name: file.title || file.format,
          format: _.toLower(file.format),
          url: file.downloadURL || file.accessURL,
          description: file.description
        };
      }).filter((file) => file.url && file.format);

      return {
        portalId,
        portal: 'portalName',
        platform: 'DKAN',
        name: dataset.title,
        portalDatasetId: dataset.identifier,
        created: dataset.issued ? toUTC(new Date(getDateString(dataset.issued))) : null,
        updated: toUTC(dataset.modified ? new Date(getDateString(dataset.modified)) : new Date()),
        description: dataset.description,
        url: dataset.landingPage || `${portalUrl}/search/type/dataset?query=${_.escape(dataset.title.replace(/ /g, '+'))}`,
        license: dataset.license,
        publisher: dataset.publisher ? dataset.publisher.name : portalName,
        tags: dataset.keyword || [],
        categories: [],
        raw: dataset,
        region: wktToGeoJSON(dataset.spatial),
        files: dataFiles
      };
    })
    .catch((error) => {
      logger.error(`Unable to download data from ${portalUrl}. Message: ${error.message}.`);
      return Rx.Observable.empty();
    });
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
