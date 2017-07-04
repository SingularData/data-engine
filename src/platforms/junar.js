import config from 'config';
import Rx from 'rxjs';
import log4js from 'log4js';
import { RxHR } from "@akanass/rx-http-request";
import { getDB } from '../database';
import { toUTC } from '../utils/pg-util';
import { getOptions } from '../utils/request-util';

const cocurrency = config.get('cocurrency');
const limit = config.get('platforms.Junar.limit');
const logger = log4js.getLogger('Junar');

/**
 * Get a list of harvesting Jobs.
 * @return {Rx.Observable}        harvest job
 */
export function downloadAll() {

  let sql = `
    SELECT portal.id, portal.name, jpi.api_url AS url, jpi.api_key AS key FROM portal
    LEFT JOIN platform ON platform.id = portal.platform_id
    LEFT JOIN junar_portal_info AS jpi ON jpi.portal_id = portal.id
    WHERE platform.name = $1::text
  `;

  return getDB()
    .query(sql, ['Junar'])
    .concatMap((portal) => download(portal.id, portal.name, portal.url, portal.key));
}

/**
 * Harvest a Junar portal.
 * @param   {String}      name  portal name
 * @return  {Observable}        a stream of dataset metadata
 */
export function downloadPortal(name) {
  let sql = `
    SELECT p.id, p.name, jpi.api_url, jpi.api_key FROM portal AS p
    LEFT JOIN platform AS pl ON pl.id = p.platform_id
    LEFT JOIN junar_portal_info AS jpi ON jpi.portal_id = p.id
    WHERE p.name = $1::text AND pl.name = $2::text
    LIMIT 1
  `;

  return getDB()
    .query(sql, [name, 'Junar'])
    .concatMap((row) => download(row.id, row.name, row.api_url, row.api_key));
}

/**
 * Harvest the given Junar portal.
 * @param  {Number}             portalId    portal ID
 * @param  {String}             portalName  portal Name
 * @param  {String}             apiUrl      portal API url
 * @param  {String}             apiKey      portal API key
 * @return {Rx.Observable}                  harvest job
 */
export function download(portalId, portalName, apiUrl, apiKey) {
  return RxHR.get(`${apiUrl}/api/v2/datasets/?auth_key=${apiKey}&offset=0&limit=1`, getOptions())
    .concatMap((result) => {

      if (!Number.isInteger(result.body.count)) {
        throw new Error(`Invalid data count for ${apiUrl}`);
      }

      let totalCount = Math.ceil(result.body.count / limit);

      return Rx.Observable.range(0, totalCount)
        .mergeMap((i) => RxHR.get(`${apiUrl}/api/v2/datasets/?auth_key=${apiKey}&offset=${i * limit}&limit=${limit}`, getOptions()), cocurrency);
    })
    .concatMap((result) => Rx.Observable.of(...result.body.results))
    .map((dataset) => {
      let created = new Date();
      let updated = new Date();

      created.setTime(dataset.created_at * 1000);
      updated.setTime(dataset.modified_at * 1000);

      return {
        portalId: portalId,
        portal: portalName,
        platform: 'Junar',
        name: dataset.title,
        portalDatasetId: dataset.guid,
        created: toUTC(created),
        updated: toUTC(updated),
        description: dataset.description,
        license: null,
        url: dataset.link,
        publisher: portalName,
        tags: dataset.tags,
        categories: [dataset.category_name],
        raw: dataset,
        files: [],
        region: null
      };
    })
    .catch((error) => {
      logger.error(`Unable to download data from ${portalName}. Message: ${error.message}.`);
      return Rx.Observable.empty();
    });
}
