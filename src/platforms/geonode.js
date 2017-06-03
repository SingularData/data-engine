import config from 'config';
import Rx from 'rxjs';
import log4js from 'log4js';
import { RxHR } from "@akanass/rx-http-request";
import { getDB } from '../database';
import { toUTC } from '../utils/pg-util';
import { wktToGeoJSON } from '../utils/geom-util';
import { getOptions } from '../utils/request-util';

const cocurrency = config.get('cocurrency');
const logger = log4js.getLogger('GeoNode');

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
    .query(sql, ['GeoNode'])
    .concatMap((portal) => download(portal.id, portal.name, portal.url));
}

/**
 * Harvest a GeoNode portal.
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
    .query(sql, [name, 'GeoNode'])
    .mergeMap((row) => download(row.id, row.name, row.url), cocurrency);
}

/**
 * Harvest the given GeoNode portal.
 * @param  {Number}             portalID    portal ID
 * @param  {String}             portalName  portal name
 * @param  {String}             portalUrl   portal URL
 * @return {Rx.Observable}                  harvest job
 */
export function download(portalID, portalName, portalUrl) {
  return RxHR.get(`${portalUrl}/api/base`, getOptions())
  .concatMap((result) => Rx.Observable.of(...result.body.objects))
  .map((dataset) => {
    let dataFiles = [];

    if (dataset.distribution_description && dataset.distribution_url) {
      dataFiles.push({ description: dataset.distribution_description, link: dataset.distribution_url });
    }

    return {
      portalID: portalID,
      name: dataset.title,
      portalDatasetID: dataset.uuid,
      createdTime: null,
      updatedTime: toUTC(new Date(dataset.date)),
      description: dataset.abstract,
      portalLink: dataset.distribution_url || `${portalUrl}${dataset.detail_url}`,
      license: null,
      publisher: portalName,
      tags: [],
      categories: [dataset.category__gn_description],
      raw: dataset,
      region: wktToGeoJSON(dataset.csw_wkt_geometry),
      data: dataFiles
    };
  })
  .catch((error) => {
    logger.error(`Unable to download data from ${portalUrl}. Message: ${error.message}.`);
    return Rx.Observable.empty();
  });
}
