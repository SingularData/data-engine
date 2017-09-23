import Rx from 'rxjs';
import log4js from 'log4js';
import { readFileSync } from 'fs';
import { RxHR } from "@akanass/rx-http-request";
import { getDB } from '../database';
import { toUTC } from '../utils/pg-util';
import { wktToGeoJSON } from '../utils/geom-util';
import { getOptions } from '../utils/request-util';

const logger = log4js.getLogger('GeoNode');

/**
 * Get a list of harvesting jobs.
 * @return {Rx.Observable}        harvest job
 */
export function downloadAll() {

  let sql = readFileSync(__dirname + '/../queries/get_platform_portals.sql', 'utf-8');

  return getDB()
    .query(sql, ['GeoNode'])
    .concatMap((portal) => download(portal));
}

/**
 * Harvest a GeoNode portal.
 * @param   {String}      name  portal name
 * @return  {Observable}        a stream of dataset metadata
 */
export function downloadPortal(name) {

  let sql = readFileSync(__dirname + '/../queries/get_portal.sql', 'utf-8');

  return getDB()
    .query(sql, [name, 'GeoNode'])
    .concatMap((portal) => download(portal));
}

/**
 * Harvest the given GeoNode portal.
 * @param  {Portal}          portal    portal information
 * @return {Rx.Observable}             dataset stream
 */
export function download(portal) {
  return RxHR.get(`${portal.url}/api/base`, getOptions())
    .concatMap((result) => Rx.Observable.of(...result.body.objects))
    .map((dataset) => {
      let distribution = [];

      if (dataset.distribution_description && dataset.distribution_url) {
        distribution.push({
          description: dataset.distribution_description,
          url: dataset.distribution_url
        });
      }

      return {
        portalId: portal.id,
        portal: portal,
        title: dataset.title,
        issued: null,
        modified: toUTC(new Date(dataset.date)),
        description: dataset.abstract,
        landingPage: dataset.distribution_url || `${portal.url}${dataset.detail_url}`,
        license: null,
        publisher: portal.name,
        keyword: [],
        theme: [dataset.category__gn_description],
        raw: dataset,
        spatial: wktToGeoJSON(dataset.csw_wkt_geometry),
        distribution
      };
    })
    .catch((error) => {
      logger.error(`Unable to download data from ${portal.url}. Message: ${error.message}.`);
      return Rx.Observable.empty();
    });
}
