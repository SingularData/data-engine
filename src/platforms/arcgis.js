import _ from 'lodash';
import log4js from 'log4js';
import { readFileSync } from 'fs';
import { Observable } from 'rxjs';
import { RxHR } from "@akanass/rx-http-request";
import { getDB } from '../database';
import { toUTC } from '../utils/pg-util';
import { getOptions } from '../utils/request-util';

const logger = log4js.getLogger('ArcGIS Open Data');

/**
 * Get a list of harvesting Jobs.
 * @return {Observable}        harvest job
 */
export function downloadAll() {

  let sql = readFileSync(__dirname + '/../queries/get_platform_portals.sql', 'utf-8');

  return getDB()
    .query(sql, ['ArcGIS Open Data'])
    .concatMap((portal) => download(portal));
}

/**
 * Harvest an ArcGIS Open Data portal.
 * @param   {string}      name  portal name
 * @return  {Observable}        a stream of dataset metadata
 */
export function downloadPortal(name) {

  let sql = readFileSync(__dirname + '/../queries/get_portal.sql', 'utf-8');

  return getDB()
    .query(sql, [name, 'ArcGIS Open Data'])
    .concatMap((portal) => download(portal));
}

/**
 * Harvest the given ArcGIS Open Data portal.
 * @param  {Portal}      portal    portal information
 * @return {Observable}            a stream of dataset metadata
 */
export function download(portal) {
  return RxHR.get(`${portal.url}/data.json`, getOptions())
    .concatMap((result) => {

      if (_.isString(result.body)) {
        throw new Error(`The target portal doesn't provide APIs: ${portal.url}`);
      }

      return Observable.of(...result.body.dataset);
    })
    .map((dataset) => {
      let dataFiles = _.map(dataset.distribution, (file) => {
        return {
          name: file.title || file.format,
          format: _.toLower(file.format),
          url: file.downloadURL || file.accessURL
        };
      });

      return {
        portalId: portal.id,
        portal: portal,
        name: dataset.title,
        portalDatasetId: dataset.identifier,
        created: dataset.issued ? toUTC(new Date(dataset.issued)) : null,
        updated: toUTC(new Date(dataset.modified)),
        description: dataset.description,
        url: dataset.landingPage,
        license: dataset.license,
        publisher: dataset.publisher.name,
        tags: dataset.keyword,
        categories: [],
        raw: dataset,
        spatial: bboxToGeoJSON(dataset.spatial),
        files: dataFiles
      };
    })
    .catch((error) => {
      logger.error(`Unable to download data from ${portal.url}. Message: ${error.message}.`);
      return Observable.empty();
    });
}

function bboxToGeoJSON(bboxString) {
  if (!bboxString) {
    return null;
  }

  let coords = bboxString.split(',').map((value) => +value);

  return {
    type: 'MultiPolygon',
    coordinates: [
      [[[coords[0], coords[1]], [coords[0], coords[3]], [coords[2], coords[3]], [coords[2], coords[1]], [coords[0], coords[1]]]]
    ]
  };
}
