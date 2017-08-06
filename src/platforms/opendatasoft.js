import _ from 'lodash';
import config from 'config';
import Rx from 'rxjs';
import log4js from 'log4js';
import { readFileSync } from 'fs';
import { RxHR } from "@akanass/rx-http-request";
import { getDB } from '../database';
import { toUTC } from '../utils/pg-util';
import { getOptions } from '../utils/request-util';

const cocurrency = config.get('cocurrency');
const rows = config.get('platforms.OpenDataSoft.rows');
const logger = log4js.getLogger('OpenDataSoft');

/**
 * Get a list of harvesting Jobs.
 * @return {Rx.Observable}        harvest job
 */
export function downloadAll() {

  let sql = readFileSync(__dirname + '/../queries/get_platform_portals.sql', 'utf-8');

  return getDB()
    .query(sql, ['OpenDataSoft'])
    .reduce((collection, portal) => {
      collection[portal.name] = portal;
      return collection;
    }, {})
    .concatMap((portals) => {
      return RxHR.get('https://data.opendatasoft.com/api/v2/catalog/datasets?rows=0&start=0', getOptions())
        .concatMap((result) => {
          let totalCount = Math.ceil(result.body.total_count / rows);

          return Rx.Observable.range(0, totalCount)
            .mergeMap((i) => download(`https://data.opendatasoft.com/api/v2/catalog/datasets?rows=${rows}&start=${i * rows}`, portals), cocurrency);
        });
    });
}

/**
 * Harvest a OpenDataSoft portal.
 * @param   {String}      name  portal name
 * @return  {Observable}        a stream of dataset metadata
 */
export function downloadPortal(name) {

  let sql = readFileSync(__dirname + '/../queries/get_portal.sql', 'utf-8');

  return getDB()
    .query(sql, [name, 'OpenDataSoft'])
    .concatMap((row) => {
      return RxHR.get(`${row.url}/api/v2/catalog/datasets?rows=0&start=0`, getOptions())
        .concatMap((result) => {
          let totalCount = Math.ceil(result.body.total_count / rows);
          let idMap = {};
          idMap[row.name] = row;

          return Rx.Observable.range(0, totalCount)
            .mergeMap((i) => download(`${row.url}/api/v2/catalog/datasets?rows=${rows}&start=${i * rows}`, idMap), cocurrency);
        });
    });
}

/**
 * Harvest the open data network of OpenDataSoft with a given url.
 * @param  {String}             url         OpenDataSoft data network API url
 * @param  {Object}             portals     portals indexed by portal name
 * @return {Rx.Observable}                  harvest job
 */
export function download(url, portals) {

  return RxHR.get(url, getOptions())
    .concatMap((result) => {
      console.log(result);
      if (result.body.status === 500) {
        throw new Error(`Unable to download data from ${url}. Message: ${result.body.message}.`);
      }

      return Rx.Observable.of(...result.body.datasets);
    })
    .map((dataset) => {
      let metas = dataset.dataset.metas.default;

      if (!portals[metas.source_domain_title]) {
        return null;
      }

      let portal = portals[metas.source_domain_title];

      return {
        portalId: portal.id,
        portal: portal,
        name: metas.title,
        portalDatasetId: dataset.dataset.dataset_id,
        created: null,
        updated: toUTC(metas.modified ? new Date(metas.modified) : new Date()),
        description: metas.description,
        url: createLink(metas.source_domain_address, metas.source_dataset),
        license: metas.license,
        publisher: metas.publisher,
        tags: getValidArray(metas.keyword),
        categories: getValidArray(metas.theme),
        raw: dataset,
        region: checkGeom(metas.geographic_area.geometry),
        files: []
      };
    })
    .catch((error) => {
      logger.error(`Unable to download data from ${url}. Message: ${error.message}.`);
      return Rx.Observable.empty();
    })
    .filter((dataset) => dataset !== null);
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

function checkGeom(geometry) {
  if (!geometry || geometry.type !== 'Polygon' || geometry.type !== 'MultiPolygon') {
    return null;
  }

  if (geometry.type !== 'MultiPolygon') {
    geometry.type = 'MultiPolygon';
    geometry.coordinates = [geometry.coordinates];
  }

  return geometry;
}
