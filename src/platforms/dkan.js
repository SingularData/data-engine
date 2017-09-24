import _ from 'lodash';
import Rx from 'rxjs';
import log4js from 'log4js';
import { readFileSync } from 'fs';
import { RxHR } from '@akanass/rx-http-request';
import { getDB } from '../database';
import { toUTC } from '../utils/pg-util';
import { wktToGeoJSON } from '../utils/geom-util';
import { getOptions } from '../utils/request-util';

const logger = log4js.getLogger('DKAN');

/**
 * Get a list of harvesting jobs.
 * @return {Rx.Observable}        harvest job
 */
export function downloadAll() {

  let sql = readFileSync(__dirname + '/../queries/get_platform_portals.sql', 'utf-8');

  return getDB()
    .query(sql, ['DKAN'])
    .concatMap((portal) => download(portal));
}

/**
 * Harvest a DKAN portal.
 * @param   {String}      name  portal name
 * @return  {Observable}        a stream of dataset metadata
 */
export function downloadPortal(name) {

  let sql = readFileSync(__dirname + '/../queries/get_portal.sql', 'utf-8');

  return getDB()
    .query(sql, [name, 'DKAN'])
    .concatMap((portal) => download(portal));
}

/**
 * Harvest the given DKAN portal.
 * @param  {Portal}          portal    portal information
 * @return {Rx.Observable}             dataset
 */
export function download(portal) {
  return RxHR.get(`${portal.url}/data.json`, getOptions())
    .concatMap((result) => {

      if (_.isString(result.body)) {
        throw new Error(`Invalid API response: ${portal.url}/data.json`);
      }

      let data = _.isArray(result.body) ? result.body : result.body.dataset;
      return Rx.Observable.of(...data);
    })
    .map((dataset) => {

      dataset.distribution = dataset.distribution.filter((dist) => dist.downloadURL || dist.accessURL);

      return {
        portal: portal,
        title: dataset.title,
        issued: dataset.issued ? toUTC(new Date(getDateString(dataset.issued))) : null,
        modified: toUTC(dataset.modified ? new Date(getDateString(dataset.modified)) : new Date()),
        description: dataset.description,
        landingPage: dataset.landingPage || `${portal.url}/search/type/dataset?query=${_.escape(dataset.title.replace(/ /g, '+'))}`,
        license: dataset.license,
        publisher: dataset.publisher ? dataset.publisher.name : portal.name,
        keyword: dataset.keyword || [],
        theme: [],
        raw: dataset,
        spatial: wktToGeoJSON(dataset.spatial),
        distribution: dataset.distribution
      };
    })
    .catch((error) => {
      logger.error(`Unable to download data from ${portal.url}. Message: ${error.message}.`);
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
