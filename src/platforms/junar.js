import config from 'config';
import Rx from 'rxjs';
import log4js from 'log4js';
import { omit } from 'lodash';
import { readFileSync } from 'fs';
import { RxHR } from "@akanass/rx-http-request";
import { getDB } from '../database';
import { toUTC, toCamelCase } from '../utils/pg-util';
import { getOptions } from '../utils/request-util';

const cocurrency = config.get('cocurrency');
const limit = config.get('platforms.Junar.limit');
const logger = log4js.getLogger('Junar');

/**
 * Get a list of harvesting Jobs.
 * @return {Rx.Observable}        harvest job
 */
export function downloadAll() {

  let sql = readFileSync(__dirname + '/../queries/get_junar_portals.sql', 'utf-8');

  return getDB()
    .query(sql, ['Junar'])
    .map((portal) => toCamelCase(portal))
    .concatMap((portal) => download(portal));
}

/**
 * Harvest a Junar portal.
 * @param   {String}      name  portal name
 * @return  {Observable}        a stream of dataset metadata
 */
export function downloadPortal(name) {

  let sql = readFileSync(__dirname + '/../queries/get_junar_portal.sql', 'utf-8');

  return getDB()
    .query(sql, [name, 'Junar'])
    .map((portal) => toCamelCase(portal))
    .concatMap((portal) => download(portal));
}

/**
 * Harvest the given Junar portal.
 * @param  {Portal}          portal    portal information
 * @return {Rx.Observable}             harvest job
 */
export function download(portal) {

  return RxHR.get(`${portal.apiUrl}/api/v2/datasets/?auth_key=${portal.apiKey}&offset=0&limit=1`, getOptions())
    .concatMap((result) => {

      if (!Number.isInteger(result.body.count)) {
        throw new Error(`Invalid data count for ${portal.apiUrl}`);
      }

      let totalCount = Math.ceil(result.body.count / limit);

      return Rx.Observable.range(0, totalCount)
        .mergeMap((i) => RxHR.get(`${portal.apiUrl}/api/v2/datasets/?auth_key=${portal.apiKey}&offset=${i * limit}&limit=${limit}`, getOptions()), cocurrency);
    })
    .concatMap((result) => Rx.Observable.of(...result.body.results))
    .map((dataset) => {
      let issued = new Date();
      let modified = new Date();

      issued.setTime(dataset.created_at * 1000);
      modified.setTime(dataset.modified_at * 1000);

      return {
        portalId: portal.id,
        portal: omit(portal, 'apiUrl', 'apiKey'),
        title: dataset.title,
        portalDatasetId: dataset.guid,
        issued: toUTC(issued),
        modified: toUTC(modified),
        description: dataset.description,
        license: null,
        landingPage: dataset.link,
        publisher: portal.name,
        keyword: dataset.tags,
        theme: [dataset.category_name],
        raw: dataset,
        distribution: [],
        spatial: null
      };
    })
    .catch((error) => {
      logger.error(`Unable to download data from ${portal.name}. Message: ${error.message}.`);
      return Rx.Observable.empty();
    });
}
