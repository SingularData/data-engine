import Rx from 'rxjs';
import log4js from 'log4js';
import { getDB, save } from './database';

import * as opendatasoft from './platforms/opendatasoft';
import * as arcgis from './platforms/arcgis';
import * as socrata from './platforms/socrata';
import * as ckan from './platforms/ckan';
import * as junar from './platforms/junar';
import * as geonode from './platforms/geonode';
import * as dkan from './platforms/dkan';

const logger = log4js.getLogger('harvester');

let downlaodAllFn = {
  'OpenDataSoft': opendatasoft.downloadAll,
  'ArcGIS Open Data': arcgis.downloadAll,
  'Socrata': socrata.downloadAll,
  'CKAN': ckan.downloadAll,
  'Junar': junar.downloadAll,
  'GeoNode': geonode.downloadAll,
  'DKAN': dkan.downloadAll
};

/**
 * Harvest the dataset metadata from one platform and save into the database.
 * @param  {String}         platform      platform name
 * @return {Rx.Observable}                no return
 */
export function harvest(platform) {
  let downloadAll = downlaodAllFn[platform];

  if (!downloadAll) {
    return Rx.Observable.throw(new Error(`Platform ${platform} is unknown.`));
  }

  return downloadAll()
    .mergeMap((metadatas) => save(metadatas), 1);
}

/**
 * Harvest the dataset metadata from all platforms and save into the database.
 * @return {Rx.Observable}              no return
 */
export function harvestAll() {
  let db = getDB();

  return db.query('SELECT name FROM platform')
    .mergeMap((platform) => {
      return harvest(platform.name).catch((error) => {
        logger.error(`Unable to download data from ${platform.name}`, error);
        return Rx.Observable.empty();
      });
    }, 1);
}
