import { Observable } from 'rxjs';
import { save, getDB, getLatestCheckList } from './database';
import { dateToString } from './utils/pg-util';
import log4js from 'log4js';
import config from 'config';
import md5 from 'md5';

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
 * @return {Observable}                   no return
 */
export function harvest(platform) {
  let downloadAll = downlaodAllFn[platform];

  if (!downloadAll) {
    return Observable.throw(new Error(`Platform ${platform} is unknown.`));
  }

  let dataCache;
  let getDataChecklist = getLatestCheckList(platform).mergeMap((checklist) => {
    dataCache = checklist;
    return Observable.empty();
  });

  return Observable.concat(getDataChecklist, downloadAll())
    .map((dataset) => {
      let key = `${dataset.portalID}:${dataset.portalDatasetID}`;
      let existing = dataCache[key];

      if (!existing) {
        let createTime = dataset.createTime || new Date(dataset.updatedTime.getTime() - 1);

        dataset.versionNumber = 1;
        dataset.versionPeriod = `[${dateToString(createTime)},)`;
      } else if (existing.md5 === md5(JSON.stringify(dataset.raw))) {
        delete dataCache[key];
        return null;
      } else {
        delete dataCache[key];
        dataset.versionNumber = existing.version + 1;
        dataset.versionPeriod = `[${dateToString(dataset.updatedTime)},)`;
      }

      return dataset;
    })
    .catch((err) => {
      logger.error(`Error of data processing at ${platform}`, err);
      return Observable.empty();
    })
    .filter((dataset) => dataset !== null)
    .bufferCount(config.get('database.insert_limit'))
    .concatMap((datasets) => save(datasets));
}

/**
 * Harvest the dataset metadata from all platforms and save into the database.
 * @return {Observable}              no return
 */
export function harvestAll() {
  let db = getDB();

  return db.query('SELECT name FROM platform')
    .concatMap((platform) => {
      return harvest(platform.name).catch((error) => {
        logger.error(`Unable to download data from ${platform.name}`, error);
        return Observable.empty();
      });
    });
}
