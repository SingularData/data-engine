import { Observable } from 'rxjs';
import { save, getDB, getLatestCheckList, refreshDatabase } from './database';
import { upsert } from './elasticsearch';
import { dateToString } from './utils/pg-util';
import uuid from 'uuid';
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

const downlaodAllFn = {
  'OpenDataSoft': opendatasoft.downloadAll,
  'ArcGIS Open Data': arcgis.downloadAll,
  'Socrata': socrata.downloadAllUS,
  'Socrata-EU': socrata.downloadAllEU,
  'CKAN': ckan.downloadAll,
  'Junar': junar.downloadAll,
  'GeoNode': geonode.downloadAll,
  'DKAN': dkan.downloadAll
};

const downloadPortalFn = {
  'OpenDataSoft': opendatasoft.downloadPortal,
  'ArcGIS Open Data': arcgis.downloadPortal,
  'Socrata': socrata.downloadPortal,
  'CKAN': ckan.downloadPortal,
  'Junar': junar.downloadPortal,
  'GeoNode': geonode.downloadPortal,
  'DKAN': dkan.downloadPortal
};

/**
 * Harvest data for a given portal.
 * @param   {String}      platform            platform name
 * @param   {String}      portal              portal name
 * @param   {Object}      [options]           options
 * @param   {Boolean}     [options.refreshDB] a boolean value indicating whether
 *                                            to refresh database after data
 *                                            harvesting
 * @returns {Observable}                      empty observable
 */
export function harvestPortal(platform, portal, options = {}) {
  let downloadPortal = downloadPortalFn[platform];

  if (!downloadPortal) {
    return Observable.throw(new Error(`Platform ${platform} is unknown.`));
  }

  let dataCache;
  let getDataChecklist = getLatestCheckList(platform).mergeMap((checklist) => {
    dataCache = checklist;
    return Observable.empty();
  });

  let observable = Observable.concat(getDataChecklist, downloadPortal(portal))
    .map((dataset) => checkDataset(dataset, dataCache))
    .catch((err) => {
      logger.error(`Error of data processing at ${platform}`, err);
      return Observable.empty();
    })
    .filter((dataset) => dataset !== null)
    .bufferCount(config.get('database.insert_limit'))
    .concatMap((datasets) => Observable.merge(save(datasets), upsert(datasets)));

  if (options.refreshDB === undefined) {
    options.refreshDB = true;
  }

  if (options.refreshDB) {
    observable = observable.concat(refreshDatabase());
  }

  return observable;
}

/**
 * Harvest the dataset metadata from one platform and save into the database.
 * @param  {String}      platform            platform name
 * @param  {Object}      [options]           options
 * @param  {Boolean}     [options.refreshDB] a boolean value indicating whether
 *                                           to refresh database after data
 *                                           harvesting
 * @return {Observable}                      empty observable
 */
export function harvestPlatform(platform, options = {}) {
  let downloadAll = downlaodAllFn[platform];

  if (!downloadAll) {
    return Observable.throw(new Error(`Platform ${platform} is unknown.`));
  }

  let dataCache;
  let getDataChecklist = getLatestCheckList(platform).mergeMap((checklist) => {
    dataCache = checklist;
    return Observable.empty();
  });

  let observable = Observable.concat(getDataChecklist, downloadAll())
    .map((dataset) => checkDataset(dataset, dataCache))
    .catch((err) => {
      logger.error(`Error of data processing at ${platform}`, err);
      return Observable.empty();
    })
    .filter((dataset) => dataset !== null)
    .bufferCount(config.get('database.insert_limit'))
    .concatMap((datasets) => Observable.merge(save(datasets), upsert(datasets)));

  if (options.refreshDB === undefined) {
    options.refreshDB = true;
  }

  if (options.refreshDB) {
    observable = observable.concat(refreshDatabase());
  }

  return observable;
}

/**
 * Harvest the dataset metadata from all platforms and save into the database.
 * @return {Observable}              no return
 */
export function harvestAll() {
  let db = getDB();

  return db.query('SELECT name FROM platform')
    .concatMap((platform) => {
      return harvestPlatform(platform.name, { refreshDB: false }).catch((error) => {
        logger.error(`Unable to download data from ${platform.name}`, error);
        return Observable.empty();
      });
    })
    .concat(refreshDatabase());
}

function checkDataset(dataset, checkList) {
  let key = `${dataset.portalId}:${dataset.portalDatasetId}`;
  let existing = checkList[key];

  if (!existing) {
    dataset.uuid = uuid.v4();
    dataset.version = 2;
    dataset.versionPeriod = `[${dateToString(dataset.updated)},)`;
  } else if (existing.md5 === md5(JSON.stringify(dataset.raw))) {
    delete checkList[key];
    return null;
  } else {
    delete checkList[key];

    dataset.uuid = existing.uuid;
    dataset.version = existing.version + 1;
    dataset.versionPeriod = `[${dateToString(dataset.updated)},)`;
  }

  return dataset;
}
