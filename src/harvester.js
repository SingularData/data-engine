import { Observable } from 'rxjs';
import { save, getDB, getLatestCheckList, refreshDatabase } from './database';
import { upsert } from './elasticsearch';
import { dateToString } from './utils/pg-util';
import { defaults } from 'lodash';
import { createHash } from 'crypto';
import { encode } from 'multihashes';
import log4js from 'log4js';
import config from 'config';

import * as opendatasoft from './platforms/opendatasoft';
import * as arcgis from './platforms/arcgis';
import * as socrata from './platforms/socrata';
import * as ckan from './platforms/ckan';
import * as junar from './platforms/junar';
import * as geonode from './platforms/geonode';
import * as dkan from './platforms/dkan';

const cryptoFunc = config.get('hash.crypto');
const multihashFunc = config.get('hash.multihashes');
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

const harvestOptions = {
  updateES: true,
  refreshDB: true
};

/**
 * Harvest data for a given portal.
 * @param   {String}      platform            platform name
 * @param   {String}      portal              portal name
 * @param   {Object}      [options]           options
 * @param   {Boolean}     [options.refreshDB=true] a boolean value indicating whether
 *                                                 to refresh database after data
 *                                                 harvesting
 * @param   {boolean}     [options.updateES=true]  a boolean value indicating whether
 *                                                 to update ElasticSearch at the
 *                                                 same time
 * @returns {Observable}                      empty observable
 */
export function harvestPortal(platform, portal, options = {}) {
  let downloadPortal = downloadPortalFn[platform];

  options = defaults(options, harvestOptions);

  if (!downloadPortal) {
    return Observable.throw(new Error(`Platform ${platform} is unknown.`));
  }

  let dataCache;
  let getDataChecklist = getLatestCheckList(platform).do((checklist) => {
    dataCache = checklist;
  });

  let downloadData = downloadPortal(portal)
    .map((dataset) => filterDataset(dataset, dataCache))
    .catch((err) => {
      logger.error(`Error of data processing at ${platform}`, err);
      return Observable.empty();
    })
    .filter((dataset) => dataset)
    .bufferCount(config.get('database.insert_limit'));

  if (options.updateES) {
    downloadData = downloadData.concatMap((datasets) => Observable.concat(save(datasets), upsert(datasets)));
  } else {
    downloadData = downloadData.concatMap((datasets) => save(datasets));
  }

  let task = Observable.concat(getDataChecklist, downloadData);

  if (options.refreshDB) {
    task = task.concat(refreshDatabase());
  }

  return task;
}

/**
 * Harvest the dataset metadata from one platform and save into the database.
 * @param  {String}      platform            platform name
 * @param  {Object}      [options]           options
 * @param  {Boolean}     [options.refreshDB=true] a boolean value indicating whether
 *                                                to refresh database after data
 *                                                harvesting
 * @param   {boolean}    [options.updateES=true]  a boolean value indicating whether
 *                                                to update ElasticSearch at the
 *                                                same time
 * @return {Observable}                      empty observable
 */
export function harvestPlatform(platform, options = {}) {
  let downloadAll = downlaodAllFn[platform];

  options = defaults(options, harvestOptions);

  if (!downloadAll) {
    return Observable.throw(new Error(`Platform ${platform} is unknown.`));
  }

  let dataCache;
  let getDataChecklist = getLatestCheckList(platform)
    .do((checklist) => {
      dataCache = checklist;
    });

  let downloadData = downloadAll()
    .map((dataset) => filterDataset(dataset, dataCache))
    .catch((err) => {
      logger.error(`Error of data processing at ${platform}`, err);
      return Observable.empty();
    })
    .filter((dataset) => dataset)
    .bufferCount(config.get('database.insert_limit'))
    .do(() => console.log('comming'));

  if (options.updateES) {
    downloadData = downloadData.concatMap((datasets) => Observable.concat(save(datasets), upsert(datasets)));
  } else {
    downloadData = downloadData.concatMap((datasets) => save(datasets));
  }

  let task = Observable.concat(getDataChecklist, downloadData);

  if (options.refreshDB) {
    task = task.concat(refreshDatabase());
  }

  return task;
}

/**
 * Harvest the dataset metadata from all platforms and save into the database.
 * @param   {object}      [options]                 harvester options
 * @param   {boolean}     [options.refreshDB=true]  a boolean value indicating whether
 *                                                  to refresh database after data
 *                                                  harvesting
 * @param   {boolean}     [options.updateES=true]   a boolean value indicating whether
 *                                                  to update ElasticSearch at the
 *                                                  same time
 * @returns {Observable}                            no return
 */
export function harvestAll(options = {}) {
  let db = getDB();

  options = defaults(options, harvestOptions);

  let platformOptions = {
    refreshDB : false,
    updateES: options.updateES
  };

  let collectData = db.query('SELECT name FROM platform')
    .concatMap((platform) => {
      return harvestPlatform(platform.name, platformOptions)
        .catch((error) => {
          logger.error(`Unable to download data from ${platform.name}`, error);
          return Observable.empty();
        });
    });

  if (options.refreshDB) {
    return Observable.concat(collectData, refreshDatabase());
  }

  return collectData;
}

function filterDataset(dataset, checkList) {
  let key = datasetKey(dataset);

  if (checkList[key]) {
    return;
  }

  dataset.identifier = hash(dataset.raw.toString());
  dataset.version = 2;
  dataset.versionPeriod = `[${dateToString(dataset.updated)},)`;
  checkList[key] = dataset.version;

  return dataset;
}

function datasetKey(dataset) {
  return `${dataset.portl}:${dataset.title}:${hash(dataset.raw.toString())}`;
}

function hash(content) {
  let hasher = createHash(cryptoFunc);
  hasher.update(content);

  return encode(hasher.digest(), multihashFunc).toString('hex');
}
