import Queue from 'promise-queue';
import Promise from 'bluebird';
import _ from 'lodash';
import config from 'config';
import { getDB, save } from './database';

import * as opendatasoft from './platforms/opendatasoft';
import * as arcgis from './platforms/arcgis';
import * as socrata from './platforms/socrata';
import * as ckan from './platforms/ckan';
import * as junar from './platforms/junar';
import * as geonode from './platforms/geonode';
import * as dkan from './platforms/dkan';

Queue.configure(Promise);

const maxConcurrent = config.get('harvester.concurrent');

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
 * @param  {Function}       [onEach]      callback function called at each task
 * @param  {Function}       [onError]     callback function called at each task failure
 * @param  {Function}       [onComplete]  callback function called when all tasks are completed
 * @return {Promise<null>}                no return
 */
export function harvest(platform, onEach, onError, onComplete) {
  let downloadAll = downlaodAllFn[platform];

  if (!downloadAll) {
    return Promise.reject(new Error(`Platform ${platform} is unknown.`));
  }

  return downloadAll(true)
    .then(tasks => {
      let queue = new Queue(maxConcurrent, Infinity, {
        onEmpty: onComplete
      });

      _.forEach(tasks, task => {
        queue.add(() => {
          if (onEach) {
            onEach(platform);
          }

          return task().then(metadatas => {
            return save(metadatas);
          })
          .catch(error => {
            if (onError) {
              onError(platform, error);
            }

            console.error(`Have problem harvesting\n${platform}`, error);
          });
        });
      });
    });
}

/**
 * Harvest the dataset metadata from all platforms and save into the database.
 * @param  {Function}       [onEach]      callback function called at each task
 * @param  {Function}       [onError]     callback function called at each task failure
 * @param  {Function}       [onComplete]  callback function called when all tasks are completed
 * @return {Promise<null>}              no return
 */
export function harvestAll(onEach, onError, onComplete) {
  let db = getDB();

  return db.any('SELECT name FROM platform')
    .then(results => {
      let tasks = [];
      let queue = new Queue(maxConcurrent, Infinity, {
        onEmpty: onComplete
      });

      for(let i = 0, n = results.length; i < n; i++) {
        let platform = results[i].name;
        let downloadAll = downlaodAllFn[platform];

        if (!downloadAll) {
          continue;
        }

        let task = downloadAll()
          .then(tasks => {
            _.forEach(tasks, task => {
              queue.add(() => {
                if (onEach) {
                  onEach(platform);
                }

                return task().then(metadatas => {
                  return save(metadatas);
                })
                .catch(error => {
                  if (onError) {
                    onError(platform, error);
                  }

                  console.error(`Have problem harvesting\n${platform}`, error);
                });
              });
            });
          });

        tasks.push(task);
      }

      return Promise.all(tasks);
    });
}
