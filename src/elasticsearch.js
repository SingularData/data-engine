const es = require('elasticsearch');

import config from 'config';
import { resolve } from 'path';
import { readFileSync } from 'fs';
import { flatMap, omit } from 'lodash';
import { Observable } from 'rxjs';
import { getDB } from './database';

let cocurrency = config.get('cocurrency');
let currentClient;

/**
 * Get the ElasticSearch client based on the application environment.
 * @returns {Client}  ElasticSearch Client
 */
export function getClient() {

  if (currentClient) {
    return currentClient;
  }

  if (process.env.NODE_ENV === 'production') {
    const awsES = require('http-aws-es');
    const AWS = require('aws-sdk');

    AWS.config.update({
      region: 'us-east-1',
      credentials: new AWS.Credentials(
        config.get('elasticsearch.accessKey'),
        config.get('elasticsearch.secretKey')
      )
    });

    currentClient = new es.Client({
      hosts: [config.get('elasticsearch.host')],
      connectionClass: awsES
    });
  } else {
    currentClient = new es.Client({
      host: config.get('elasticsearch.host')
    });
  }

  return currentClient;
}

/**
 * Update or insert (if not exists) datasets
 * @param   {Object[]}    datasets dataset metadatas
 * @returns {Observable}           empty observable
 */
export function upsert(datasets) {
  let client = getClient();
  let body = flatMap(datasets, (dataset) => {
    let action = {
      index: {
        _index: 'datarea',
        _type: 'metadata',
        _id: dataset.identifier
      }
    };

    let source = omit(dataset,
      'identifier',
      'portalId',
      'raw',
      'spatial',
      'version',
      'versionPeriod'
    );

    return [action, source];
  });

  return Observable.fromPromise(client.bulk({ body }));
}

/**
 * Clear all dataset metadata index
 * @param   {String} [index='datarea'] data index
 * @returns {Observable} empty observable
 */
export function clear(index = 'datarea') {
  return Observable.defer(() => {
    let client = getClient();
    let task = client.indices.exists({ index })
      .then((exists) => {
        if (exists) {
          return client.indices.delete({ index });
        }
      });

    return task;
  });
}

/**
 * Clear all indexes and rebuild the indexes.
 * @returns {Observable} empty observable
 */
export function reindex() {
  return clear()
    .mergeMap(() => {
      let db = getDB();
      let sql = readFileSync(resolve(__dirname, 'queries/get_latest_data.sql'), 'utf8');
      return db.query(sql);
    })
    .bufferCount(config.get('database.insert_limit'))
    .concatMap((datasets) => upsert(datasets), cocurrency);
}

/**
 * Ensure search index exists. If not, create it.
 * @param   {String} [index='datarea'] data index
 * @returns {undefined}                no return
 */
export function ensureIndex(index = 'datarea') {
  return Observable.defer(() => {
    let client = getClient();
    let task = client.indices.exists({ index })
      .then((exists) => {
        if (!exists) {
          return client.indices.create({ index });
        }
      });

    return task;
  });
}
