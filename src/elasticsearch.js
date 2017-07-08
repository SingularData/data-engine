const es = require('elasticsearch');
const awsES = require('http-aws-es');

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
    currentClient = new es.Client({
      hosts: config.get('elasticsearch.host'),
      connectionClass: awsES,
      amazonES: {
        region: config.get('elasticsearch.region'),
        accessKey: config.get('elasticsearch.accessKey'),
        secretKey: config.get('elasticsearch.secretKey')
      }
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
        _id: dataset.uuid
      }
    };
    let source = omit(dataset,
      'uuid',
      'portalId',
      'portalDatasetId',
      'raw',
      'version',
      'versionPeriod'
    );

    return [action, source];
  });

  return Observable.fromPromise(client.bulk({ body }));
}

/**
 * Clear all dataset metadata index
 * @returns {Observable} empty observable
 */
export function clear() {
  let client = getClient();

  return Observable.fromPromise(client.indices.delete({
    index: 'datarea'
  }));
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
    .mergeMap((datasets) => upsert(datasets), cocurrency);
}
