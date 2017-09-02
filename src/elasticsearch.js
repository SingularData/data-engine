const es = require('elasticsearch');
const QueryStream = require('pg-query-stream');

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
  let clearData = clear().catch(() => Observable.empty());

  let stream;
  let db = getDB('pg-pool');
  let insertData = Observable.defer(() => db.connect())
    .concatMap((client) => {
      return Observable.create((observer) => {
        let sql = readFileSync(resolve(__dirname, 'queries/get_latest_data.sql'), 'utf8');

        stream = client.query(new QueryStream(sql));
        stream.on('data', (row) => observer.next(row));
        stream.on('error', (error) => observer.error(error));
        stream.on('end', () => observer.complete());

        return client.release;
      });
    })
    .bufferCount(config.get('database.insert_limit'))
    .do(() => stream.pause())
    .mergeMap((datasets) => upsert(datasets), cocurrency)
    .do(() => stream.resume());

  return Observable.concat(clearData, insertData);
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
      .catch(() => client.indexes.create({ index }));

    return task;
  });
}
