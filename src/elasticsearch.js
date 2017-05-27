const es = require('elasticsearch');
const awsES = require('http-aws-es');

import config from 'config';
import { flatMap, omit } from 'lodash';
import { Observable } from 'rxjs';

let currentClient;

/**
 * Get the ElasticSearch client based on the application environment.
 */
export function getClient() {

  if (currentClient) {
    return currentClient;
  }

  if (process.env.NODE_ENV === 'production') {
    currentClient = new es.Client({
      hosts: config.get('elasticsearch.aws.host'),
      connectionClass: awsES,
      amazonES: {
        region: config.get('elasticsearch.aws.region'),
        accessKey: config.get('elasticsearch.aws.accessKey'),
        secretKey: config.get('elasticsearch.aws.secretKey')
      }
    });
  } else {
    currentClient = new es.Client({
      host: config.get('elasticsearch.development.host')
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

    let body = {
      upsert: omit(dataset, 'uuid', 'portalID', 'raw', 'versionNumber', 'versionPeriod')
    };

    return [action, body];
  });

  return Observable.fromPromise(client.bulk({ body }));
}

/**
 * Clear all dataset metadata index
 * @returns {Observable} empty observable
 */
export function clear() {
  let client = getClient();
  let configs = {
    index: 'datarea',
    type: 'metadata',
    body: {
      query: {
        match_all: {}
      }
    }
  };

  return Observable.fromPromise(client.deleteByQuery(configs));
}
