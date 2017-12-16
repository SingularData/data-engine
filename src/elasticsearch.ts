import es = require("elasticsearch");
import fse = require("fs-extra");
import Rx = require("rxjs/Rx");
import * as config from "config";
import { flatMap } from "lodash";

const index = config.get("elasticsearch.index");
const type = config.get("elasticsearch.type");

let currentClient;

/**
 * Get the ElasticSearch client based on the application environment.
 * @returns {Client}  ElasticSearch Client
 */
export function getClient() {
  if (currentClient) {
    return currentClient;
  }

  if (process.env.NODE_ENV === "production") {
    const awsES = require("http-aws-es");
    const AWS = require("aws-sdk");

    AWS.config.update({
      region: "us-east-1",
      credentials: new AWS.Credentials(
        config.get("elasticsearch.accessKey"),
        config.get("elasticsearch.secretKey")
      )
    });

    currentClient = new es.Client({
      hosts: [config.get("elasticsearch.url")],
      connectionClass: awsES
    });
  } else {
    currentClient = new es.Client({
      host: config.get("elasticsearch.url")
    });
  }

  return currentClient;
}

/**
 * Get a checksum map whose keys are dataset id.
 * @return {Observable} an observable with checksum map
 */
export function getChecksumMap() {
  return Rx.Observable.defer(() => {
    const client = getClient();
    const params = {
      index,
      _sourceInclude: ["checksum"],
      body: {
        query: {
          match_all: {}
        }
      }
    };

    return client.search(params).then(result => {
      const checksumMap = {};

      for (let dataset of result.hits.hits) {
        checksumMap[dataset._id] = dataset._source.checksum;
      }

      return checksumMap;
    });
  });
}

/**
 * Update or insert (if not exists) datasets
 * @param   {Object[]}    datasets dataset metadatas
 * @returns {Observable}           empty observable
 */
export function upsert(datasets) {
  return Rx.Observable.defer(() => {
    const client = getClient();
    const body = flatMap(datasets, dataset => {
      const action = {
        index: {
          _index: index,
          _type: type,
          _id: dataset.dcat.identifier
        }
      };

      return [action, dataset];
    });

    return client.bulk({ body });
  }).catch(err => {
    console.error(err);
    return Rx.Observable.empty();
  });
}

/**
 * Clear all dataset metadata index
 * @returns {Observable} empty observable
 */
export function deleteIndex() {
  return Rx.Observable.defer(() => {
    const client = getClient();
    const task = client.indices.exists({ index }).then(exists => {
      if (exists) {
        return client.indices.delete({ index });
      }
    });

    return task;
  });
}

/**
 * Ensure search index exists. If not, create it.
 * @returns {undefined} no return
 */
export function ensureIndex() {
  return Rx.Observable.defer(() => {
    const client = getClient();
    const task = client.indices.exists({ index }).then(exists => {
      if (!exists) {
        return client.indices.create({ index });
      }
    });

    return task;
  });
}

/**
 * Check whether an index exists.
 * @return {boolean} a boolean
 */
export function indexExists() {
  return Rx.Observable.defer(() => {
    const client = getClient();
    return client.indices.exists({ index });
  });
}
