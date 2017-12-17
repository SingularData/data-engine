import es = require("elasticsearch");
import fse = require("fs-extra");
import Rx = require("rxjs/Rx");
import * as config from "config";
import { flatMap } from "lodash";

const index = config.get("elasticsearch.index");

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
  return Rx.Observable.create(observer => {
    const client = getClient();
    const datasets = [];

    client.search(
      {
        index,
        scroll: "30s",
        body: {
          query: {
            match_all: {}
          }
        },
        size: 1000,
        _source: ["checksum"]
      },
      function getMoreUntilDone(error, response) {
        if (error) {
          return observer.error(error);
        }

        datasets.push(...response.hits.hits);

        if (response.hits.total > datasets.length) {
          client.scroll(
            {
              scrollId: response._scroll_id,
              scroll: "30s"
            },
            getMoreUntilDone
          );
        } else {
          const checksumMap = datasets.reduce((map, dataset) => {
            map[dataset._id] = dataset._source.checksum;
            return map;
          }, {});

          observer.next(checksumMap);
          observer.complete();
        }
      }
    );
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
    const body = [];

    for (let dataset of datasets) {
      const action = {
        index: {
          _index: index,
          _type: dataset.type,
          _id: dataset.dcat.identifier
        }
      };

      body.push(action, dataset);
    }

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
