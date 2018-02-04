import * as _ from "lodash";

export function indexDatasets(es, datasets) {
  const body = [];

  for (let dataset of datasets) {
    const action = {
      index: {
        _index: process.env.ES_INDEX,
        _type: process.env.ES_DOC_TYPE,
        _id: dataset.dcat.identifier
      }
    };

    body.push(action, dataset);
  }

  return es.bulk({ body });
}

export function saveChecksum(dynamodb, datasets) {
  const tasks = _.chain(datasets)
    .chunk(25)
    .map(chunk => {
      const params = {
        RequestItems: {}
      };

      params.RequestItems[process.env.DYNAMODB_CHECKSUM] = [];

      for (let dataset of chunk) {
        const item = {
          PutRequest: {
            Item: {
              identifier: {
                S: dataset.dcat.identifier
              },
              checksum: {
                S: dataset.checksum
              }
            }
          }
        };

        params.RequestItems[process.env.DYNAMODB_CHECKSUM].push(item);
      }

      return dynamodb.batchWriteItem(params).promise();
    })
    .value();

  return Promise.all(tasks);
}
