export function ensureIndex(es, index) {
  return es.indices.exists({ index }).then(exists => {
    if (!exists) {
      return es.indices.create({ index });
    }
  });
}

export function indexDatasets(es, datasets) {
  const body = [];

  for (let dataset of datasets) {
    const action = {
      index: {
        _index: process.env.ES_INDEX,
        _type: dataset.type,
        _id: dataset.dcat.identifier
      }
    };

    body.push(action, dataset);
  }

  return es.bulk({ body });
}

export function saveChecksum(dynamodb, datasets) {
  const params = {
    RequestItems: {}
  };

  params.RequestItems[process.env.DYNAMODB_CHECKSUM] = [];

  for (let dataset of datasets) {
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
}
