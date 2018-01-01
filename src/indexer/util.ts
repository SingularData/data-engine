export function ensureIndex(es, index) {
  return es.indices.exists({ index }).then(exists => {
    if (!exists) {
      return es.indices.create({ index });
    }
  });
}

export function indexDataset(es, dataset) {
  const params = {
    index: process.env.ES_INDEX,
    type: dataset.type,
    _id: dataset.dcat.identifier,
    body: dataset
  };

  return es.index(params);
}

export function saveChecksum(dynamodb, dataset) {
  const params = {
    Item: {
      identifier: {
        S: dataset.dcat.identifier
      },
      checksum: {
        S: dataset.checksum
      }
    },
    TableName: "sdn-dataset-checksum"
  };

  return dynamodb.putItem(params).promise();
}
