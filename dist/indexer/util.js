"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
function ensureIndex(es, index) {
    return es.indices.exists({ index }).then(exists => {
        if (!exists) {
            return es.indices.create({ index });
        }
    });
}
exports.ensureIndex = ensureIndex;
function indexDataset(es, dataset) {
    const params = {
        index: process.env.ES_INDEX,
        type: dataset.type,
        _id: dataset.dcat.identifier,
        body: dataset
    };
    return es.index(params);
}
exports.indexDataset = indexDataset;
function saveChecksum(dynamodb, dataset) {
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
    return dynamodb
        .putItem(params)
        .promise();
}
exports.saveChecksum = saveChecksum;
