"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const crypto_1 = require("crypto");
const w3c_dcat_1 = require("w3c-dcat");
const _ = require("lodash");
function deduplicate(dynamodb, datasets) {
    const filtered = [];
    const tasks = [];
    const params = {
        TableName: "sdn-dataset-checksum",
        Key: {
            identifier: {
                S: ''
            }
        },
        ConsistentRead: true
    };
    for (let dataset of datasets) {
        params.Key.identifier.S = dataset.dcat.identifier;
        const task = dynamodb
            .getItem(params)
            .promise()
            .then(data => {
            if (_.get(data, "Item.checksum") !== dataset.checksum) {
                filtered.push(dataset);
            }
        })
            .catch(err => {
            console.error(err);
            console.log("Unable to check duplication for item: ", dataset.dcat);
        });
        tasks.push(task);
    }
    return Promise.all(tasks).then(() => filtered);
}
exports.deduplicate = deduplicate;
function wrapDataset(type, dataset) {
    return {
        type,
        dcat: w3c_dcat_1.Dataset.from(type, dataset).toJSON(),
        checksum: sha256(JSON.stringify(dataset)),
        original: dataset
    };
}
exports.wrapDataset = wrapDataset;
function sha256(data) {
    const hash = crypto_1.createHash("sha256");
    hash.update(data);
    return hash.digest("base64");
}
