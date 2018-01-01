"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const AWS = require("aws-sdk");
const es = require("elasticsearch");
const awsES = require("http-aws-es");
const util_1 = require("./util");
AWS.config.region = "us-east-1";
exports.index = (event, context) => {
    const dataset = JSON.parse(event.Records[0].Sns.Message);
    const client = new es.Client({
        hosts: [process.env.ES_URL],
        connectionClass: awsES
    });
    const dynamodb = new AWS.DynamoDB();
    return util_1.ensureIndex(client, process.env.ES_INDEX)
        .then(() => util_1.indexDataset(client, dataset))
        .then(() => util_1.saveChecksum(dynamodb, dataset))
        .catch(error => {
        console.error("Unable to index datase: ", error);
    });
};
