"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const AWS = require("aws-sdk");
const _ = require("lodash");
const arcgis = require("./source-handler/arcgis");
const ckan = require("./source-handler/ckan");
const dkan = require("./source-handler/dkan");
const geonode = require("./source-handler/geonode");
const junar = require("./source-handler/junar");
const opendatasoft = require("./source-handler/opendatasoft");
const socrata = require("./source-handler/socrata");
const util_1 = require("./util");
const sourceHandlers = {
    arcgis,
    ckan,
    dkan,
    geonode,
    junar,
    opendatasoft,
    socrata
};
AWS.config.region = "us-east-1";
exports.fetch = (event, context) => {
    const source = JSON.parse(event.Records[0].Sns.Message);
    const sns = new AWS.SNS();
    switch (source.messageType) {
        case "FetchSource":
            console.log(`Received fetch source task for: ${source.name}.`);
            return sourceHandlers[source.type.toLowerCase()]
                .getPageUrls(source)
                .then(urls => {
                const tasks = [];
                for (let url of urls) {
                    console.log(`Publishing fetch task for data source page: ${source.name}.`);
                    const sourcePage = _.assign({}, source, {
                        messageType: "FetchPage",
                        url
                    });
                    const task = sns
                        .publish({
                        Message: JSON.stringify(sourcePage),
                        TopicArn: process.env.SNS_FETCH_QUEUE
                    })
                        .promise()
                        .then(() => console.log(`Published fetch task for data source page: ${source.name}.`))
                        .catch(err => {
                        console.log(`Failed to publish fetch page task for ${source.name}.`);
                        console.error(err);
                    });
                    tasks.push(task);
                }
                return Promise.all(tasks);
            })
                .then(() => context.done(null, "Finished fetch source task for: ${source.url}."))
                .catch(err => context.done(err));
        case "FetchPage":
            console.log(`Received fetch page task for: ${source.name}.`);
            const dynamodb = new AWS.DynamoDB();
            return sourceHandlers[source.type.toLowerCase()]
                .fetchPage(source)
                .then(datasets => util_1.deduplicate(dynamodb, datasets))
                .then(datasets => {
                const tasks = [];
                for (let dataset of datasets) {
                    console.log(`Publishing index task for dataset: ${source.name}.`);
                    const task = sns
                        .publish({
                        Message: JSON.stringify(dataset),
                        TopicArn: process.env.SNS_INDEX_QUEUE
                    })
                        .promise()
                        .then(() => console.log(`Published index task for dataset: ${source.name}.`))
                        .catch(err => {
                        console.log(`Failed to publish index task for ${source.name}.`);
                        console.error(err);
                    });
                    tasks.push(task);
                }
                return Promise.all(tasks);
            })
                .then(() => context.done(null, `Finished fetch page task for: ${source.url}.`))
                .catch(err => context.done(err));
    }
};
