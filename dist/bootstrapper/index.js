"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const AWS = require("aws-sdk");
AWS.config.region = "us-east-1";
exports.bootstrap = (event, context) => {
    console.log("Start publishing data request tasks.");
    const dynamodb = new AWS.DynamoDB();
    const sns = new AWS.SNS();
    getSources(dynamodb)
        .then((list) => {
        const tasks = [];
        for (let source of list) {
            console.log(`Publishing request task for data source ${source.name}.`);
            source.messageType = "FetchSource";
            const task = sns
                .publish({
                Message: JSON.stringify(source),
                TopicArn: process.env.SNS_FETCH_QUEUE
            })
                .promise()
                .then(() => console.log(`Published request task for data source ${source.name}.`))
                .catch(err => {
                console.log(`Failed to publish task for ${source.name}.`);
                console.error(err);
            });
            tasks.push(task);
        }
        return Promise.all(tasks);
    })
        .then(() => context.done(null, "Published task!"))
        .catch(err => context.done(err));
};
function getSources(dynamodb, start) {
    return __awaiter(this, void 0, void 0, function* () {
        try {
            const params = {
                TableName: process.env.DYNAMODB_TABLE,
                ExclusiveStartKey: start
            };
            const sources = [];
            const data = yield dynamodb.scan(params).promise();
            if (data.Count === 0) {
                return sources;
            }
            for (let item of data.Items) {
                const source = {};
                for (let key in item) {
                    if (!item.hasOwnProperty(key)) {
                        continue;
                    }
                    source[key] = item[key].S;
                }
                sources.push(source);
            }
            if (data.LastEvaluatedKey) {
                const rest = yield getSources(dynamodb, data.LastEvaluatedKey);
                sources.push(...rest);
            }
            return sources;
        }
        catch (err) {
            return Promise.reject(err);
        }
    });
}
