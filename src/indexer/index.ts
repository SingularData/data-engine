import AWS = require("aws-sdk");
import es = require("elasticsearch");
import awsES = require("http-aws-es");
import { indexDatasets, saveChecksum } from "./util";

AWS.config.region = "us-east-1";

export function index(event, context, callback) {
  const datasets = JSON.parse(event.Records[0].Sns.Message);
  const client = new es.Client({
    hosts: [process.env.ES_URL],
    connectionClass: awsES
  });
  const dynamodb = new AWS.DynamoDB();

  return indexDatasets(client, datasets)
    .then(() => saveChecksum(dynamodb, datasets))
    .then(() => callback())
    .catch(error => callback(error));
}
