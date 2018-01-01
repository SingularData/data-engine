import AWS = require("aws-sdk");
import es = require("elasticsearch");
import awsES = require("http-aws-es");
import { ensureIndex, indexDataset, saveChecksum } from "./util";

AWS.config.region = "us-east-1";

exports.index = (event, context) => {
  const dataset = JSON.parse(event.Records[0].Sns.Message);
  const client = new es.Client({
    hosts: [process.env.ES_URL],
    connectionClass: awsES
  });
  const dynamodb = new AWS.DynamoDB();

  return ensureIndex(client, process.env.ES_INDEX)
    .then(() => indexDataset(client, dataset))
    .then(() => saveChecksum(dynamodb, dataset))
    .catch(error => {
      console.error("Unable to index datase: ", error);
    });
};
