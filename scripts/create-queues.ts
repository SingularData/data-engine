import AWS = require("aws-sdk");
import env = require("dotenv");

env.config();

AWS.config.region = "us-east-1";

const sns = new AWS.SNS();

Promise.all([
  sns.createTopic({ Name: "sdn-pipeline-fetch-queue" }).promise(),
  sns.createTopic({ Name: "sdn-pipeline-index-queue" }).promise()
])
  .then(() => console.log("Create success!"))
  .catch(err => console.error(err));
