import AWS = require("aws-sdk");
import env = require("dotenv");

env.config();

AWS.config.region = "us-east-1";

const sns = new AWS.SNS();

Promise.all([
  sns.deleteTopic({ TopicArn: process.env.SNS_FETCH_QUEUE }).promise(),
  sns.deleteTopic({ TopicArn: process.env.SNS_INDEX_QUEUE }).promise()
])
  .then(() => console.log("Delete success!"))
  .catch(err => console.error(err));
