import AWS = require("aws-sdk");
import * as _ from "lodash";
import * as jobHandler from "./job-handler";

AWS.config.region = "us-east-1";

export function fetch(event, context, callback) {
  const job = JSON.parse(event.Records[0].Sns.Message);
  const sns = new AWS.SNS();

  switch (job.messageType) {
    case "FetchSource":
      console.log(`Received fetch source task for: ${job.name}.`);

      return jobHandler
        .fetchSource(job, { sns })
        .then(() => console.log(`Finished fetch source task for: ${job.url}.`))
        .then(() => callback())
        .catch(err => callback(err));
    case "FetchPage":
      console.log(`Received fetch page task for: ${job.name}.`);

      const dynamodb = new AWS.DynamoDB();

      return jobHandler
        .fetchPage(job, { sns, dynamodb })
        .then(() => console.log(`Finished fetch page task for: ${job.url}.`))
        .then(() => callback())
        .catch(err => callback(err));
  }
}
