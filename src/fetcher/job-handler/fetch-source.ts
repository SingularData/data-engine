import AWS = require("aws-sdk");
import * as _ from "lodash";
import * as sourceHandlers from "../source-handler";

AWS.config.region = "us-east-1";

export function fetchSource(job, aws) {
  return sourceHandlers[job.type.toLowerCase()].getPageUrls(job).then(urls => {
    if (urls.length === 0) {
      throw new Error("No url is found for " + job.url);
    }

    const sns = new AWS.SNS();
    const tasks = [];

    for (let url of urls) {
      console.log(`Publishing fetch task for data source page: ${job.name}.`);

      const sourcePage = _.assign({}, job, {
        messageType: "FetchPage",
        url
      });

      const task = sns
        .publish({
          Message: JSON.stringify(sourcePage),
          TopicArn: process.env.SNS_FETCH_QUEUE
        })
        .promise()
        .then(() =>
          console.log(`Published fetch task for data source page: ${job.name}.`)
        )
        .catch(err => {
          console.log(`Failed to publish fetch page task for ${job.name}.`);
          console.error(err);
        });

      tasks.push(task);
    }

    return Promise.all(tasks);
  });
}
