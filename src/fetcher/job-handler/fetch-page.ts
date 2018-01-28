import * as _ from "lodash";
import * as sourceHandlers from "../source-handler";
import { deduplicate, stringPlunk } from "../util";

export function fetchPage(job, aws) {
  return sourceHandlers[job.type.toLowerCase()]
    .fetchPage(job)
    .then(datasets => deduplicate(aws.dynamodb, datasets))
    .then(datasets => stringPlunk(datasets, 256 * 1000))
    .then(plunks => {
      const tasks = [];

      for (let plunk of plunks) {
        console.log(`Publishing index task for dataset: ${job.name}.`);

        const task = aws.sns
          .publish({
            Message: plunk,
            TopicArn: process.env.SNS_INDEX_QUEUE
          })
          .promise()
          .then(() =>
            console.log(`Published index task for dataset: ${job.name}.`)
          )
          .catch(err => {
            console.log(`Failed to publish index task for ${job.name}.`);
            console.error(err);
          });

        tasks.push(task);
      }

      return Promise.all(tasks);
    });
}
