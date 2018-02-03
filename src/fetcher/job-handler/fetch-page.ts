import * as _ from "lodash";
import * as sourceHandlers from "../source-handler";
import { deduplicate, chunkBySize } from "../util";

export function fetchPage(job, aws) {
  return (
    sourceHandlers[job.type.toLowerCase()]
      .fetchPage(job)
      // .then(datasets => deduplicate(aws.dynamodb, datasets))
      .then(datasets => chunkBySize(datasets, process.env.MAX_SNS_MESSAGE_SIZE))
      .then(chunks => {
        const tasks = [];

        for (let chunk of chunks) {
          console.log(`Publishing index task for dataset: ${job.name}.`);

          const task = aws.sns
            .publish({
              Message: JSON.stringify(chunk),
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
      })
  );
}
