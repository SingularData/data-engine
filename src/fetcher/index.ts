import * as _ from "lodash";
import * as jobHandler from "./job-handler";

export function fetch(event, context, callback) {
  const job = JSON.parse(event.Records[0].Sns.Message);

  switch (job.messageType) {
    case "FetchSource":
      console.log(`Received fetch source task for: ${job.name}.`);

      return jobHandler
        .fetchSource(job)
        .then(() => console.log(`Finished fetch source task for: ${job.url}.`))
        .then(() => callback())
        .catch(err => callback(err));
    case "FetchPage":
      console.log(`Received fetch page task for: ${job.name}.`);

      return jobHandler
        .fetchPage(job)
        .then(() => console.log(`Finished fetch page task for: ${job.url}.`))
        .then(() => callback())
        .catch(err => callback(err));
    default:
      callback(new Error(`Unrecognized job type: ${job.messageType}`));
  }
}
