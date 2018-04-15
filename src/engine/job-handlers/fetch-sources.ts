import AWS = require("aws-sdk");
import _ = require("lodash");
import uuid = require("uuid/v1");
import { FetchSourceJob } from "../classes/FetchSourceJob";
import { FetchDatasetJob } from "../classes/FetchDatasetJob";
import * as sources from "../utils/sources";

export async function fetchSources(pushToQueue, job: FetchSourceJob) {
  const sourceType = job.data.sourceType.toLowerCase();
  const urls = await sources[sourceType].getSourceUrls(job.data);

  if (urls.length === 0) {
    console.log("No source url is found for " + job.data.url);
    return;
  }

  const chunks = _.chunk(urls, 10);

  for (const chunk of chunks) {
    const jobs = [];

    for (const url of chunk) {
      jobs.push(new FetchDatasetJob(job.data.sourceType, url as string));
    }

    await pushToQueue(jobs);
  }
}
