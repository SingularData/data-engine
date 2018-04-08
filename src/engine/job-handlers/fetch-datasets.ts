import AWS = require("aws-sdk");
import _ = require("lodash");
import uuid = require("uuid/v1");
import { FetchDatasetJob } from "../classes/FetchDatasetJob";
import { UpdateIndexJob } from "../classes/UpdateIndexJob";
import { chunkBySize } from "../utils";
import * as sources from "../utils/sources";

export async function fetchDatasets(queue, job: FetchDatasetJob) {
  const sourceType = job.data.sourceType.toLowerCase();
  const datasets = await sources[sourceType].getDatasets(job.data);

  if (datasets.length === 0) {
    console.log("No dataset is found for " + job.data.url);
    return;
  }

  const chunks = chunkBySize(datasets, 64000);
  const jobs = _.map(chunks, chunk => new UpdateIndexJob(chunk as any[]));

  for (const chunk of _.chunk(jobs, 10)) {
    await queue.push(chunk);
  }
}
