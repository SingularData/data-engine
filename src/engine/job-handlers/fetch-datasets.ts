import AWS = require("aws-sdk");
import _ = require("lodash");
import uuid = require("uuid/v1");
import { FetchDatasetJob } from "../classes/FetchDatasetJob";
import { UpdateIndexJob } from "../classes/UpdateIndexJob";
import { chunkBySize } from "../utils";
import * as sources from "../utils/sources";

export async function fetchDatasets(
  pushToQueue,
  datasetExists,
  job: FetchDatasetJob
) {
  const sourceType = job.data.sourceType.toLowerCase();
  const datasets = await sources[sourceType].getDatasets(job.data);

  if (datasets.length === 0) {
    return;
  }

  const newDatasets = datasets.filter(dataset => !datasetExists(dataset));

  if (newDatasets.length === 0) {
    return;
  }

  const chunks = chunkBySize(newDatasets, 64000);
  const jobs = _.map(chunks, chunk => new UpdateIndexJob(chunk as any[]));

  for (const chunk of _.chunk(jobs, 10)) {
    await pushToQueue(chunk);
  }
}
