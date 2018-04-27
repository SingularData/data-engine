import AWS = require("aws-sdk");
import _ = require("lodash");
import uuid = require("uuid/v1");
import { FetchDatasetJob } from "../classes/FetchDatasetJob";
import { UpdateIndexJob } from "../classes/UpdateIndexJob";
import { chunkBySize } from "../utils";

const MAX_SQS_MESSAGE_SIZE = 10 * 1024;

export async function fetchDatasets(
  getDatasets,
  datasetExists,
  pushToQueue,
  job: FetchDatasetJob
) {
  const datasets = await getDatasets(job.data);

  if (datasets.length === 0) {
    return;
  }

  const newDatasets = datasets.filter(
    async dataset => await !datasetExists(dataset)
  );

  if (newDatasets.length === 0) {
    return;
  }

  const chunks = chunkBySize(newDatasets, MAX_SQS_MESSAGE_SIZE);
  const jobs = _.map(chunks, chunk => new UpdateIndexJob(chunk as any[]));

  for (const chunk of _.chunk(jobs, 10)) {
    await pushToQueue(chunk);
  }
}
