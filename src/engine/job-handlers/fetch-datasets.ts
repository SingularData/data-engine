import AWS = require("aws-sdk");
import _ = require("lodash");
import uuid = require("uuid/v1");
import { FetchDatasetJob } from "../classes/FetchDatasetJob";
import { UpdateIndexJob } from "../classes/UpdateIndexJob";
import { chunkBy } from "../utils";

const MAX_SQS_MESSAGE_SIZE = 200 * 1024;

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

  const chunks = chunkBy(newDatasets, {
    size: MAX_SQS_MESSAGE_SIZE,
    count: 10
  });

  for (const chunk of chunks) {
    await pushToQueue([new UpdateIndexJob(chunk as any[])]);
  }
}
