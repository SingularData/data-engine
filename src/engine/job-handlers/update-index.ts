import AWS = require("aws-sdk");
import _ = require("lodash");
import uuid = require("uuid/v1");
import { UpdateIndexJob } from "../classes/UpdateIndexJob";
import * as sources from "../utils/sources";

export async function updateIndex(indexDatasets, job: UpdateIndexJob) {
  await indexDatasets(job.datasets);
}
