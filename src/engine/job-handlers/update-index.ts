import AWS = require("aws-sdk");
import _ = require("lodash");
import uuid = require("uuid/v1");
import { UpdateIndexJob } from "../classes/UpdateIndexJob";
import * as sources from "../utils/sources";

export async function updateIndex(es, job: UpdateIndexJob) {
  await es.index(job.datasets);
}
