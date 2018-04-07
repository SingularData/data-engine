import AWS = require("aws-sdk");
import _ = require("lodash");
import uuid = require("uuid/v1");
import { IndexDatasetJob } from "../classes/IndexDatasetJob";
import * as sources from "../utils/sources";

export async function fetchSources(es, job: IndexDatasetJob) {
  await es.index(job.datasets);
}
