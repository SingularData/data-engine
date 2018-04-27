import env = require("dotenv");
import { expect } from "chai";
import { fetchDatasets } from "../../../../src/engine/job-handlers/fetch-datasets";
import { FetchDatasetJob } from "../../../../src/engine/classes/FetchDatasetJob";
import { UpdateIndexJob } from "../../../../src/engine/classes/UpdateIndexJob";

env.config();

describe("engine/job-handlers/fetch-datasets.ts", () => {
  it("should handle fetch-dataset job.", async () => {
    const job = new FetchDatasetJob("ckan", "test url");
    const dataset = {
      type: "ckan",
      dcat: {},
      checksum: "test checksum",
      origin: {}
    };

    const getDatasets = async () => [dataset];
    const datasetExists = async () => false;
    const pushToQueue = async (jobs: UpdateIndexJob[]) => {
      expect(jobs).to.have.lengthOf(1);

      const job = jobs[0];
      expect(job.datasets).to.have.lengthOf(1);

      const toIndex = job.datasets[0];
      expect(toIndex).to.deep.equal(dataset);
    };

    await fetchDatasets(getDatasets, datasetExists, pushToQueue, job);
  });
});
