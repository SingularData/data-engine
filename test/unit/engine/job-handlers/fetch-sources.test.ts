import env = require("dotenv-safe");
import { expect } from "chai";
import { fetchSources } from "../../../../src/engine/job-handlers/fetch-sources";
import { FetchSourceJob } from "../../../../src/engine/classes/FetchSourceJob";
import { FetchDatasetJob } from "../../../../src/engine/classes/FetchDatasetJob";

if (process.env.NODE_ENV !== "ci") {
  env.config();
}

describe("engine/job-handlers/fetch-sources.ts", () => {
  it("should handle fetch-dataset job.", async () => {
    const job = new FetchSourceJob("ckan", "test url");

    const getSourceUrls = async () => ["url 1"];
    const pushToQueue = async (jobs: FetchDatasetJob[]) => {
      expect(jobs).to.have.lengthOf(1);

      const job = jobs[0];
      expect(job.data.sourceType).to.equal("ckan");
      expect(job.data.url).to.equal("url 1");
    };

    await fetchSources(getSourceUrls, pushToQueue, job);
  });
});
