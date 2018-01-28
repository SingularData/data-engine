import env = require("dotenv");
import { expect } from "chai";
import { outputJsonSync, statSync, emptyDir } from "fs-extra";
import * as util from "../../../src/fetcher/util";

env.config();

describe("fetcher/util.chunkBySize()", () => {
  it("should chunk an array of json into chunks with the given size limit.", () => {
    const dataset = require("../../mock/index-queue-item.json")[0];
    const harvested = [];

    for (let i = 0; i < 100; i++) {
      harvested.push(dataset);
    }

    const size = parseInt(process.env.MAX_SNS_MESSAGE_SIZE, 10);
    const chunks = util.chunkBySize(harvested, size);

    expect(chunks.length).to.be.gt(0);

    // check if each chunk is smaller than the limit
    for (let i = 0; i < chunks.length; i++) {
      outputJsonSync(`../../temp/dataset_chunk_${i}.json`, chunks[i]);

      const stats = statSync(`../../temp/dataset_chunk_${i}.json`);
      expect(stats.size).to.be.lte(size);
    }
  });

  after(() => {
    emptyDir("../../temp");
  });
});
