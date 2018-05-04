import env = require("dotenv-safe");
import AWS = require("aws-sdk");
import AM = require("aws-sdk-mock");
import { expect } from "chai";
import { outputJsonSync, statSync, removeSync } from "fs-extra";
import * as util from "../../../../src/engine/utils";

if (process.env.NODE_ENV !== "ci") {
  env.config();
}

describe("src/engine/utils.removeNull()", () => {
  it("should recursively remove all null values in an object.", () => {
    const dataset = {
      identifier: "123",
      description: "",
      issued: null,
      distribution: [{ tittle: "123", description: "" }]
    };

    const cleaned = util.removeNull(dataset);

    expect(cleaned).to.deep.equal({
      identifier: "123",
      distribution: [{ tittle: "123" }]
    });
  });
});

describe("src/engine/utils.chunkBySize()", () => {
  it("should chunk an array of json into chunks with the given size limit.", () => {
    const dataset = require("../../../mock/index-queue-item.json")[0];
    const harvested = [];

    for (let i = 0; i < 100; i++) {
      harvested.push(dataset);
    }

    const size = 10000;
    const count = 10;
    const chunks = util.chunkBy(harvested, { size, count });

    expect(chunks.length).to.be.gt(0);

    // check if each chunk is smaller than the limit
    for (let i = 0; i < chunks.length; i++) {
      expect(chunks[i].length).to.be.lte(count);

      outputJsonSync(`./temp/dataset_chunk_${i}.json`, chunks[i]);

      const stats = statSync(`./temp/dataset_chunk_${i}.json`);
      expect(stats.size).to.be.lte(size);
    }
  });

  after(() => {
    removeSync("./temp");
  });
});
