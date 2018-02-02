import env = require("dotenv");
import AWS = require("aws-sdk");
import AM = require("aws-sdk-mock");
import { expect } from "chai";
import { outputJsonSync, statSync, removeSync } from "fs-extra";
import * as util from "../../../src/fetcher/util";

env.config();

describe("fetcher/util.removeNull()", () => {
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
      outputJsonSync(`./temp/dataset_chunk_${i}.json`, chunks[i]);

      const stats = statSync(`./temp/dataset_chunk_${i}.json`);
      expect(stats.size).to.be.lte(size);
    }
  });

  after(() => {
    removeSync("./temp");
  });
});

describe("fetcher/util.deduplicate()", () => {
  const datasets = require("../../mock/index-queue-item");

  it("should filter out datasets with unchanged checksum.", done => {
    AM.mock("DynamoDB", "getItem", (params, callback) => {
      callback(null, {
        Item: {
          identifier: {
            S: "308afd50-7398-4af8-926d-14a3e312fcf5"
          },
          checksum: {
            S: "12"
          }
        }
      });
    });

    const dynamodb = new AWS.DynamoDB();

    util
      .deduplicate(dynamodb, datasets)
      .then(filtered => {
        expect(filtered.length).to.equal(0);
        done();
      })
      .catch(err => done(err));
  });

  it("should accept datasets with changed checksum.", done => {
    AM.mock("DynamoDB", "getItem", (params, callback) => {
      callback(null, {
        Item: {
          identifier: {
            S: "308afd50-7398-4af8-926d-14a3e312fcf5"
          },
          checksum: {
            S: "13"
          }
        }
      });
    });

    const dynamodb = new AWS.DynamoDB();

    util
      .deduplicate(dynamodb, datasets)
      .then(filtered => {
        expect(filtered.length).to.equal(1);
        done();
      })
      .catch(err => done(err));
  });

  it("should accept datasets with non-existing checksum.", done => {
    AM.mock("DynamoDB", "getItem", (params, callback) => {
      callback(null, {});
    });

    const dynamodb = new AWS.DynamoDB();

    util
      .deduplicate(dynamodb, datasets)
      .then(filtered => {
        expect(filtered.length).to.equal(1);
        done();
      })
      .catch(err => done(err));
  });

  afterEach(() => {
    AM.restore();
  });
});
