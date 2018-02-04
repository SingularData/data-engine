import env = require("dotenv");
import AWS = require("aws-sdk");
import AM = require("aws-sdk-mock");
import { expect } from "chai";
import { outputJsonSync, statSync, removeSync } from "fs-extra";
import * as util from "../../../src/indexer/util";

env.config();

describe("indexer/util.saveChecksum()", () => {
  it("should save checksum of datasets.", done => {
    const dataset = require("../../mock/index-queue-item")[0];
    const datasets = new Array(100).fill(dataset);
    let count = 0;

    AM.mock("DynamoDB", "batchWriteItem", (params, callback) => {
      count++;
      callback(null, {});
    });

    const dynamodb = new AWS.DynamoDB();

    util
      .saveChecksum(dynamodb, datasets)
      .then(filtered => {
        expect(count).to.equal(4);
        done();
      })
      .catch(err => done(err));
  });

  afterEach(() => {
    AM.restore();
  });
});
