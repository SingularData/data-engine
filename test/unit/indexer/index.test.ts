import AWS = require("aws-sdk-mock");
import env = require("dotenv");
import { expect } from "chai";
import { wrapDataset } from "../../../src/fetcher/util";

env.config();

describe("indexer/index.ts", function() {
  this.timeout(20000);

  it("should receive index tasks.", done => {
    let count = 0;

    AWS.mock("DynamoDB", "batchWriteItem", (params, callback) => {
      count++;

      expect(
        params.RequestItems[process.env.DYNAMODB_CHECKSUM].length
      ).to.equal(1);
      callback();
    });

    const handler = require("../../../src/indexer");
    const dataset = require("../../mock/dkan-dataset.json");

    dataset.identifier = dataset.identifier + "-test";

    handler.index(
      {
        Records: [
          {
            Sns: {
              Message: JSON.stringify([wrapDataset("dkan", dataset)])
            }
          }
        ]
      },
      {},
      err => {
        expect(count).to.equal(1);
        done(err);
      }
    );
  });

  afterEach(() => {
    AWS.restore();
  });
});
