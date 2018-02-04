import { expect } from "chai";
import { wrapDataset } from "../../../src/fetcher/util";

describe("indexer/index.ts", function() {
  this.timeout(20000);

  it("should receive index tasks.", done => {
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
        expect(true).to.equal(true);
        done(err);
      }
    );
  });
});
