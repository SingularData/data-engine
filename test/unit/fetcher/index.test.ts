import AWS = require("aws-sdk-mock");
import env = require("dotenv");
import { expect } from "chai";

env.config();

describe("fetcher/index.ts", function() {
  this.timeout(20000);

  it("should receive fetch-source tasks.", done => {
    let count = 0;

    AWS.mock("SNS", "publish", (params, callback) => {
      count++;

      const message = JSON.parse(params.Message);
      expect(message.messageType).to.equal("FetchPage");
      expect(message.url).to.be.a("string");

      callback();
    });

    const handler = require("../../../src/fetcher");

    handler.fetch(
      {
        Records: [
          {
            Sns: {
              Message:
                '{"name":"Energy Data eXchange","type":"CKAN","url":"https://edx.netl.doe.gov","messageType":"FetchSource"}'
            }
          }
        ]
      },
      {},
      err => {
        expect(count).to.be.gt(0);
        done(err);
      }
    );
  });

  it("should receive fetch-page tasks.", done => {
    let count = 0;

    AWS.mock("SNS", "publish", (params, callback) => {
      count++;

      const message = JSON.parse(params.Message);
      expect(message.length).to.be.gt(0);

      callback();
    });

    AWS.mock("DynamoDB", "getItem", (params, callback) => {
      callback(null, {
        Item: {
          checksum: null
        }
      });
    });

    const handler = require("../../../src/fetcher");

    handler.fetch(
      {
        Records: [
          {
            Sns: {
              Message:
                '{"name":"Energy Data eXchange","type":"CKAN","url":"https://edx.netl.doe.gov","messageType":"FetchPage","url":"https://edx.netl.doe.gov/api/3/action/package_search?start=0&rows=100"}'
            }
          }
        ]
      },
      {},
      err => {
        expect(count).to.be.gt(0);
        done(err);
      }
    );
  });

  afterEach(() => {
    AWS.restore();
  });
});
