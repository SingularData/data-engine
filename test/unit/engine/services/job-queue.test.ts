import AWS = require("aws-sdk-mock");
import env = require("dotenv");
import { expect } from "chai";

env.config();

describe("engine/services/job-queue.ts", () => {
  it("should pull jobs.", async () => {
    const message = {
      type: "FetchSource",
      data: {
        sourceType: "test type",
        url: "test url"
      }
    };

    AWS.mock("SQS", "receiveMessage", (params, callback) => {
      expect(params.QueueUrl).to.equal(process.env.SQS_QUEUE_URL);
      expect(params.MaxNumberOfMessages).to.least(1);
      expect(params.MaxNumberOfMessages).to.most(10);

      expect(params.WaitTimeSeconds).to.be.least(0);
      expect(params.WaitTimeSeconds).to.be.most(20);

      callback(null, {
        Messages: [
          {
            MessageId: "test id",
            ReceiptHandle: "test handler",
            Body: JSON.stringify(message)
          }
        ]
      });
    });

    const queue = require("../../../../src/engine/services/job-queue");
    const jobs = await queue.pull();

    expect(jobs).to.have.lengthOf(1);

    const job = jobs[0];
    expect(job).to.deep.equal({
      messageId: "test id",
      receiptHandle: "test handler",
      ...message
    });
  });

  afterEach(() => {
    AWS.restore();
  });
});
