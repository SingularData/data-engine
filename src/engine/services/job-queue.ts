// this module will provide the push and pull function for the AWS SQS
import AWS = require("aws-sdk");
import uuid = require("uuid/v1");

const sqs = new AWS.SQS();

export interface IJob {
  type: string;
  messageId: string;
}

export async function push(jobs: IJob[]) {
  if (jobs.length > 10) {
    throw new Error("Job list is longer than 10.");
  }

  const params = {
    QueueUrl: process.env.SQS_QUEUE_URL,
    Entries: jobs.map(job => {
      return {
        Id: job.messageId,
        MessageBody: JSON.stringify(job)
      };
    })
  };

  return sqs.sendMessageBatch(params).promise();
}

export async function pull(jobs: IJob[]) {
  const params = {
    QueueUrl: process.env.SQS_QUEUE_URL,
    MaxNumberOfMessages: 10,
    WaitTimeSeconds: 30
  };

  return sqs
    .receiveMessage(params)
    .promise()
    .then(result =>
      result.Messages.map(message => {
        return { messageId: message.MessageId, ...JSON.parse(message.Body) };
      })
    );
}

export async function remove(jobs: IJob[]) {
  const params = {
    QueueUrl: process.env.SQS_QUEUE_URL,
    Entries: jobs.map(job => {
      return { Id: job.messageId };
    })
  };

  return sqs.deleteMessageBatch(params).promise();
}
