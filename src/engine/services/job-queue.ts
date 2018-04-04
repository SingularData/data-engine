// this module will provide the push and pull function for the AWS SQS
import AWS = require("aws-sdk");
import uuid = require("uuid/v1");

enum JobType {
  FetchDatasets = "FetchDatasets",
  FetchSources = "FetchSources",
  UpdateIndex = "UpdateIndex"
}

interface IJob {
  type: JobType;
  data: any;
  messageId?: string;
}

const sqs = new AWS.SQS();

async function push(jobs: IJob[]) {
  if (jobs.length > 10) {
    throw new Error("Job list is longer than 10.");
  }

  for (const job of jobs) {
    job.messageId = uuid();
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

async function pull(jobs: IJob[]) {
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

async function remove(jobs: IJob[]) {
  const params = {
    QueueUrl: process.env.SQS_QUEUE_URL,
    Entries: jobs.map(job => {
      return { Id: job.messageId };
    })
  };

  return sqs.deleteMessageBatch(params).promise();
}

export { JobType, IJob, push, pull, remove };
