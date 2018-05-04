import AWS = require("aws-sdk");
import _ = require("lodash");
import uuid = require("uuid/v1");

export function bootstrap(event, context, callback) {
  console.log("Start publishing data request tasks.");

  const s3 = new AWS.S3();
  const sqs = new AWS.SQS();

  const params = {
    Bucket: process.env.S3_BUCKET,
    Key: process.env.S3_DATA_SOURCE
  };

  s3
    .getObject(params)
    .promise()
    .then((data: any) => {
      const list = JSON.parse(data.Body);
      const messages = [];

      for (const source of list) {
        console.log(`Publishing request task for data source ${source.name}.`);

        source.messageType = "FetchSource";
        messages.push(source);
      }

      const chunks = _.chunk(messages, 10);
      const tasks = [];

      for (const chunk of chunks) {
        const entries = [];

        for (const source of chunk) {
          entries.push({
            Id: uuid(),
            MessageBody: JSON.stringify(source)
          });
        }

        const task = sqs
          .sendMessageBatch({
            QueueUrl: process.env.SQS_QUEUE_URL,
            Entries: entries
          })
          .promise();

        tasks.push(task);
      }

      return Promise.all(tasks);
    })
    .then(() => {
      console.log("Published tasks!");
      callback();
    })
    .catch(err => callback(err));
}
