import AWS = require("aws-sdk");

AWS.config.region = "us-east-1";

export function bootstrap(event, context, callback) {
  console.log("Start publishing data request tasks.");

  const s3 = new AWS.S3();
  const sns = new AWS.SNS();

  const params = {
    Bucket: process.env.S3_BUCKET,
    Key: process.env.S3_DATA_SOURCE
  };

  s3
    .getObject(params)
    .promise()
    .then((data: any) => {
      const list = JSON.parse(data.Body);
      const tasks = [];

      for (let source of list) {
        console.log(`Publishing request task for data source ${source.name}.`);

        source.messageType = "FetchSource";

        const task = sns
          .publish({
            Message: JSON.stringify(source),
            TopicArn: process.env.SNS_FETCH_QUEUE
          })
          .promise()
          .then(() =>
            console.log(
              `Published request task for data source ${source.name}.`
            )
          )
          .catch(err => {
            console.log(`Failed to publish task for ${source.name}.`);
            console.error(err);
          });

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
