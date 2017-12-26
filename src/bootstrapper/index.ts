import AWS = require("aws-sdk");

AWS.config.region = "us-east-1";

exports.handler = (event, context) => {
  console.log("Start publishing data request tasks.");

  const s3 = new AWS.S3();
  const sns = new AWS.SNS();

  const s3Params = {
    Bucket: process.env.S3_BUCKET,
    Key: process.env.SS_DATA_SOURCE_LIST
  };

  s3
    .getObject(s3Params)
    .promise()
    .then(data => {
      const list = JSON.parse(data.Body.toString());
      const tasks = [];

      for (let source of list) {
        console.log(`Publishing request task for data source ${source.name}.`);

        const task = sns
          .publish({
            Message: JSON.stringify(source),
            MessageStructure: "json",
            TopicArn: process.env.SNS_REQUEST_QUEUE
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
    .then(() => context.done(null, "Request task publication finished!"))
    .catch(err => context.done(err));
};
