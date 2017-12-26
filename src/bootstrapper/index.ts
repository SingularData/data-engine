import AWS = require("aws-sdk");

AWS.config.region = "us-east-1";

exports.handler = (event, context) => {
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
        const task = sns
          .publish({
            Message: JSON.stringify(source),
            MessageStructure: "json",
            TopicArn: process.env.SNS_REQUEST_QUEUE
          })
          .promise()
          .catch(err => console.error(err));

        tasks.push(task);
      }

      return Promise.all(tasks);
    })
    .then(() => context.done(null, "Function Finished!"))
    .catch(err => context.done(err));
};
