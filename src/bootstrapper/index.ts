import AWS = require("aws-sdk");

AWS.config.region = "us-east-1";

exports.bootstrap = (event, context) => {
  console.log("Start publishing data request tasks.");

  const dynamodb = new AWS.DynamoDB();
  const sns = new AWS.SNS();

  getSources(dynamodb)
    .then((list: any) => {
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
    .then(() => context.done(null, "Request task publication finished!"))
    .catch(err => context.done(err));
};

async function getSources(dynamodb, start?): Promise<any[]> {
  try {
    const params = {
      TableName: process.env.DYNAMODB_TABLE,
      ExclusiveStartKey: start
    };
    const sources = [];

    const data = await dynamodb.scan(params).promise();

    if (data.Count === 0) {
      return sources;
    }

    for (let item of data.Items) {
      const source = {};

      for (let key in item) {
        if (!item.hasOwnProperty(key)) {
          continue;
        }

        source[key] = item[key].S;
      }

      sources.push(source);
    }

    if (data.LastEvaluatedKey) {
      const rest: any = await getSources(dynamodb, data.LastEvaluatedKey);
      sources.push(...rest);
    }

    return sources;
  } catch (err) {
    return Promise.reject(err);
  }
}
