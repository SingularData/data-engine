import AWS = require("aws-sdk");

exports.fetch = (event, context) => {
  const source = JSON.parse(event.Records[0].Sns.Message);
  const sns = new AWS.SNS();
};
