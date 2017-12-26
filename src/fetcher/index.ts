exports.handler = (event, context) => {
  const source = JSON.parse(event.Records[0].Sns.Message);
};
