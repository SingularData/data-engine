import es = require("elasticsearch");
import awsES = require("http-aws-es");
import AWS = require("aws-sdk");

AWS.config.region = "us-east-1";

const client = new es.Client({
  hosts: [process.env.ES_URL],
  connectionClass: awsES
});

function index(datasets) {
  const body = [];

  for (let dataset of datasets) {
    const action = {
      index: {
        _index: process.env.ES_INDEX,
        _type: process.env.ES_DOC_TYPE,
        _id: dataset.dcat.identifier
      }
    };

    body.push(action, dataset);
  }

  return client.bulk({ body });
}

function exists(dataset) {
  const params = {
    index: process.env.ES_INDEX,
    type: process.env.ES_DOC_TYPE,
    id: dataset.dcat.identifier,
    _sourceInclude: ["checksum"]
  };

  return client
    .get(params)
    .then((result: any) => {
      if (!result.found) {
        return false;
      }

      if (result._source.checksum !== dataset.checksum) {
        return false;
      }

      return true;
    })
    .catch(err => {
      console.error(err);
      return false;
    });
}

export { client, index };
