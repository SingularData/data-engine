import es = require("elasticsearch");
import awsES = require("http-aws-es");

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

export { client, index };
