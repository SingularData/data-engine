import AWS = require("aws-sdk");
import es = require("elasticsearch");
import awsES = require("http-aws-es");
import env = require("dotenv");

// tslint:disable-next-line
const mappings = require("./index-mappings.json");

env.config();

AWS.config.region = "us-east-1";

const client = new es.Client({
  hosts: [process.env.ES_URL],
  connectionClass: awsES
});

client.indices
  .exists({ index: process.env.ES_INDEX })
  .then(result => {
    if (result) {
      return;
    }

    return client.indices.create({
      index: process.env.ES_INDEX,
      body: { mappings }
    });
  })
  .then(() => console.log("index created"))
  .catch(err => console.error(err));
