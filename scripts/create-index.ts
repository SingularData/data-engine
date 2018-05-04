import es = require("elasticsearch");
import env = require("dotenv");
import sleep = require("sleep-promise");

// tslint:disable-next-line
const mappings = require("./index-mappings.json");

env.config();

const client = new es.Client({
  // host: process.env.ES_URL
  host: "localhost:9200"
});

sleep(50)
  .then(() => client.indices.exists({ index: process.env.ES_INDEX }))
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
