import AWS = require("aws-sdk");
import * as _ from "lodash";
import * as arcgis from "./source-handler/arcgis";
import * as ckan from "./source-handler/ckan";
import * as dkan from "./source-handler/dkan";
import * as geonode from "./source-handler/geonode";
import * as junar from "./source-handler/junar";
import * as opendatasoft from "./source-handler/opendatasoft";
import * as socrata from "./source-handler/socrata";

const sourceHandlers = {
  arcgis,
  ckan,
  dkan,
  geonode,
  junar,
  opendatasoft,
  socrata
};

exports.fetch = (event, context) => {
  const source = JSON.parse(event.Records[0].Sns.Message);
  const sns = new AWS.SNS();

  switch (source.messageType) {
    case "FetchSource":
      return sourceHandlers[source.type.toLowerCase()]
        .getPageUrls(source)
        .then(urls => {
          const tasks = [];

          for (let url of urls) {
            console.log(
              `Publishing fetch task for data source page: ${source.name}.`
            );

            const sourcePage = _.assign({}, source, {
              messageType: "FetchPage",
              url
            });

            const task = sns
              .publish({
                Message: JSON.stringify(sourcePage),
                TopicArn: process.env.SNS_FETCH_QUEUE
              })
              .promise()
              .then(() =>
                console.log(
                  `Published fetch task for data source page: ${source.name}.`
                )
              )
              .catch(err => {
                console.log(
                  `Failed to publish fetch page task for ${source.name}.`
                );
                console.error(err);
              });

            tasks.push(task);
          }

          return Promise.all(tasks);
        });
    case "FetchPage":
      return sourceHandlers[source.type.toLowerCase()]
        .fetchPage(source)
        .then(datasets => {
          const tasks = [];

          for (let dataset of datasets) {
            console.log(`Publishing index task for dataset: ${source.name}.`);

            const task = sns
              .publish({
                Message: JSON.stringify(dataset),
                TopicArn: process.env.SNS_INDEX_QUEUE
              })
              .promise()
              .then(() =>
                console.log(`Published index task for dataset: ${source.name}.`)
              )
              .catch(err => {
                console.log(`Failed to publish index task for ${source.name}.`);
                console.error(err);
              });

            tasks.push(task);
          }

          return Promise.all(tasks);
        });
  }
};
