import { createHash } from "crypto";
import { Dataset } from "w3c-dcat";
import * as _ from "lodash";

export function stringPlunk(datasets, size) {
  const plunks = [];
  let currentSize = 0;
  let currentPlunk = [];

  // substract the size of "[]"
  size -= 4;

  for (let dataset of datasets) {
    const s = JSON.stringify(dataset);
    const textSize = claculateSize(s);

    if (textSize + currentSize < size) {
      currentPlunk.push(s);
      currentSize += textSize;
    } else {
      plunks.push("[" + currentPlunk.join(",") + "]");

      currentSize = 0;
      currentPlunk = [];
    }
  }

  return plunks;
}

export function deduplicate(dynamodb, datasets) {
  const filtered = [];
  const tasks = [];
  const params = {
    TableName: process.env.DYNAMODB_CHECKSUM,
    Key: {
      identifier: {
        S: ""
      }
    },
    ConsistentRead: true
  };

  for (let dataset of datasets) {
    params.Key.identifier.S = dataset.dcat.identifier;

    const task = dynamodb
      .getItem(params)
      .promise()
      .then(data => {
        if (_.get(data, "Item.checksum") !== dataset.checksum) {
          filtered.push(dataset);
        }
      })
      .catch(err => {
        console.error(err);
        console.log("Unable to check duplication for item: ", dataset.dcat);
      });

    tasks.push(task);
  }

  return Promise.all(tasks).then(() => filtered);
}

export function wrapDataset(type, dataset) {
  return {
    type,
    dcat: Dataset.from(type, dataset).toJSON(),
    checksum: sha256(JSON.stringify(dataset)),
    original: dataset
  };
}

/**
 * Create checksum in SHA-256.
 * @param  {any}    data data
 * @return {string}      SHA-256 checksum
 */
function sha256(data: string): string {
  const hash = createHash("sha256");
  hash.update(data);

  return hash.digest("base64");
}

function claculateSize(s) {
  return ~-encodeURI(s).split(/%..|./).length;
}
