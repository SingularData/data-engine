import { createHash } from "crypto";
import { Dataset } from "w3c-dcat";
import * as _ from "lodash";

export function chunkBySize(datasets, size) {
  const chunks = [];
  let currentSize = 0;
  let currentChunk = [];

  for (let dataset of datasets) {
    const textSize = claculateSize(JSON.stringify(dataset));

    if (textSize + currentSize < size) {
      currentChunk.push(dataset);
      currentSize += textSize;
    } else {
      chunks.push(currentChunk);

      currentSize = 0;
      currentChunk = [];
    }
  }

  if (currentChunk.length > 0) {
    chunks.push(currentChunk);
  }

  return chunks;
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
        if (_.get(data, "Item.checksum.S") !== dataset.checksum) {
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
    type: type.toLowerCase(),
    dcat: Dataset.from(type.toLowerCase(), dataset).toJSON(),
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
  return Buffer.from(s).length;
}
