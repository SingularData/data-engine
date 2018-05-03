import { createHash } from "crypto";
import { gzipSync, gunzipSync } from "zlib";
import { Dataset } from "w3c-dcat";

export function compress(data: any): string {
  return gzipSync(JSON.stringify(data)).toString();
}

export function decompress(content: string): any {
  return JSON.parse(gunzipSync(Buffer.from(content)).toString());
}

export function removeNull(dataset) {
  for (let key in dataset) {
    if (!dataset[key]) {
      delete dataset[key];
    } else if (typeof dataset[key] === "object") {
      removeNull(dataset[key]);
    }
  }

  return dataset;
}

export function chunkBy(datasets, limits) {
  const chunks = [];
  let currentSize = 0;
  let currentChunk = [];

  for (let dataset of datasets) {
    const textSize = claculateSize(JSON.stringify(dataset));

    if (
      textSize + currentSize < limits.size &&
      currentChunk.length < limits.count
    ) {
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

export function wrapDataset(type, dataset) {
  const collection = {
    type: type.toLowerCase(),
    dcat: Dataset.from(type.toLowerCase(), dataset).toJSON(),
    checksum: sha256(JSON.stringify(dataset)),
    original: dataset
  };

  return removeNull(collection);
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
