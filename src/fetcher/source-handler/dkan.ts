import fetch from "node-fetch";
import { Dataset } from "w3c-dcat";
import { sha256 } from "../hash-util";

export function getPageUrls(source) {
  return Promise.resolve([source.url]);
}

export function fetchPage(source) {
  return fetch(`${source.url}/data.json`)
    .then(res => res.json())
    .then(res => {
      const data = Array.isArray(res) ? res : res.dataset;
      const datasets = [];

      for (let dataset of data) {
        datasets.push({
          type: "dkan",
          dcat: Dataset.from("DKAN", dataset).toJSON(),
          checksum: sha256(JSON.stringify(dataset)),
          original: dataset
        });
      }

      return datasets;
    });
}
