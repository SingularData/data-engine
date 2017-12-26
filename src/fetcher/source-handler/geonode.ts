import fetch from "node-fetch";
import { Dataset } from "w3c-dcat";
import { sha256 } from "../hash-util";

export function getPageUrls(source) {
  return Promise.resolve([source.url]);
}

export function fetchPage(source) {
  return fetch(`${source.url}/api/base`)
    .then(res => res.json())
    .then(res => {
      const datasets = [];

      for (let dataset of res.objects) {
        datasets.push({
          type: "geonode",
          dcat: Dataset.from("GeoNode", dataset).toJSON(),
          checksum: sha256(JSON.stringify(dataset)),
          original: dataset
        });
      }

      return datasets;
    });
}
