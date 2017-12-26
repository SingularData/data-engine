import fetch from "node-fetch";
import { Dataset } from "w3c-dcat";
import { sha256 } from "../hash-util";

const requestSize = 500;

export function getPageUrls(source) {
  return fetch(createUrl(source.url, 0, 0))
    .then(res => res.json())
    .then(res => {
      const urls = [];
      const count = Math.ceil(res.result.count / requestSize);

      for (let i = 1; i <= count; i++) {
        urls.push(createUrl(source.url, i, requestSize));
      }

      return urls;
    });
}

export function fetchPage(source) {
  return fetch(source.url)
    .then(res => res.json())
    .then(res => {
      const datasets = [];

      for (let dataset of res.result.results) {
        datasets.push({
          type: "ckan",
          dcat: Dataset.from("CKAN", dataset).toJSON(),
          checksum: sha256(JSON.stringify(dataset)),
          original: dataset
        });
      }

      return datasets;
    });
}

function createUrl(portalUrl, start, rows) {
  return `${portalUrl}/api/3/action/package_search?start=${start}&rows=${rows}`;
}
