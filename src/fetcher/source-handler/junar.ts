import fetch from "node-fetch";
import { Dataset } from "w3c-dcat";
import { sha256 } from "../hash-util";

const requestSize = 100;

export function getPageUrls(source) {
  return fetch(createUrl(source.apiUrl, source.apiKey, 0, 1))
    .then(res => res.json())
    .then(res => {
      const urls = [];
      const count = Math.ceil(res.count / requestSize);

      for (let i = 1; i <= count; i++) {
        urls.push(
          createUrl(source.apiUrl, source.apiKey, i * requestSize, requestSize)
        );
      }

      return urls;
    });
}

export function fetchPage(source) {
  return fetch(source.url)
    .then(res => res.json())
    .then(res => {
      const datasets = [];

      for (let dataset of res.results) {
        datasets.push({
          type: "junar",
          dcat: Dataset.from("JUnar", dataset).toJSON(),
          checksum: sha256(JSON.stringify(dataset)),
          original: dataset
        });
      }

      return datasets;
    });
}

function createUrl(apiUrl, apiKey, offset, limit) {
  return `${apiUrl}/api/v2/datasets/?auth_key=${apiKey}&offset=${offset}&limit=${limit}`;
}
