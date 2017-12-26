import fetch from "node-fetch";
import { Dataset } from "w3c-dcat";
import { sha256 } from "../hash-util";

const requestSize = 100;

export function getPageUrls() {
  return fetch(createUrl(1, 0))
    .then(res => res.json())
    .then(res => {
      const urls = [];
      const count = Math.ceil(res.meta.stats.totalCount / requestSize);

      for (let i = 1; i <= count; i++) {
        urls.push(createUrl(i, requestSize));
      }

      return urls;
    });
}

export function fetchPage(source) {
  return fetch(source.url)
    .then(res => res.json())
    .then(res => {
      const datasets = [];

      for (let dataset of res.data) {
        datasets.push({
          type: "acrgis",
          dcat: Dataset.from("ArcGIS", dataset).toJSON(),
          checksum: sha256(JSON.stringify(dataset)),
          original: dataset
        });
      }

      return datasets;
    });
}

function createUrl(pageNumber, pageSize) {
  return `https://opendata.arcgis.com/api/v2/datasets?page[number]=${pageNumber}&page[size]=${pageSize}`;
}
