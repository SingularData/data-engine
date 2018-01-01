import fetch from "node-fetch";
import { wrapDataset } from "../util";

const requestSize = 100;

export function getPageUrls() {
  return fetch(createUrl(0, 0))
    .then(res => res.json())
    .then(res => {
      const urls = [];
      const count = Math.ceil(res.total_count / requestSize);

      for (let i = 0; i < count; i++) {
        urls.push(createUrl(i * requestSize, requestSize));
      }

      return urls;
    });
}

export function fetchPage(source) {
  return fetch(source.url)
    .then(res => res.json())
    .then(res => res.datasets.map(d => wrapDataset("OpenDataSoft", d)));
}

function createUrl(start, rows) {
  return `https://data.opendatasoft.com/api/v2/catalog/datasets?rows=${rows}&start=${start}`;
}
