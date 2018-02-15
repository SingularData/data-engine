import fetch from "node-fetch";
import { wrapDataset } from "../util";
import { map } from "lodash";

const requestSize = 100;
const regions = ["us", "eu"];

export function getPageUrls() {
  const urls = [];
  const tasks = [];

  for (let region of regions) {
    const task = fetch(createUrl(region, 0, 0))
      .then(res => res.json())
      .then(res => {
        const count = Math.ceil(res.resultSetSize / requestSize);

        for (let i = 0; i < count; i++) {
          urls.push(createUrl(region, i * requestSize, requestSize));
        }
      });

    tasks.push(task);
  }

  return Promise.all(tasks).then(() => urls);
}

export function fetchPage(source) {
  return fetch(source.url)
    .then(res => res.json())
    .then(res => map(res.results, d => wrapDataset("Socrata", d)));
}

function createUrl(region, offset, limit) {
  return `http://api.${region}.socrata.com/api/catalog/v1?offset=${offset}&limit=${limit}`;
}
