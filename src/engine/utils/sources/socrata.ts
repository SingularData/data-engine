import fetch from "node-fetch";
import { wrapDataset } from "../index";
import { map } from "lodash";

const requestSize = 100;
const regions = ["us", "eu"];

export async function getSourceUrls() {
  const urls = [];
  const tasks = [];

  for (const region of regions) {
    const res = await fetch(createUrl(region, 0, 0));
    const result = await res.json();

    const count = Math.ceil(result.resultSetSize / requestSize);

    for (let i = 0; i < count; i++) {
      urls.push(createUrl(region, i * requestSize, requestSize));
    }
  }

  return urls;
}

export async function getDatasets(source) {
  const res = await fetch(source.url);
  const result = await res.json();

  return map(result.results, d => wrapDataset("Socrata", d));
}

function createUrl(region, offset, limit) {
  return `http://api.${region}.socrata.com/api/catalog/v1?offset=${offset}&limit=${limit}`;
}
