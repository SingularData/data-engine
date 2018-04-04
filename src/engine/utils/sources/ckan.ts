import fetch from "node-fetch";
import { wrapDataset } from "../index";

const requestSize = 100;

export async function getSourceUrls(source) {
  const res = await fetch(createUrl(source.url, 0, 0));
  const result = await res.json();

  const urls = [];
  const count = Math.ceil(result.result.count / requestSize);

  for (let i = 0; i < count; i++) {
    urls.push(createUrl(source.url, i * requestSize, requestSize));
  }

  return urls;
}

export async function getDatasets(source) {
  const res = await fetch(source.url);
  const result = await res.json();

  return result.result.results.map(d => wrapDataset("CKAN", d));
}

function createUrl(portalUrl, start, rows) {
  return `${portalUrl}/api/3/action/package_search?start=${start}&rows=${rows}`;
}
