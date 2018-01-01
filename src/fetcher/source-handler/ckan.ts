import fetch from "node-fetch";
import { wrapDataset } from "../util";

const requestSize = 500;

export function getPageUrls(source) {
  return fetch(createUrl(source.url, 0, 0))
    .then(res => res.json())
    .then(res => {
      const urls = [];
      const count = Math.ceil(res.result.count / requestSize);

      for (let i = 0; i < count; i++) {
        urls.push(createUrl(source.url, i * requestSize, requestSize));
      }

      return urls;
    });
}

export function fetchPage(source) {
  return fetch(source.url)
    .then(res => res.json())
    .then(res => res.result.results.map(d => wrapDataset("CKAN", d)));
}

function createUrl(portalUrl, start, rows) {
  return `${portalUrl}/api/3/action/package_search?start=${start}&rows=${rows}`;
}
