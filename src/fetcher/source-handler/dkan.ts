import fetch from "node-fetch";
import { wrapDataset } from "../util";

export function getPageUrls(source) {
  return Promise.resolve([source.url]);
}

export function fetchPage(source) {
  return fetch(`${source.url}/data.json`)
    .then(res => res.json())
    .then(res => {
      const data = Array.isArray(res) ? res : res.dataset;
      return data.map(d => wrapDataset("DKAN", d));
    });
}
