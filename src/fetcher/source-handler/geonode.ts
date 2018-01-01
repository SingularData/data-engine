import fetch from "node-fetch";
import { wrapDataset } from "../util";

export function getPageUrls(source) {
  return Promise.resolve([source.url]);
}

export function fetchPage(source) {
  return fetch(`${source.url}/api/base`)
    .then(res => res.json())
    .then(res => res.objects.map(d => wrapDataset("GeoNode", d)));
}
