import fetch from "node-fetch";
import { wrapDataset } from "../util";

const requestSize = 100;

export function getPageUrls(source) {
  return fetch(createUrl(source.apiUrl, source.apiKey, 0, 1))
    .then(res => res.json())
    .then(res => {
      const urls = [];
      const count = Math.ceil(res.count / requestSize);

      for (let i = 0; i < count; i++) {
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
    .then(res => res.results.map(d => wrapDataset("Junar", d)));
}

function createUrl(apiUrl, apiKey, offset, limit) {
  return `${apiUrl}/api/v2/datasets/?auth_key=${apiKey}&offset=${offset}&limit=${limit}`;
}
