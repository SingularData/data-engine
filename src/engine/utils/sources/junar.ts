import fetch from "node-fetch";
import { wrapDataset } from "../index";

const requestSize = 100;

export async function getSourceUrls(source) {
  const res = await fetch(createUrl(source.apiUrl, source.apiKey, 0, 1));
  const result = await res.json();

  const urls = [];
  const count = Math.ceil(result.count / requestSize);

  for (let i = 0; i < count; i++) {
    urls.push(
      createUrl(source.apiUrl, source.apiKey, i * requestSize, requestSize)
    );
  }

  return urls;
}

export async function getDatasets(source) {
  const res = await fetch(source.url);
  const result = await res.json();

  return result.results.map(d => wrapDataset("Junar", d));
}

function createUrl(apiUrl, apiKey, offset, limit) {
  return `${apiUrl}/api/v2/datasets/?auth_key=${apiKey}&offset=${offset}&limit=${limit}`;
}
