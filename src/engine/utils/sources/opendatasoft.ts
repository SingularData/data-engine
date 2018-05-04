import fetch from "node-fetch";
import { wrapDataset } from "../index";

const requestSize = 100;

export async function getSourceUrls() {
  const res = await fetch(createUrl(0, 0));
  const result = await res.json();

  const urls = [];
  const count = Math.ceil(result.total_count / requestSize);

  for (let i = 0; i < count; i++) {
    urls.push(createUrl(i * requestSize, requestSize));
  }

  return urls;
}

export async function getDatasets(source) {
  const res = await fetch(source.url);
  const result = await res.json();

  return result.datasets.map(d => wrapDataset("OpenDataSoft", d));
}

function createUrl(start, rows) {
  return `https://data.opendatasoft.com/api/v2/catalog/datasets?rows=${rows}&start=${start}`;
}
