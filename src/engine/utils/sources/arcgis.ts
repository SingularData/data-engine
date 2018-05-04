import fetch from "node-fetch";
import { wrapDataset } from "../index";

const requestSize = 100;

export async function getSourceUrls() {
  const res = await fetch(createUrl(1, 0));
  const result = await res.json();

  const urls = [];
  const count = Math.ceil(result.meta.stats.totalCount / requestSize);

  for (let i = 1; i <= count; i++) {
    urls.push(createUrl(i, requestSize));
  }

  return urls;
}

export async function getDatasets(source) {
  const res = await fetch(source.url);
  const result = await res.json();
  return result.data.map(d => wrapDataset("ArcGIS", d));
}

function createUrl(pageNumber, pageSize) {
  return `https://opendata.arcgis.com/api/v2/datasets?page[number]=${pageNumber}&page[size]=${pageSize}`;
}
