import fetch from "node-fetch";
import { wrapDataset } from "../index";

export async function getSourceUrls(source) {
  return [source.url];
}

export async function getDatasets(source) {
  const res = await fetch(`${source.url}/data.json`);
  const result = await res.json();
  const data = Array.isArray(result) ? result : result.dataset;

  return data.map(d => wrapDataset("DKAN", d));
}
