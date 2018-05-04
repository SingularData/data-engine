import fetch from "node-fetch";
import { wrapDataset } from "../index";

export async function getSourceUrls(source) {
  return [source.url];
}

export async function getDatasets(source) {
  const res = await fetch(`${source.url}/api/base`);
  const result = await res.json();

  return result.objects.map(d => wrapDataset("GeoNode", d));
}
