import Rx = require("rxjs/Rx");
import * as config from "config";
import { Dataset } from "w3c-dcat";
import { fetchRx } from "../utils/fetch-util";
import { sha256 } from "../utils/hash-util";

const cocurrency: number = config.get("cocurrency");
const requestSize = 100;

/**
 * Harvest ArcGIS Open Data network.
 * @return {Observable}         a stream of dataset metadata
 */
export function harvest() {
  return fetchRx(createUrl(1, 0))
    .mergeMap(res =>
      Rx.Observable.range(1, Math.ceil(res.meta.stats.totalCount / requestSize))
    )
    .mergeMap(i => fetchRx(createUrl(i, requestSize)), cocurrency)
    .mergeMap(res => Rx.Observable.of(...res.data))
    .map(data => {
      return {
        type: "acrgis",
        dcat: Dataset.from("ArcGIS", data).toJSON(),
        checksum: sha256(JSON.stringify(data)),
        original: data
      };
    })
    .catch(error => {
      console.error(error);
      return Rx.Observable.empty();
    });
}

function createUrl(pageNumber, pageSize) {
  return `https://opendata.arcgis.com/api/v2/datasets?page[number]=${pageNumber}&page[size]=${pageSize}`;
}
