import Rx = require("rxjs/Rx");
import * as config from "config";
import { Dataset } from "w3c-dcat";
import { fetchRx } from "../utils/fetch-util";
import { sha256 } from "../utils/hash-util";

const cocurrency: number = config.get("cocurrency");
const requestSize = 100;

/**
 * Harvest OpenDataSoft network.
 * @return {Observable}         a stream of dataset metadata
 */
export function harvest() {
  return fetchRx(createUrl(0, 0))
    .mergeMap(res =>
      Rx.Observable.range(0, Math.ceil(res.total_count / requestSize))
    )
    .mergeMap(i => fetchRx(createUrl(i * requestSize, requestSize)), cocurrency)
    .mergeMap(res => Rx.Observable.of(...res.datasets))
    .map(data => {
      return {
        type: "opendatasoft",
        dcat: Dataset.from("OpenDataSoft", data).toJSON(),
        checksum: sha256(JSON.stringify(data)),
        original: data
      };
    })
    .catch(error => {
      console.error(error);
      return Rx.Observable.empty();
    });
}

function createUrl(start, rows) {
  return `https://data.opendatasoft.com/api/v2/catalog/datasets?rows=${rows}&start=${start}`;
}
