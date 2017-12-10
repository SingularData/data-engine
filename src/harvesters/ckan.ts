import Rx = require("rxjs/Rx");
import * as config from "config";
import { Dataset } from "w3c-dcat";
import { fetchRx } from "../utils/fetch-util";
import { sha256 } from "../utils/hash-util";

const cocurrency: number = config.get("cocurrency");
const requestSize = 500;

/**
 * Harvest CKAN portal.
 * @param  {string}     url   CKAN portal URL
 * @return {Observable}       a stream of dataset metadata
 */
export function harvest(url) {
  return fetchRx(createUrl(url, 0, 0))
    .mergeMap(res =>
      Rx.Observable.range(0, Math.ceil(res.result.count / requestSize))
    )
    .mergeMap(i => fetchRx(createUrl(url, i, requestSize)), cocurrency)
    .mergeMap(res => Rx.Observable.of(...res.result.results))
    .map(data => {
      return {
        dcat: Dataset.from("CKAN", data).toJSON(),
        checksum: sha256(JSON.stringify(data)),
        original: data
      };
    })
    .catch(error => {
      console.error(error);
      return Rx.Observable.empty();
    });
}

function createUrl(portalUrl, start, rows) {
  return `${portalUrl}/api/3/action/package_search?start=${start}&rows=${rows}`;
}
