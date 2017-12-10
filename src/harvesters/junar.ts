import Rx = require("rxjs/Rx");
import * as _ from "lodash";
import * as config from "config";
import { Dataset } from "w3c-dcat";
import { fetchRx } from "../utils/fetch-util";
import { sha256 } from "../utils/hash-util";

const cocurrency: number = config.get("cocurrency");
const requestSize = 100;

/**
 * Harvest GeoNode portal.
 * @param  {string}     url   portal API url
 * @param  {string}     key   portal API key
 * @return {Observable}       a stream of dataset metadata
 */
export function harvest(url, key) {
  return fetchRx(createUrl(url, key, 0, 1))
    .mergeMap(res => Rx.Observable.range(0, Math.ceil(res.count / requestSize)))
    .mergeMap(
      (i: number) => fetchRx(createUrl(url, key, i * requestSize, requestSize)),
      cocurrency
    )
    .mergeMap(res => Rx.Observable.of(...res.results))
    .map(data => {
      return {
        dcat: Dataset.from("Junar", data).toJSON(),
        checksum: sha256(JSON.stringify(data)),
        original: data
      };
    })
    .catch(error => {
      console.error(error);
      return Rx.Observable.empty();
    });
}

function createUrl(apiUrl, apiKey, offset, limit) {
  return `${apiUrl}/api/v2/datasets/?auth_key=${apiKey}&offset=${offset}&limit=${limit}`;
}
