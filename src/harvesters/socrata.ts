import Rx = require("rxjs/Rx");
import * as config from "config";
import { Dataset } from "w3c-dcat";
import { fetchRx } from "../utils/fetch-util";
import { sha256 } from "../utils/hash-util";

const cocurrency: number = config.get("cocurrency");
const requestSize = 100;

/**
 * Harvest Socrata network.
 * @return {Observable}         a stream of dataset metadata
 */
export function harvest() {
  return Rx.Observable.of("eu", "us")
    .concatMap(region => {
      const task = fetchRx(createUrl(region, 0, 0))
        .mergeMap(res =>
          Rx.Observable.range(0, Math.ceil(res.resultSetSize / requestSize))
        )
        .mergeMap(
          i => fetchRx(createUrl(region, i * requestSize, requestSize)),
          cocurrency
        );

      return task;
    })
    .mergeMap(res => Rx.Observable.of(...res.results))
    .map(data => {
      return {
        dcat: Dataset.from("Socrata", data).toJSON(),
        checksum: sha256(JSON.stringify(data)),
        original: data
      };
    })
    .catch(error => {
      console.error(error);
      return Rx.Observable.empty();
    });
}

function createUrl(region, offset, limit) {
  return `http://api.${region}.socrata.com/api/catalog/v1?offset=${offset}&limit=${limit}`;
}
