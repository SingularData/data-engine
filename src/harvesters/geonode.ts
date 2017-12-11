import Rx = require("rxjs/Rx");
import * as config from "config";
import { Dataset } from "w3c-dcat";
import { fetchRx } from "../utils/fetch-util";
import { sha256 } from "../utils/hash-util";

/**
 * Harvest GeoNode portal.
 * @param  {object}      source   GeoNode data source (portal)
 * @return {Observable}           a stream of dataset metadata
 */
export function harvest(source) {
  return fetchRx(`${source.url}/api/base`)
    .mergeMap(res => Rx.Observable.of(...res.objects))
    .map(data => {
      return {
        dcat: Dataset.from("GeoNode", data).toJSON(),
        checksum: sha256(JSON.stringify(data)),
        original: data
      };
    })
    .catch(error => {
      console.error(error);
      return Rx.Observable.empty();
    });
}
