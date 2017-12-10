import Rx = require("rxjs/Rx");
import * as _ from "lodash";
import * as config from "config";
import { Dataset } from "w3c-dcat";
import { fetchRx } from "../utils/fetch-util";
import { sha256 } from "../utils/hash-util";

/**
 * Harvest DKAN portal.
 * @param  {string}      url  portal url
 * @return {Observable}       a stream of dataset metadata
 */
export function harvest(url) {
  return fetchRx(`${url}/data.json`)
    .mergeMap(res => {
      const datdasets = _.isArray(res) ? res : res.dataset;
      return Rx.Observable.of(...datdasets);
    })
    .map(data => {
      return {
        dcat: Dataset.from("DKAN", data).toJSON(),
        checksum: sha256(JSON.stringify(data)),
        original: data
      };
    })
    .catch(error => {
      console.error(error);
      return Rx.Observable.empty();
    });
}
