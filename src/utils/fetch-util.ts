import { sample } from "lodash";
import * as config from "config";
import fetch, { RequestInit } from "node-fetch";
import Rx = require("rxjs/Rx");

const userAgents = config.get("userAgents");

/**
 * Send a GET request with the given URL.
 * @param  {string}        url url
 * @return {Observable}    an Observable
 */
export function fetchRx(url: string): Rx.Observable<any> {
  const fetchOptions: RequestInit = {
    method: "GET",
    headers: {
      "Content-Type": "application/json",
      "User-Agent": sample(userAgents)
    }
  };

  return Rx.Observable.defer(() => {
    const task = Rx.Observable.fromPromise(
      fetch(url, fetchOptions).then(res => res.json())
    );
    return task;
  });
}
