import Rx = require("rxjs/Rx");
import fse = require("fs-extra");
import * as config from "config";
import { parse } from "path";
import { upsert, getChecksumMap } from "./elasticsearch";
import { defaults } from "lodash";

const harvesters = {};

fse.readdirSync("./harvesters").forEach(file => {
  const parsed = parse(file);
  harvesters[parsed.name] = require(`./harvesters/${file}`).harvest;
});

export function execute() {
  const sources = fse.readJsonSync("./data/sources.json");
  const checksumMap = {};

  const updateTask = Rx.Observable.of(...sources)
    .filter(source => harvesters[source.type])
    .concatMap(source => harvesters[source.type](source))
    .filter(
      (dataset: any) =>
        checksumMap[dataset.dcat.identifier] !== dataset.checksum
    )
    .bufferCount(500)
    .concatMap(datasets => upsert(datasets));

  return Rx.Observable.concat(getChecksumMap(), updateTask);
}
