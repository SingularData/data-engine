import Rx = require("rxjs/Rx");
import fse = require("fs-extra");
import * as config from "config";
import { parse } from "path";
import { upsert, getChecksumMap, ensureIndex } from "./elasticsearch";
import { defaults } from "lodash";

const harvesters = {};
const bulkInsertSize: number = config.get("elasticsearch.bulkInsertSize");

fse.readdirSync(__dirname + "/harvesters").forEach(file => {
  const parsed = parse(file);
  harvesters[parsed.name] = require(__dirname + `/harvesters/${file}`).harvest;
});

export function execute(): Rx.Observable<any> {
  const sources = fse.readJsonSync(__dirname + "/../data/sources.json");

  let checksumMap = {};

  const setChecksum = getChecksumMap().do(result => {
    checksumMap = result;
  });

  const updateTask = Rx.Observable.of(...sources)
    .filter(source => harvesters[source.type.toLowerCase()])
    .concatMap(source => harvesters[source.type.toLowerCase()](source))
    .filter(
      (dataset: any) =>
        checksumMap[dataset.dcat.identifier] !== dataset.checksum
    )
    .bufferCount(bulkInsertSize)
    .concatMap(datasets => upsert(datasets));

  return ensureIndex()
    .concatMap(() => setChecksum)
    .concatMap(() => updateTask);
}
