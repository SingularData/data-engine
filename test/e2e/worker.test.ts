import { expect } from "chai";
import * as config from "config";
import * as worker from "../../src/worker";
import * as es from "../../src/elasticsearch";
import * as _ from "lodash";
import fetch from "node-fetch";
import s = require("sleep");

describe("worker.ts", function() {
  this.timeout(60000);

  before(done => {
    fetch(config.get("elasticsearch.url"))
      .then(() => done())
      .catch(() => done(new Error("ElasticSearch is not running")));
  });

  before(done => {
    es.ensureIndex().subscribe(_.noop, err => done(err), () => done());
  });

  it("execute() should run the complete harvest-index task.", done => {
    worker
      .execute()
      .first()
      .do(() => s.sleep(5))
      .concatMap(() => es.getChecksumMap())
      .subscribe(
        result => {
          expect(Object.keys(result).length).to.be.above(1);
        },
        err => done(err),
        () => done()
      );
  });
});
