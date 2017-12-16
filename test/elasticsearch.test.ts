import fetch, { RequestInit } from "node-fetch";
import * as config from "config";
import * as es from "../src/elasticsearch";
import { expect } from "chai";
import { Dataset } from "w3c-dcat";
import { sha256 } from "../src/utils/hash-util";
import s = require("sleep");

describe("elasticsearch.ts", function() {
  this.timeout(10000);

  const dataset = {
    dcat: {},
    checksum: "",
    original: {
      identifier: 12,
      title: "test data",
      url: "localhost",
      publisher: "demo",
      distribution: []
    }
  };

  dataset.dcat = Dataset.from("DKAN", dataset.original);
  dataset.checksum = sha256(JSON.stringify(dataset.original));

  before(done => {
    fetch(config.get("elasticsearch.url"))
      .then(() => done())
      .catch(() => done(new Error("ElasticSearch is not running")));
  });

  beforeEach(() => {
    // s.sleep(5);
  });

  it("deleteIndex() should drop existing index.", done => {
    es
      .ensureIndex()
      .concatMap(() => es.deleteIndex())
      .concatMap(() => es.indexExists())
      .subscribe(
        exists => expect(exists).to.be.false,
        err => done(err),
        () => done()
      );
  });

  it("ensureIndex() should create non-existing index.", done => {
    es
      .deleteIndex()
      .concatMap(() => es.ensureIndex())
      .concatMap(() => es.indexExists())
      .subscribe(
        exists => expect(exists).to.be.true,
        err => done(err),
        () => done()
      );
  });

  // For some async reason ?, this doesn't work with other tests but it works
  // by its own.
  it("upsert() should create a new document.", done => {
    es
      .ensureIndex()
      .concatMap(() => es.upsert([dataset]))
      .do(() => s.sleep(2))
      .concatMap(() => es.getChecksumMap())
      .subscribe(
        map => {
          expect(map).to.has.key((dataset.dcat as Dataset).identifier);
        },
        err => done(err),
        () => done()
      );
  });
});
