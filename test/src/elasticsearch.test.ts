// import fetch, { RequestInit } from "node-fetch";
// import * as config from "config";
// import * as es from "../../src/elasticsearch";
// import { expect } from "chai";
// import { Dataset } from "w3c-dcat";
// import { sha256 } from "../../src/utils/hash-util";
// import s = require("sleep");
// import fse = require("fs-extra");
//
// describe("elasticsearch.ts", function() {
//   this.timeout(10000);
//
//   const dkan = fse.readJsonSync(__dirname + "/../mock/dkan-dataset.json");
//   const dataset = {
//     type: "dkan",
//     dcat: Dataset.from("DKAN", dkan),
//     checksum: sha256(JSON.stringify(dkan)),
//     original: dkan
//   };
//
//   before(done => {
//     fetch(config.get("elasticsearch.url"))
//       .then(() => done())
//       .catch(() => done(new Error("ElasticSearch is not running")));
//   });
//
//   it("deleteIndex() should drop existing index.", done => {
//     es
//       .ensureIndex()
//       .concatMap(() => es.deleteIndex())
//       .concatMap(() => es.indexExists())
//       .subscribe(
//         exists => expect(exists).to.be.false,
//         err => done(err),
//         () => done()
//       );
//   });
//
//   it("ensureIndex() should create non-existing index.", done => {
//     es
//       .deleteIndex()
//       .concatMap(() => es.ensureIndex())
//       .concatMap(() => es.indexExists())
//       .subscribe(
//         exists => expect(exists).to.be.true,
//         err => done(err),
//         () => done()
//       );
//   });
//
//   it("upsert() should create a new document.", done => {
//     es
//       .ensureIndex()
//       .concatMap(() => es.upsert([dataset]))
//       .do(() => s.sleep(2))
//       .concatMap(() => es.getChecksumMap())
//       .subscribe(
//         map => {
//           expect(map[(dataset.dcat as Dataset).identifier]).to.be.a("string");
//         },
//         err => done(err),
//         () => done()
//       );
//   });
// });
