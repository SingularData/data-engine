import Database = require("better-sqlite3");
import crypto = require("crypto");
import _ = require("lodash");

let db;

async function initialize(es) {
  db = new Database(process.env.DATASET_DB);

  // create dataset table
  db.prepare("CREATE TABLE dataset (md5)").run();
  db.prepare("CREATE INDEX ON dataset (md5)").run();

  // get all dataset md5 from the elastisearch
  const params = {
    index: process.env.ES_INDEX,
    scroll: "30s",
    source: ["md5"],
    q: "*"
  };

  return es.search(params).then(function getResults(result) {
    const values = _.map(result.hits.hits, "md5");
    const rows = _.map(result.hits.hits, () => "(?)");
    const query = `INSERT INTO dataset (md5) VALUES ${rows.join(",")}`;

    db.prepare(query).run(...values);
  });
}

function insert(dataset) {
  const hash = md5(dataset);
  db.prepare("INSERT INTO dataset (?)").run(hash);
}

function exists(dataset) {
  const hash = md5(dataset);
  return db
    .prepare("SELECT EXISTS (SELECT 1 FROM datast WHERE md5 = ?)")
    .get(hash).exists;
}

function close() {
  db.close();
}

function md5(data) {
  return crypto
    .createHash("md5")
    .update(JSON.stringify(data))
    .digest("hex");
}

export { insert, exists, close };
