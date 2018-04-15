import { start } from "./subscription";
import * as queue from "./services/job-queue";
import * as es from "./services/es";
import * as contentMap from "./services/content-map";

console.log("content-map-initializing");

contentMap
  .initialize(es)
  .then(() => {
    console.log("content-map-initialized");
    start({ queue, es, contentMap });
  })
  .catch(err => {
    console.error(err);
    process.exit(1);
  });
