import { start } from "./subscription";
import * as queue from "./services/job-queue";
import * as es from "./services/es";

start({ queue, es }).catch(err => {
  console.error(err);
  process.exit(1);
});
