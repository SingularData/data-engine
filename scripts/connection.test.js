import { getDB } from '../src/database';
import { getClient } from '../src/elasticsearch';

getDB();
let es = getClient();

es.ping()
  .then(() => {
    console.log('Connection Established!');
    process.exit();
  })
  .catch((err) => {
    console.error(err);
    process.exit();
  });
