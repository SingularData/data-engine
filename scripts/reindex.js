import { reindex } from '../src/elasticsearch.js';

reindex()
  .subscribe(
    () => {},
    (err) => {
      console.error(err);
      process.exit();
    },
    () => {
      console.log('complete');
      process.exit();
    }
  );
