import { Observable } from 'rxjs';
import { ensureIndex, reindex } from '../src/elasticsearch.js';

Observable.concat(ensureIndex(), reindex())
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
