import { Observable } from 'rxjs';
import { noop } from 'lodash';
import { clear as clearDB } from '../src/database';
import { clear as clearES } from '../src/elasticsearch';

Observable.merge(clearDB(), clearES())
  .subscribe(noop, (err) => {
    console.error(err);
  }, () => {
    console.log('complete!');
    process.exit();
  });
