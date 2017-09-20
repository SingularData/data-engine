import { Observable } from 'rxjs';
import { harvestAll } from '../src/harvester';
import { ensureIndex } from '../src/elasticsearch';
import _ from 'lodash';

let collectData = harvestAll({ updateES: true });

Observable.concat(ensureIndex(), collectData)
  .subscribe(
    _.noop,
    (error) => {
      console.error(error);
      process.exit();
    },
    () => {
      console.log('completed!');
      process.exit();
    }
  );
