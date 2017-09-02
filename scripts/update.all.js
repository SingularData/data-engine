import { Observable } from 'rxjs';
import { harvestAll } from '../src/harvester';
import { reindex, ensureIndex } from '../src/elasticsearch';
import _ from 'lodash';

let collectData = harvestAll({ updateES: false });

Observable.concat(ensureIndex(), collectData, reindex())
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
