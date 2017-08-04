import { Observable } from 'rxjs';
import { harvestAll } from '../src/harvester';
import { reindex } from '../src/elasticsearch';
import _ from 'lodash';

let collectData = harvestAll({ updateES: false });
let refreshES = reindex();

Observable.concat(collectData, refreshES)
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
