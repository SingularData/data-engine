import { harvestAll } from '../src/harvester';
import _ from 'lodash';

harvestAll()
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
