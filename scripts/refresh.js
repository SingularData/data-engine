import { refreshDatabase } from '../src/database';
import _ from 'lodash';

refreshDatabase()
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
