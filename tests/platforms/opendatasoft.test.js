import { harvest, harvestAll, __RewireAPI__ as ToDosRewireAPI } from '../../src/platforms/opendatasoft';
import { validateMetadata } from '../database.test';
import Promise from 'bluebird';
import chai from 'chai';

chai.use(require('chai-as-promised'));

const expect = chai.expect;

describe('OpenDataSoft metadata crawler', function() {

  this.timeout(30000);

  it('crawl() should return a list of valid dataset metadata.', () => {
    return harvest('https://data.opendatasoft.com/api/v2/catalog/datasets?rows=1&start=0')
      .then(results => {
        expect(results).to.have.lengthOf(1);
        validateMetadata(results[0]);
      });
  });

  it('crawlAll() should return an array of Promise tasks', () => {
    ToDosRewireAPI.__Rewire__('harvest', () => Promise.resolve([]));

    return harvestAll()
      .then(tasks => {
        expect(tasks).to.be.an('array');
      });
  });

});
