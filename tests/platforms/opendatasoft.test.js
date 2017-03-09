import { harvest, harvestAll, __RewireAPI__ as ToDosRewireAPI } from '../../src/platforms/opendatasoft';
import { validateMetadata } from '../database.test';
import Promise from 'bluebird';
import chai from 'chai';

chai.use(require('chai-as-promised'));

const expect = chai.expect;

describe('platforms/opendatasoft.js', function() {

  this.timeout(30000);

  it('crawl() should return a list of valid dataset metadata, with provided portal IDs.', () => {
    let portalIDs = {
      'OPEN DATA RTE': 1
    };

    return harvest('https://data.opendatasoft.com/api/v2/catalog/datasets?rows=1&start=0', portalIDs)
      .then(results => {
        expect(results).to.have.lengthOf(1);
        validateMetadata(results[0]);
      });
  });

  it('crawl() should return a list of valid dataset metadata, without provided portal IDs.', () => {
    ToDosRewireAPI.__Rewire__('getDB', () => {
      return {
        any: () => Promise.resolve([{ id: 1, name: 'OPEN DATA RTE' }])
      };
    });

    return harvest('https://data.opendatasoft.com/api/v2/catalog/datasets?rows=1&start=0')
      .then(results => {
        expect(results).to.have.lengthOf(1);
        validateMetadata(results[0]);
      });
  });

  it('crawl() should skip the result if the portal is unrecognized.', () => {
    return harvest('https://data.opendatasoft.com/api/v2/catalog/datasets?rows=1&start=0', {})
      .then(results => {
        expect(results).to.have.lengthOf(0);
      });
  });

  it('crawlAll() should return an array of Promise tasks', () => {
    ToDosRewireAPI.__Rewire__('harvest', () => Promise.resolve([]));
    ToDosRewireAPI.__Rewire__('getDB', () => {
      return {
        any: () => Promise.resolve([])
      };
    });

    return harvestAll()
      .then(tasks => {
        expect(tasks).to.be.an('array');
      });
  });

});
