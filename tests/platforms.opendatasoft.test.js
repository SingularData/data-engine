import { download, downloadAll, __RewireAPI__ as ToDosRewireAPI } from '../src/platforms/opendatasoft';
import { validateMetadata } from './database.test';
import Promise from 'bluebird';
import chai from 'chai';

chai.use(require('chai-as-promised'));

const expect = chai.expect;

describe('platforms/opendatasoft.js', () => {

  it('download() should return a list of valid dataset metadata, with provided portal IDs.', () => {
    let portalIDs = {
      'OPEN DATA RTE': 1
    };

    return download('https://data.opendatasoft.com/api/v2/catalog/datasets?rows=1&start=0', portalIDs)
      .then(results => {
        expect(results).to.have.lengthOf(1);
        validateMetadata(results[0]);
      });
  });

  it('download() should return a list of valid dataset metadata, without provided portal IDs.', () => {
    ToDosRewireAPI.__Rewire__('getDB', () => {
      return {
        any: () => Promise.resolve([{ id: 1, name: 'OPEN DATA RTE' }])
      };
    });

    return download('https://data.opendatasoft.com/api/v2/catalog/datasets?rows=1&start=0')
      .then(results => {
        expect(results).to.have.lengthOf(1);
        validateMetadata(results[0]);
      });
  });

  it('download() should skip the result if the portal is unrecognized.', () => {
    return download('https://data.opendatasoft.com/api/v2/catalog/datasets?rows=1&start=0', {})
      .then(results => {
        expect(results).to.have.lengthOf(0);
      });
  });

  it('downloadAll() should return an array of Promise tasks', () => {
    ToDosRewireAPI.__Rewire__('harvest', () => Promise.resolve([]));
    ToDosRewireAPI.__Rewire__('getDB', () => {
      return {
        any: () => Promise.resolve([])
      };
    });

    return downloadAll()
      .then(tasks => {
        expect(tasks).to.be.an('array');
      });
  });

});
