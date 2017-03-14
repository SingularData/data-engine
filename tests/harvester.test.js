import { harvest, harvestAll, __RewireAPI__ as ToDosRewireAPI } from '../src/harvester';
import Promise from 'bluebird';
import chai from 'chai';

chai.use(require('chai-as-promised'));
chai.should();

const expect = chai.expect;

describe('harvester.js', () => {

  it('harvest() should reject unknown platform name.', () => {
    return harvest().should.be.rejected;
  });

  it('harvest() should download and save metadata if a platform name is given.', () => {

    let finished = 0;

    ToDosRewireAPI.__Rewire__('downlaodAllFn', {
      OpenDataSoft: () => Promise.resolve([() => Promise.resolve([null])])
    });

    ToDosRewireAPI.__Rewire__('save', () => {
      finished += 1;
      return Promise.resolve();
    });

    return harvest('OpenDataSoft', undefined, undefined, () => {
      expect(finished).to.equal(1);
    });
  });

  it('harvestAll() should download and save the metadata from all platforms.', () => {

    let finished = 0;

    ToDosRewireAPI.__Rewire__('getDB', () => {
      return {
        any: () => Promise.resolve([{ name: 'OpenDataSoft' }])
      };
    });

    ToDosRewireAPI.__Rewire__('downlaodAllFn', {
      OpenDataSoft: () => Promise.resolve([() => Promise.resolve([null])])
    });

    ToDosRewireAPI.__Rewire__('save', () => {
      finished += 1;
      return Promise.resolve();
    });

    return harvestAll(undefined, undefined, () => {
      expect(finished).to.equal(1);
    });
  });

});
