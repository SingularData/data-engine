import { download, chunkDonwload, downloadAll, __RewireAPI__ as ToDosRewireAPI } from '../src/platforms/ckan';
import { validateMetadata } from './database.test';
import Promise from 'bluebird';
import chai from 'chai';

chai.use(require('chai-as-promised'));

const expect = chai.expect;

describe('platfoms/ckan.js', () => {

  it('download() should return a function for harvesting, with provided portal ID and URL.', () => {
    let requestCount = 0;

    ToDosRewireAPI.__Rewire__('rp', request => {
      if(request.uri.endsWith('rows=0')) {
        return Promise.resolve({
          result: {
            count: 2000
          }
        });
      } else {
        requestCount++;
        return Promise.resolve({
          result: {
            results: [
              {
                title: 'test',
                id: '231',
                created_at: '2014-06-24T06:52:24.000',
                updated_at: '2014-06-24T06:52:24.000',
                notes: 'test description',
                license_title: 'MIT',
                groups: [
                  { display_name: 'data portal' }
                ],
                tags: [
                  { display_name: 'test' }
                ]
              }
            ]
          }

        });
      }
    });

    let task = download(1, 'testUrl');

    return task()
      .then(results => {
        expect(requestCount).to.equal(2);
        expect(results).to.have.lengthOf(2);
        validateMetadata(results[0]);
      });
  });

  it('chunkDonwload() should return an array of functions for harvesting, with provided portal ID and URL.', () => {
    let requestCount = 0;

    ToDosRewireAPI.__Rewire__('rp', request => {
      if(request.uri.endsWith('rows=0')) {
        return Promise.resolve({
          result: {
            count: 2000
          }
        });
      } else {
        requestCount++;
        return Promise.resolve({
          result: {
            results: [
              {
                title: 'test',
                id: '231',
                created_at: '2014-06-24T06:52:24.000',
                updated_at: '2014-06-24T06:52:24.000',
                notes: 'test description',
                license_title: 'MIT',
                groups: [
                  { display_name: 'data portal' }
                ],
                tags: [
                  { display_name: 'test' }
                ]
              }
            ]
          }

        });
      }
    });

    let getJobs = chunkDonwload(1, 'testUrl');

    return getJobs.then(results => {
      expect(results).to.be.an('array');
      expect(results).to.have.lengthOf(2);

      return Promise.all([results[0](), results[1]()]);
    })
    .then(results => {
      expect(requestCount).to.equal(2);
      expect(results).to.have.lengthOf(2);
      validateMetadata(results[0][0]);
    });
  });

  it('downloadAll(false) should return an array of Promise tasks.', () => {
    ToDosRewireAPI.__Rewire__('donwload', () => Promise.resolve([]));
    ToDosRewireAPI.__Rewire__('getDB', () => {
      return {
        any: () => Promise.resolve([{ id: 1, url: 'test' }])
      };
    });

    return downloadAll()
      .then(tasks => {
        expect(tasks).to.be.an('array');
        expect(tasks).to.have.lengthOf(1);
      });
  });

  it('downloadAll(true) should return an array of Promise tasks.', () => {
    ToDosRewireAPI.__Rewire__('chunkDonwload', () => Promise.resolve([
      () => Promise.resolve([]),
      () => Promise.resolve([])
    ]));
    ToDosRewireAPI.__Rewire__('getDB', () => {
      return {
        any: () => Promise.resolve([{ id: 1, url: 'test' }])
      };
    });

    return downloadAll(true)
      .then(tasks => {
        expect(tasks).to.be.an('array');
        expect(tasks).to.have.lengthOf(2);
      });
  });

});
