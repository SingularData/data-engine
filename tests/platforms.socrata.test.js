import { download, downloadAll, __RewireAPI__ as ToDosRewireAPI } from '../src/platforms/socrata';
import { validateMetadata } from './database.test';
import Promise from 'bluebird';
import chai from 'chai';

chai.use(require('chai-as-promised'));

const expect = chai.expect;

describe('platfoms/socrata.js', () => {

  it('download() should return a function for harvesting, with provided portal ID and URL.', () => {
    let requestCount = 0;

    ToDosRewireAPI.__Rewire__('rp', request => {
      if(request.uri.endsWith('limit=0')) {
        return Promise.resolve({
          resultSetSize: 200
        });
      } else {
        requestCount++;
        return Promise.resolve({
          results: [
            {
              resource: {
                name: 'test',
                id: '231',
                createdAt: '2014-06-24T06:52:24.000Z',
                updatedAt: '2014-06-24T06:52:24.000Z',
                description: 'test description',
                metadata: {
                  license: 'MIT',
                  domain: 'localhost'
                },
                permalink: 'localhost',
                classification: {
                  tags: ['test'],
                  categories: ['test'],
                  domain_tags: [],
                  domain_category: 'test2'
                }
              }
            }
          ]
        });
      }
    });

    let task = download(1, 'testUrl', 'us');

    return task()
      .then(results => {
        expect(requestCount).to.equal(2);
        expect(results).to.have.lengthOf(2);
        validateMetadata(results[0]);
      });
  });

  it('downloadAll() should return an array of Promise tasks.', () => {
    ToDosRewireAPI.__Rewire__('donwload', () => Promise.resolve([]));
    ToDosRewireAPI.__Rewire__('getDB', () => {
      return {
        any: () => Promise.resolve([{ id: 1, url: 'test', region: 'eu' }])
      };
    });

    return downloadAll()
      .then(tasks => {
        expect(tasks).to.be.an('array');
        expect(tasks).to.have.lengthOf(1);
      });
  });

});
