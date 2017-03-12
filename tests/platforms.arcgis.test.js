import { download, downloadAll, __RewireAPI__ as ToDosRewireAPI } from '../src/platforms/arcgis';
import { validateMetadata } from './database.test';
import Promise from 'bluebird';
import chai from 'chai';

chai.use(require('chai-as-promised'));

const expect = chai.expect;

describe('platfoms/arcgis.js', () => {

  it('download() should return a list of valid dataset metadata, with provided portal ID and URL.', () => {
    let requestCount = 0;

    ToDosRewireAPI.__Rewire__('rp', request => {
      if(request.uri.endsWith('per_page=0')) {
        return Promise.resolve({
          stats: {
            total_count: 200
          }
        });
      } else {
        requestCount++;
        return Promise.resolve({
          data: [
            {
              name: 'test',
              id: '231',
              created_at: '2014-06-24T06:52:24.000Z',
              updated_at: '2014-06-24T06:52:24.000Z',
              description: 'test description',
              license: 'MIT',
              sites: [
                { title: 'data portal' }
              ],
              tags: ['test']
            }
          ]
        });
      }
    });

    return download(1, 'testUrl')
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
        any: () => Promise.resolve([{ id: 1, url: 'test' }])
      };
    });

    return downloadAll()
      .then(tasks => {
        expect(tasks).to.be.an('array');
        expect(tasks).to.have.lengthOf(1);
      });
  });

});
