import { download, downloadAll, __RewireAPI__ as ToDosRewireAPI } from '../src/platforms/junar';
import { validateMetadata } from './database.test';
import Promise from 'bluebird';
import chai from 'chai';

chai.use(require('chai-as-promised'));

const expect = chai.expect;

describe('platfoms/junar.js', () => {

  it('download() should return a function for harvesting, with provided portal ID and URL.', () => {
    let requestCount = 0;

    ToDosRewireAPI.__Rewire__('rp', request => {
      if(request.uri.endsWith('limit=1')) {
        return Promise.resolve({
          count: 200
        });
      } else {
        requestCount++;
        return Promise.resolve({
          results: [
            {
              title: 'test',
              guid: '231',
              created_at: 1486742469,
              modified_at: 1486742469,
              description: 'test description',
              link: 'localhost',
              category_name: 'test',
              tags: ['test']
            }
          ]
        });
      }
    });

    let task = download(1, 'Test Portal', 'API URL', 'API Key');

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
