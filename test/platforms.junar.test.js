import chai from 'chai';
import Rx from 'rxjs';
import { download, downloadAll, __RewireAPI__ as ToDosRewireAPI } from '../src/platforms/junar';
import { validateMetadata } from './database.test';

const expect = chai.expect;

describe('platfoms/junar.js', () => {

  it('downloadAll() should return an observable of dataset stream.', (done) => {
    ToDosRewireAPI.__Rewire__('getDB', () => {
      return {
        query: () => Rx.Observable.create((observer) => {
          observer.next({ id: 1, name: 'test portal', url: 'testUrl', apiKey: '234sdr' });
          observer.complete();
        })
      };
    });

    ToDosRewireAPI.__Rewire__('download', () => {
      return Rx.Observable.create((observer) => {
        observer.next([1]);
        observer.complete();
      });
    });

    let requestCount = 0;

    downloadAll()
      .subscribe((data) => {
        expect(data).to.be.an('array');
        requestCount += 1;
      },
      null,
      () => {
        expect(requestCount).to.equal(1);
        done();
      });
  });

  it('download() should return an observable of dataset stream , with provided portal ID and URL.', (done) => {
    ToDosRewireAPI.__Rewire__('RxHR', {
      get: (url) => {
        if (url.endsWith('limit=1')) {
          return Rx.Observable.of({
            body: {
              count: 190
            }
          });
        } else {
          return Rx.Observable.of({
            body: {
              results: [
                {
                  "result": null,
                  "endpoint": "file://4575/0475/75487840637754682629754469421296137673",
                  "description": "Annual Crime Statistics",
                  "parameters": [],
                  "tags": [
                    ""
                  ],
                  "timestamp": 1486742469000,
                  "created_at": 1486742469,
                  "title": "Sacramento Annual Crime Statistics",
                  "modified_at": 1486742469,
                  "category_id": "38920",
                  "link": "http://data.cityofsacramento.org/datasets/72416/sacramento-annual-crime-statistics/",
                  "user": "sacramento",
                  "guid": "SACRA-ANNUA-CRIME-STATI-90418",
                  "category_name": "Public Safety"
                }
              ]
            }
          });
        }
      }
    });

    let requestCount = 0;

    download(1, 'testUrl')
      .subscribe((results) => {
        requestCount += 1;
        expect(results).to.have.lengthOf(1);
        validateMetadata(results[0]);
      },
      null,
      () => {
        expect(requestCount).to.equal(2);
        done();
      }
      );
  });

});
