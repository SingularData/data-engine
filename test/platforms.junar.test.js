import chai from 'chai';
import Rx from 'rxjs';
import { download, downloadAll, __RewireAPI__ as ToDosRewireAPI } from '../src/platforms/junar';
import { validateMetadata } from './database.test';

const expect = chai.expect;
const dataset = require('./dataset/junar.json');

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
        observer.next({});
        observer.complete();
      });
    });

    let datasetCount = 0;

    downloadAll()
      .subscribe((data) => {
        expect(data).to.be.an('object');
        datasetCount += 1;
      },
      null,
      () => {
        expect(datasetCount).to.equal(1);
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
                dataset
              ]
            }
          });
        }
      }
    });

    download({ id: 1, url: 'test' })
      .subscribe(
        (result) => validateMetadata(result),
        null,
        () => done()
      );
  });

});
