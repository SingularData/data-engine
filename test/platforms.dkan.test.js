import chai from 'chai';
import Rx from 'rxjs';
import { download, downloadAll, __RewireAPI__ as ToDosRewireAPI } from '../src/platforms/dkan';
import { validateMetadata } from './database.test';

const expect = chai.expect;
const dataset = require('./dataset/dkan.json');

describe('platfoms/dkan.js', () => {

  it('downloadAll() should return an observable of dataset stream.', (done) => {
    ToDosRewireAPI.__Rewire__('getDB', () => {
      return {
        query: () => Rx.Observable.create((observer) => {
          observer.next({ id: 1, name: 'test', url: 'testUrl' });
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
      .subscribe(() => {
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
      get: () => Rx.Observable.of({
        body: [
          dataset
        ]
      })
    });

    download({ id: 1, name: 'portal', url: 'test' })
      .subscribe(
        (result) => validateMetadata(result),
        null,
        () => done()
      );
  });

});
