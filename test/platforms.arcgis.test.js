import chai from 'chai';
import Rx from 'rxjs';
import _ from 'lodash';
import { download, downloadAll, __RewireAPI__ as ToDosRewireAPI } from '../src/platforms/arcgis';
import { validateMetadata } from './database.test';

const expect = chai.expect;
const dataset = require('./dataset/arcgis.json');

describe('platfoms/arcgis.js', () => {

  it('downloadAll() should return an observable of dataset stream.', (done) => {
    ToDosRewireAPI.__Rewire__('getDB', () => {
      return {
        query: () => Rx.Observable.create((observer) => {
          observer.next({ id: 1, url: 'testUrl' });
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
      get: () => {
        return Rx.Observable.of({
          body: {
            dataset: [
              dataset
            ]
          }
        });
      }
    });

    download({ id: 1, url: 'test' })
      .subscribe((dataset) => {
        validateMetadata(dataset);
      }, _.noop, () => done());
  });

});
