import Rx from 'rxjs';
import chai from 'chai';
import { harvest, harvestAll, __RewireAPI__ as ToDosRewireAPI } from '../src/harvester';

const expect = chai.expect;

describe('harvester.js', () => {

  it('harvest() should finish a series download-save tasks.', (done) => {

    ToDosRewireAPI.__Rewire__('downlaodAllFn', {
      'DKAN': () => {
        return Rx.Observable.create((observer) => {
          observer.next(Rx.Observable.empty());
          observer.next(Rx.Observable.empty());
          observer.complete();
        });
      }
    });

    ToDosRewireAPI.__Rewire__('save', () => {
      return Rx.Observable.create((observer) => {
        observer.next(Rx.Observable.empty());
        observer.complete();
      });
    });

    let requestCount = 0;

    harvest('DKAN')
      .subscribe(
        () => { requestCount++; },
        null,
        () => {
          expect(requestCount).to.equal(2);
          done();
        }
      );
  });

  it('harvestAll() should finish a series harvest tasks.', (done) => {

    ToDosRewireAPI.__Rewire__('getDB', () => {
      return {
        query: () => Rx.Observable.of({ name: 'DKAN' })
      };
    });

    ToDosRewireAPI.__Rewire__('harvest', () => {
      return Rx.Observable.create((observer) => {
        observer.next(Rx.Observable.empty());
        observer.next(Rx.Observable.empty());
        observer.complete();
      });
    });

    let requestCount = 0;

    harvestAll()
      .subscribe(
        () => { requestCount++; },
        null,
        () => {
          expect(requestCount).to.equal(2);
          done();
        }
      );
  });
});
