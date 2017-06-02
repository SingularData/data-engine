import Rx from 'rxjs';
import chai from 'chai';
import _ from 'lodash';
import { harvestPlatform, harvestAll, __RewireAPI__ as ToDosRewireAPI } from '../src/harvester';

const expect = chai.expect;

function dataset(portalID, portalDatasetID, updatedTime) {
  return { portalID, portalDatasetID, updatedTime, raw: {} };
}

describe('harvester.js', () => {

  it('harvest() should finish a series download-save tasks.', (done) => {

    ToDosRewireAPI.__Rewire__('downlaodAllFn', {
      'DKAN': () => {
        return Rx.Observable.create((observer) => {
          observer.next(dataset(1, '1', new Date()));
          observer.next(dataset(1, '2', new Date()));
          observer.complete();
        });
      }
    });

    ToDosRewireAPI.__Rewire__('save', (metadatas) => {
      expect(metadatas).to.have.lengthOf(2);

      return Rx.Observable.create((observer) => {
        observer.next(Rx.Observable.empty());
        observer.complete();
      });
    });

    ToDosRewireAPI.__Rewire__('getLatestCheckList', () => Rx.Observable.of({}));
    ToDosRewireAPI.__Rewire__('refreshDatabase', () => Rx.Observable.empty());
    ToDosRewireAPI.__Rewire__('upsert', () => Rx.Observable.empty());

    harvestPlatform('DKAN')
      .subscribe(
        _.noop,
        _.noop,
        () => done()
      );
  });

  it('harvestAll() should finish a series harvest tasks.', (done) => {

    ToDosRewireAPI.__Rewire__('getDB', () => {
      return {
        query: () => Rx.Observable.of({ name: 'DKAN' })
      };
    });

    ToDosRewireAPI.__Rewire__('harvestPlatform', () => {
      return Rx.Observable.create((observer) => {
        observer.next(Rx.Observable.empty());
        observer.next(Rx.Observable.empty());
        observer.complete();
      });
    });

    let datasetCount = 0;

    harvestAll()
      .subscribe(
        () => { datasetCount++; },
        null,
        () => {
          expect(datasetCount).to.equal(2);
          done();
        }
      );
  });
});
