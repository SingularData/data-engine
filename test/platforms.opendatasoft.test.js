import chai from 'chai';
import Rx from 'rxjs';
import config from 'config';
import { download, downloadAll, __RewireAPI__ as ToDosRewireAPI } from '../src/platforms/opendatasoft';
import { validateMetadata } from './database.test';

const rows = config.get('platforms.OpenDataSoft.rows');
const expect = chai.expect;
const dataset = require('./dataset/opendatasoft.json');

describe('platforms/opendatasoft.js', () => {

  it('download() should return a stream of dataset', (done) => {
    ToDosRewireAPI.__Rewire__('RxHR', {
      get: () => {
        return Rx.Observable.of({
          body: {
            datasets: [
              dataset,
              dataset
            ]
          }
        });
      }
    });

    let portals = {
      'OPEN DATA RTE': { id: 1 }
    };
    let datasetCount = 0;

    download('https://data.opendatasoft.com/api/v2/catalog/datasets?rows=1&start=0', portals)
      .subscribe(
        (result) => {
          datasetCount++;
          validateMetadata(result);
        },
        (err) => console.error(err),
        () => {
          expect(datasetCount).to.equal(2);
          done();
        }
      );
  });

  it('download() should skip the result if the portal is unrecognized.', (done) => {
    ToDosRewireAPI.__Rewire__('RxHR', {
      get: () => {
        return Rx.Observable.of({
          body: {
            datasets: [
              dataset
            ]
          }
        });
      }
    });

    let datasetCount = 0;

    download('https://data.opendatasoft.com/api/v2/catalog/datasets?rows=1&start=0', {})
      .subscribe(
        () => {
          datasetCount++;
        },
        null,
        () => {
          expect(datasetCount).to.equal(0);
          done();
        }
      );
  });

  it('downloadAll() should return a series of datasets.', (done) => {

    let datasetCount = 450;

    ToDosRewireAPI.__Rewire__('getDB', () => {
      return {
        query: () => Rx.Observable.create((observer) => {
          observer.next({ id: 1, url: 'testUrl', name: 'test portal', key: '34lkh' });
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

    ToDosRewireAPI.__Rewire__('RxHR', {
      get: () => {
        return Rx.Observable.of({
          body: {
            total_count: datasetCount,
          }
        });
      }
    });

    let requestCount = 0;

    downloadAll()
      .subscribe((data) => {
        expect(data).to.be.an('object');
        requestCount++;
      },
      null,
      () => {
        expect(requestCount).to.equal(Math.ceil(datasetCount / rows));
        done();
      });
  });

});
