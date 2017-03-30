import chai from 'chai';
import Rx from 'rxjs';
import { download, downloadAll, __RewireAPI__ as ToDosRewireAPI } from '../src/platforms/socrata';
import { validateMetadata } from './database.test';

const expect = chai.expect;

describe('platfoms/socrata.js', () => {

  it('downloadAll() should return an observable of dataset stream.', (done) => {
    ToDosRewireAPI.__Rewire__('getDB', () => {
      return {
        query: () => Rx.Observable.create((observer) => {
          observer.next({ id: 1, url: 'testUrl', name: 'test portal', region: 'eu' });
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
      .subscribe(() => {
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
        if (url.endsWith('limit=0')) {
          return Rx.Observable.of({
            body: {
              resultSetSize: 190
            }
          });
        } else {
          return Rx.Observable.of({
            body: {
              results: [
                {
                  "resource": {
                    "view_count": {
                      "page_views_total": 3670365,
                      "page_views_total_log": 21.807492501618096,
                      "page_views_last_week_log": 1.5849625007211563,
                      "page_views_last_month": 15,
                      "page_views_last_week": 2,
                      "page_views_last_month_log": 4
                    },
                    "obe_fxf": null,
                    "description": "Calls for Service by computer aided dispatch (CAD) event type, date, time, location, statistical reporting area (SRA), and beat.",
                    "name": "Howard County Police Department Call For Service - 2014 - 2015",
                    "parent_fxf": null,
                    "nbe_fxf": null,
                    "attribution": null,
                    "provenance": "official",
                    "columns_field_name": [
                      "statistical_reporting_area"
                    ],
                    "download_count": 175,
                    "columns_name": [
                      "Statistical_Reporting_Area"
                    ],
                    "page_views": {
                      "page_views_total": 3670365,
                      "page_views_total_log": 21.807492501618096,
                      "page_views_last_week_log": 1.5849625007211563,
                      "page_views_last_month": 15,
                      "page_views_last_week": 2,
                      "page_views_last_month_log": 4
                    },
                    "updatedAt": "2016-03-24T17:56:41.000Z",
                    "type": "dataset",
                    "id": "qccx-65fg",
                    "createdAt": "2015-05-04T15:33:31.000Z",
                    "columns_description": [
                      ""
                    ]
                  },
                  "classification": {
                    "categories": [],
                    "tags": [],
                    "domain_category": "Public Safety",
                    "domain_tags": [
                      "beat"
                    ],
                    "domain_metadata": [
                      {
                        "value": "Howard County, MD",
                        "key": "Additional-Metadata_Publisher"
                      }
                    ]
                  },
                  "metadata": {
                    "domain": "opendata.howardcountymd.gov"
                  },
                  "permalink": "https://opendata.howardcountymd.gov/d/qccx-65fg",
                  "link": "https://opendata.howardcountymd.gov/Public-Safety/Howard-County-Police-Department-Call-For-Service-2/qccx-65fg"
                }
              ]
            }
          });
        }
      }
    });

    let requestCount = 0;

    download(1, 'test portal', 'testURL', 'eu')
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
