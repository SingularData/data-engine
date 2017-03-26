import chai from 'chai';
import Rx from 'rxjs';
import { download, downloadAll, __RewireAPI__ as ToDosRewireAPI } from '../src/platforms/arcgis';
import { validateMetadata } from './database.test';

const expect = chai.expect;

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
        if (url.endsWith('per_page=0')) {
          return Rx.Observable.of({
            body: {
              metadata: {
                stats: {
                  total_count: 200
                }
              }
            }
          });
        } else {
          return Rx.Observable.of({
            body: {
              data: [
                {
                  "id": "3e44363e5b4b41fcaeb607c5ef15ed39",
                  "landing_page": "https://www.arcgis.com/home/item.html?id=3e44363e5b4b41fcaeb607c5ef15ed39",
                  "description": "",
                  "extent": {
                    "coordinates": [
                      [
                        5.1352,
                        51.4861
                      ],
                      [
                        5.1391,
                        51.4876
                      ]
                    ]
                  },
                  "fields": [],
                  "item_name": "Paardenstraat 2A te Hilvarenbeek",
                  "type": "ItemMap",
                  "item_type": "Web Map",
                  "license": "",
                  "max_record_count": null,
                  "name": "Paardenstraat 2A te Hilvarenbeek",
                  "owner": "htimmers_AGEL",
                  "record_count": -1,
                  "tags": [
                    "20140253"
                  ],
                  "thumbnail_url": "https://www.arcgis.com/sharing/rest/content/items/3e44363e5b4b41fcaeb607c5ef15ed39/info/thumbnail/ago_downloaded.png",
                  "public": true,
                  "created_at": "2014-06-24T06:52:24.000Z",
                  "updated_at": "2014-07-10T16:29:58.000Z",
                  "url": "https://www.arcgis.com/home/webmap/viewer.html?webmap=3e44363e5b4b41fcaeb607c5ef15ed39",
                  "views": null,
                  "quality": 61,
                  "coverage": "local",
                  "current_version": "n/a",
                  "comments_enabled": true,
                  "service_spatial_reference": null,
                  "metadata_url": null,
                  "org_id": null,
                  "metadata": {
                    "published": null,
                    "present": false,
                    "url": null,
                    "online_resources": []
                  },
                  "structured_license": {
                    "type": "none"
                  },
                  "use_standardized_queries": false,
                  "advanced_query_capabilities": null,
                  "supported_extensions": "",
                  "sites": [
                    {
                      "title": "AGEL open data site",
                      "url": "http://agelopendatasite.agel.opendata.arcgis.com",
                      "logo": null
                    }
                  ],
                  "main_group_title": "AGEL adviseurs openbaar EXTERN",
                  "main_group_description": "",
                  "main_group_thumbnail_url": null
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
