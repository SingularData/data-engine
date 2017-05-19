// import chai from 'chai';
// import Rx from 'rxjs';
// import { download, downloadAll, __RewireAPI__ as ToDosRewireAPI } from '../src/platforms/geonode';
// import { validateMetadata } from './database.test';
//
// const expect = chai.expect;
//
// describe('platfoms/geonode.js', () => {
//
//   it('downloadAll() should return an observable of dataset stream.', (done) => {
//     ToDosRewireAPI.__Rewire__('getDB', () => {
//       return {
//         query: () => Rx.Observable.create((observer) => {
//           observer.next({ id: 1, name: 'test', url: 'testUrl' });
//           observer.complete();
//         })
//       };
//     });
//
//     ToDosRewireAPI.__Rewire__('download', () => {
//       return Rx.Observable.create((observer) => {
//         observer.next([1]);
//         observer.complete();
//       });
//     });
//
//     let requestCount = 0;
//
//     downloadAll()
//       .subscribe((data) => {
//         expect(data).to.be.an('array');
//         requestCount += 1;
//       },
//       null,
//       () => {
//         expect(requestCount).to.equal(1);
//         done();
//       });
//   });
//
//   it('download() should return an observable of dataset stream , with provided portal ID and URL.', (done) => {
//     ToDosRewireAPI.__Rewire__('RxHR', {
//       get: () => Rx.Observable.of({
//         body: {
//           "meta": {
//             "limit": 0,
//             "offset": 0,
//             "total_count": 25
//           },
//           "objects": [
//             {
//               "abstract": "This is the detailed version of the detailed Large Scale International Boundaries (LSIB) dataset.\r\n\r\nThe boundary lines reflect all current US government policies on boundaries, boundary disputes, and sovereignty.\r\n\r\nThere are no restrictions on use of this public domain data. This dataset will be updated as needed and is current as of Jan 30, 2017.",
//               "category__gn_description": "Boundaries",
//               "csw_type": "dataset",
//               "csw_wkt_geometry": "POLYGON((-141.0019559999999 -55.12502199499994,-141.0019559999999 69.64744868500009,48.001056000000176 69.64744868500009,48.001056000000176 -55.12502199499994,-141.0019559999999 -55.12502199499994))",
//               "date": "2017-01-30T02:44:00",
//               "detail_url": "/layers/geonode%3AAfrica_Americas_LSIB7a",
//               "distribution_description": "Web address (URL)",
//               "distribution_url": "http://geonode.state.gov/layers/geonode%3AAfrica_Americas_LSIB7a",
//               "id": 50,
//               "owner__username": "hiu",
//               "popular_count": 105,
//               "rating": 0,
//               "share_count": 0,
//               "srid": "EPSG:4326",
//               "supplemental_information": "No information provided",
//               "thumbnail_url": "http://geonode.state.gov/uploaded/thumbs/layer-57523940-e988-11e6-ae85-0a06d512a67a-thumb.png",
//               "title": "Africa Americas LSIB Lines Detailed 2017Jan30",
//               "uuid": "57523940-e988-11e6-ae85-0a06d512a67a"
//             }
//           ]
//         }
//       })
//     });
//
//     let requestCount = 0;
//
//     download(1, 'testPortal', 'testUrl')
//       .subscribe((results) => {
//         requestCount += 1;
//         expect(results).to.have.lengthOf(1);
//         validateMetadata(results[0]);
//       },
//       null,
//       () => {
//         expect(requestCount).to.equal(1);
//         done();
//       }
//       );
//   });
//
// });
