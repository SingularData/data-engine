import chai from 'chai';
import Rx from 'rxjs';
import { download, downloadAll, __RewireAPI__ as ToDosRewireAPI } from '../src/platforms/dkan';
import { validateMetadata } from './database.test';

const expect = chai.expect;

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
        observer.next([1]);
        observer.complete();
      });
    });

    let requestCount = 0;

    downloadAll()
      .map((x) => {
        requestCount += 1;
        return x;
      })
      .subscribe((results) => {
        expect(results).to.deep.equal(results);
      },
      null,
      () => {
        expect(requestCount).to.equal(1);
        done();
      });
  });

  it('download() should return an observable of dataset stream , with provided portal ID and URL.', (done) => {
    ToDosRewireAPI.__Rewire__('RxHR', {
      get: () => Rx.Observable.of([
        {
          "@type": "dcat:Dataset",
          "accessLevel": "public",
          "contactPoint": {
            "fn": "Gray, Stefanie",
            "hasEmail": "mailto:datademo@nucivic.com"
          },
          "description": "<p>This data created by the National Democratic Institute (NDI) in partnership with Development Seed, a Washington, D.C.-based online communications consultancy, is designed to make data from the August 20, 2009, Afghanistan presidential election accessible and transparent. We have provided this csv file as there is no download for the whole dataset. There are many more files on the website.</p>\n",
          "distribution": [
            {
              "@type": "dcat:Distribution",
              "downloadURL": "http://demo.getdkan.com/sites/default/files/district_centerpoints_0.csv",
              "mediaType": "text/csv",
              "format": "csv",
              "description": "<p>You can see this data plotted on a map, by clicking on 'Map' below. Individual data records can be seen by clicking on each point.</p>\n",
              "title": "District Names"
            }
          ],
          "identifier": "c9e2d352-e24c-4051-9158-f48127aa5692",
          "issued": "2012-10-30",
          "keyword": [
            "country-afghanistan",
            "election",
            "politics",
            "transparency"
          ],
          "license": "http://opendefinition.org/licenses/odc-by/",
          "modified": "2017-01-13",
          "publisher": {
            "@type": "org:Organization",
            "name": "Committee on International Affairs"
          },
          "spatial": "POLYGON ((60.8642578125 29.878755346038, 61.787109375 30.977609093349, 61.7431640625 31.391157522825, 60.7763671875 31.653381399664, 60.8642578125 32.361403315275, 60.556640625 33.137551192346, 60.908203125 33.578014746144, 60.5126953125 33.614619292334, 60.5126953125 34.307143856288, 60.8642578125 34.343436068483, 61.3037109375 35.603718740697, 62.666015625 35.353216101238, 64.5556640625 36.421282443649, 64.86328125 37.195330582801, 65.654296875 37.195330582801, 65.654296875 37.474858084971, 67.9833984375 37.055177106661, 68.7744140625 37.265309955619, 69.345703125 37.125286284967, 69.697265625 37.683820326694, 70.3125 37.683820326694, 70.3125 38.065392351332, 71.0595703125 38.582526159353, 71.279296875 37.78808138412, 71.7626953125 37.926867601481, 71.4990234375 37.405073750177, 71.630859375 36.738884124394, 73.3447265625 37.474858084971, 73.7841796875 37.474858084971, 73.7841796875 37.265309955619, 74.7509765625 37.335224359306, 74.9267578125 37.160316546737, 74.1357421875 36.809284702059, 72.9931640625 36.949891786813, 71.54296875 36.315125147481, 71.2353515625 36.066862132579, 71.5869140625 35.460669951495, 71.54296875 34.813803317113, 71.103515625 34.560859367084, 71.1474609375 34.08906131585, 70.3125 33.979808728725, 69.873046875 33.979808728725, 70.2685546875 33.321348526699, 69.345703125 32.916485347314, 69.2578125 32.324275588877, 69.1259765625 31.802892586707, 68.7744140625 31.578535426473, 68.0712890625 31.802892586707, 67.67578125 31.615965936476, 67.8515625 31.353636941501, 67.4560546875 31.278550858947, 67.0166015625 31.278550858947, 66.4892578125 30.939924331023, 66.2255859375 30.334953881989, 66.357421875 29.91685223307, 65.0390625 29.535229562948, 64.248046875 29.611670115197, 64.2041015625 29.420460341013, 63.4130859375 29.458731185355, 62.6220703125 29.420460341013))",
          "title": "Afghanistan Election Districts"
        }
      ])
    });

    let requestCount = 0;

    download(1, 'testPortal', 'testUrl')
      .map((x) => {
        requestCount += 1;
        return x;
      })
      .subscribe((results) => {
        expect(results).to.have.lengthOf(1);
        validateMetadata(results[0]);
      },
      null,
      () => {
        expect(requestCount).to.equal(1);
        done();
      }
      );
  });

});