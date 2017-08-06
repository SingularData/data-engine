import chai from 'chai';
import Rx from 'rxjs';
import config from 'config';
import { download, downloadAll, __RewireAPI__ as ToDosRewireAPI } from '../src/platforms/opendatasoft';
import { validateMetadata } from './database.test';

const rows = config.get('platforms.OpenDataSoft.rows');
const expect = chai.expect;

describe('platforms/opendatasoft.js', () => {

  it('download() should return a stream of dataset', (done) => {
    ToDosRewireAPI.__Rewire__('RxHR', {
      get: () => {
        return Rx.Observable.of({
          body: {
            datasets: [
              {
                "dataset": {
                  "dataset_id": "evolution_region_longueurs_circuits_files@rte",
                  "metas": {
                    "default": {
                      "publisher": "RTE",
                      "source_domain_address": "rte.opendatasoft.com",
                      "license": "Licence Ouverte (Etalab)",
                      "source_domain": "rte",
                      "source_dataset": "evolution_region_longueurs_circuits_files",
                      "modified": "2016-07-04T12:28:31+00:00",
                      "theme": [
                        "Réseau",
                        "Territoires et régions"
                      ],
                      "metadata_processed": "2016-09-02T11:52:16+00:00",
                      "keyword": [
                        "Région",
                        "Bilan électrique"
                      ],
                      "source_domain_title": "OPEN DATA RTE",
                      "data_processed": "2016-09-02T11:52:15+00:00",
                      "title": "Evolutions régionales annuelles  des longueurs de circuits et files de pylônes du réseau de transport d'électricité (2013 à 2015)",
                      "description": "test data"
                    }
                  }
                }
              },
              {
                "dataset": {
                  "dataset_id": "evolution_region_longueurs_circuits_files@rte",
                  "metas": {
                    "default": {
                      "publisher": "RTE",
                      "source_domain_address": "rte.opendatasoft.com",
                      "license": "Licence Ouverte (Etalab)",
                      "source_domain": "rte",
                      "source_dataset": "evolution_region_longueurs_circuits_files",
                      "modified": "2016-07-04T12:28:31+00:00",
                      "theme": [
                        "Réseau",
                        "Territoires et régions"
                      ],
                      "metadata_processed": "2016-09-02T11:52:16+00:00",
                      "keyword": [
                        "Région",
                        "Bilan électrique"
                      ],
                      "source_domain_title": "OPEN DATA RTE",
                      "data_processed": "2016-09-02T11:52:15+00:00",
                      "title": "Evolutions régionales annuelles  des longueurs de circuits et files de pylônes du réseau de transport d'électricité (2013 à 2015)",
                      "description": "test data"
                    }
                  }
                }
              }
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
              {
                "dataset": {
                  "dataset_id": "evolution_region_longueurs_circuits_files@rte",
                  "has_records": true,
                  "attachments": [],
                  "data_visible": true,
                  "metas": {
                    "default": {
                      "publisher": "RTE",
                      "source_domain_address": "rte.opendatasoft.com",
                      "license": "Licence Ouverte (Etalab)",
                      "language": "fr",
                      "records_count": 180,
                      "source_domain": "rte",
                      "source_dataset": "evolution_region_longueurs_circuits_files",
                      "modified": "2016-07-04T12:28:31+00:00",
                      "theme": [
                        "Réseau",
                        "Territoires et régions"
                      ],
                      "metadata_processed": "2016-09-02T11:52:16+00:00",
                      "keyword": [
                        "Electricité",
                        "Circuit",
                        "Réseau",
                        "File de pylônes",
                        "Territoire",
                        "Région",
                        "Bilan électrique"
                      ],
                      "source_domain_title": "OPEN DATA RTE",
                      "data_processed": "2016-09-02T11:52:15+00:00",
                      "title": "Evolutions régionales annuelles  des longueurs de circuits et files de pylônes du réseau de transport d'électricité (2013 à 2015)",
                      "description": "test data"
                    }
                  },
                  "features": [
                    "geo",
                    "analyze",
                    "timeserie"
                  ]
                }
              }
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
