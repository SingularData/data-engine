import chai from 'chai';
import Rx from 'rxjs';
import { download, downloadAll, __RewireAPI__ as ToDosRewireAPI } from '../src/platforms/opendatasoft';
import { validateMetadata } from './database.test';

const expect = chai.expect;

describe('platforms/opendatasoft.js', () => {

  it('download() should return a stream of dataset', (done) => {
    ToDosRewireAPI.__Rewire__('RxHR', {
      get: () => {
        return Rx.Observable.of({
          body: {
            datasets: [
              {
                "links": [
                  {
                    "href": "https://data.opendatasoft.com/api/v2/catalog/datasets/evolution_region_longueurs_circuits_files@rte",
                    "rel": "self"
                  },
                  {
                    "href": "https://data.opendatasoft.com/api/v2/catalog/datasets",
                    "rel": "datasets"
                  },
                  {
                    "href": "https://data.opendatasoft.com/api/v2/catalog/datasets/evolution_region_longueurs_circuits_files@rte/records",
                    "rel": "records"
                  },
                  {
                    "href": "https://data.opendatasoft.com/api/v2/catalog/datasets/evolution_region_longueurs_circuits_files@rte/exports",
                    "rel": "exports"
                  },
                  {
                    "href": "https://data.opendatasoft.com/api/v2/catalog/datasets/evolution_region_longueurs_circuits_files@rte/aggregates",
                    "rel": "aggregate"
                  }
                ],
                "dataset": {
                  "fields": [
                    {
                      "name": "circ_sout_63_kv_km",
                      "label": "Circ sout 63 kV (km)",
                      "type": "double",
                      "annotations": [],
                      "description": "Circuit souterrain 63 kV en km\n63 kV underground circuit in km"
                    }
                  ],
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
                      "description": "<p>\n\tEvolution  régionale annuelle des longueurs (km) de files de pylônes et de circuits en exploitation du réseau de transport d’électricité par niveau de tension (63 kV, 90 kV, 225 kV, 400 kV, ...) et par nature d’évolution (nouvelle infrastructure, renouvellement, mise en souterrain, ferraillée, autres modifications). Données définitives.</p><p>\n\tLes natures d'évolution d'un tronçon sont :</p><ul>\n\t\n<li>Nouveau : quand l’installation du tronçon fait l’objet d’un nouveau tracé (une nouvelle liaison par exemple)</li>\t\n<li>Renouvelé : quand un tronçon aérien est remplacé par un tronçon aérien ou quand un tronçon souterrain est remplacé par un tronçon souterrain</li>\t\n<li>Mis en souterrain : quand un tronçon aérien est remplacé par un tronçon souterrain\t </li>\t\n<li>Ferraillé : quand le tronçon a été retiré physiquement du réseau\t </li>\t\n<li>Autres modifications : Ce sont toutes les variations de longueur non recensées dans les tronçons neufs et ferraillés. Cela concerne principalement les tronçons mis en réserve (déconnectés du réseau mais toujours en place) mais aussi les corrections d’erreur ou de longueur</li></ul><hr/><p>\n<strong>Regional changes in lengths of circuits and pylon line of the electricity transport network (2013 to 2015)</strong></p><p>Regional annual changes of lengths (km) of circuits and pylon line in operation on the electricity transport network, by voltage level (63 kV, 90 kV, 225 kV, 400 kV, ...) and by type of change (new infrastructure, renewal, underground laying, scrapped, other changes). Final data.</p><p>Types of changes to a section include : \n<ul> \n<li>New : when installation of the section is a new route (a new link for example) </li><li>Renewed : when an overhead section is replaced with an overhead section or when an underground section is replaced with an underground section </li><li>Buried : when an overhead section is replaced with an underground section </li><li>Scrapped : when the section is physically retired from the network </li><li>Other changes : These are all the unrecorded length variations in new and scrapped sections. These are mainly sections placed in reserve (disconnected from the network but still in place), but also corrections of errors and length </li></ul><p><strong>Keywords :</strong> Electricity, Circuit, Network, Annual electricty report</p></p>"
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

    let portals = {
      'OPEN DATA RTE': { id: 1 }
    };

    download('https://data.opendatasoft.com/api/v2/catalog/datasets?rows=1&start=0', portals)
      .subscribe(
        (result) => validateMetadata(result),
        null,
        () => done()
      );
  });

  it('download() should skip the result if the portal is unrecognized.', (done) => {
    ToDosRewireAPI.__Rewire__('RxHR', {
      get: () => {
        return Rx.Observable.of({
          body: {
            datasets: [
              {
                "links": [
                  {
                    "href": "https://data.opendatasoft.com/api/v2/catalog/datasets/evolution_region_longueurs_circuits_files@rte",
                    "rel": "self"
                  },
                  {
                    "href": "https://data.opendatasoft.com/api/v2/catalog/datasets",
                    "rel": "datasets"
                  },
                  {
                    "href": "https://data.opendatasoft.com/api/v2/catalog/datasets/evolution_region_longueurs_circuits_files@rte/records",
                    "rel": "records"
                  },
                  {
                    "href": "https://data.opendatasoft.com/api/v2/catalog/datasets/evolution_region_longueurs_circuits_files@rte/exports",
                    "rel": "exports"
                  },
                  {
                    "href": "https://data.opendatasoft.com/api/v2/catalog/datasets/evolution_region_longueurs_circuits_files@rte/aggregates",
                    "rel": "aggregate"
                  }
                ],
                "dataset": {
                  "fields": [
                    {
                      "name": "circ_sout_63_kv_km",
                      "label": "Circ sout 63 kV (km)",
                      "type": "double",
                      "annotations": [],
                      "description": "Circuit souterrain 63 kV en km\n63 kV underground circuit in km"
                    }
                  ],
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
                      "description": "<p>\n\tEvolution  régionale annuelle des longueurs (km) de files de pylônes et de circuits en exploitation du réseau de transport d’électricité par niveau de tension (63 kV, 90 kV, 225 kV, 400 kV, ...) et par nature d’évolution (nouvelle infrastructure, renouvellement, mise en souterrain, ferraillée, autres modifications). Données définitives.</p><p>\n\tLes natures d'évolution d'un tronçon sont :</p><ul>\n\t\n<li>Nouveau : quand l’installation du tronçon fait l’objet d’un nouveau tracé (une nouvelle liaison par exemple)</li>\t\n<li>Renouvelé : quand un tronçon aérien est remplacé par un tronçon aérien ou quand un tronçon souterrain est remplacé par un tronçon souterrain</li>\t\n<li>Mis en souterrain : quand un tronçon aérien est remplacé par un tronçon souterrain\t </li>\t\n<li>Ferraillé : quand le tronçon a été retiré physiquement du réseau\t </li>\t\n<li>Autres modifications : Ce sont toutes les variations de longueur non recensées dans les tronçons neufs et ferraillés. Cela concerne principalement les tronçons mis en réserve (déconnectés du réseau mais toujours en place) mais aussi les corrections d’erreur ou de longueur</li></ul><hr/><p>\n<strong>Regional changes in lengths of circuits and pylon line of the electricity transport network (2013 to 2015)</strong></p><p>Regional annual changes of lengths (km) of circuits and pylon line in operation on the electricity transport network, by voltage level (63 kV, 90 kV, 225 kV, 400 kV, ...) and by type of change (new infrastructure, renewal, underground laying, scrapped, other changes). Final data.</p><p>Types of changes to a section include : \n<ul> \n<li>New : when installation of the section is a new route (a new link for example) </li><li>Renewed : when an overhead section is replaced with an overhead section or when an underground section is replaced with an underground section </li><li>Buried : when an overhead section is replaced with an underground section </li><li>Scrapped : when the section is physically retired from the network </li><li>Other changes : These are all the unrecorded length variations in new and scrapped sections. These are mainly sections placed in reserve (disconnected from the network but still in place), but also corrections of errors and length </li></ul><p><strong>Keywords :</strong> Electricity, Circuit, Network, Annual electricty report</p></p>"
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
            total_count: 450,
          }
        });
      }
    });

    let datasetCount = 0;

    downloadAll()
      .subscribe((data) => {
        expect(data).to.be.an('object');
        datasetCount++;
      },
      null,
      () => {
        expect(datasetCount).to.equal(2);
        done();
      });
  });

});
