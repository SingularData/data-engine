import chai from 'chai';
import Rx from 'rxjs';
import { save, __RewireAPI__ as ToDosRewireAPI } from '../src/database.js';

const expect = chai.expect;

export function validateMetadata(metadata) {
  expect(metadata).to.have.property('portalID');
  expect(metadata).to.have.property('name');
  expect(metadata).to.have.property('portalDatasetID');
  expect(metadata).to.have.property('createdTime');
  expect(metadata).to.have.property('updatedTime');
  expect(metadata).to.have.property('description');
  expect(metadata).to.have.property('portalLink');
  expect(metadata).to.have.property('dataLink');
  expect(metadata).to.have.property('license');
  expect(metadata).to.have.property('publisher');
  expect(metadata).to.have.property('tags');
  expect(metadata).to.have.property('categories');
  expect(metadata).to.have.property('raw');

  expect(metadata.portalID).to.be.a('number');
  expect(metadata.name).to.be.a('string');
  expect(metadata.updatedTime).to.be.a('date');
  expect(metadata.portalDatasetID).to.be.a('string');
  expect(metadata.portalLink).to.be.a('string');
  expect(metadata.tags).to.be.an('array');
  expect(metadata.categories).to.be.an('array');
  expect(metadata.raw).to.be.an('object');
}

describe('database.js', () => {

  it('save() should succefully construct the query to save metadata.', (done) => {
    ToDosRewireAPI.__Rewire__('getDB', () => {
      return {
        query: (sql) => Rx.Observable.of(sql)
      };
    });

    let metadatas = [{
      portalID: 1,
      portalDatasetID: 1,
      name: 'test',
      description: null,
      createdTime: null,
      updatedTime: new Date(Date.UTC(2017, 1, 1)),
      portalLink: 'http://localhost',
      dataLink: null,
      publisher: 'my portal',
      tags: ['of no use'],
      categories: ['ToDelete'],
      raw: {}
    }];

    let expectedSQL = `
    INSERT INTO view_latest_dataset (
      portal_id,
      portal_dataset_id,
      name,
      description,
      created_time,
      updated_time,
      portal_link,
      data_link,
      publisher,
      tags,
      categories,
      raw
    ) VALUES
  (
      1,
      1,
      'test',
      NULL,
      NULL,
      '2017-02-01T00:00:00.000Z',
      'http://localhost',
      NULL,
      'my portal',
      ARRAY['of no use']::text[],
      ARRAY['ToDelete']::text[],
      '{}'
    )`;

    save(metadatas)
      .subscribe(
        (query) => {
          expect(query).to.equal(expectedSQL);
        },
        null,
        () => done()
      );
  });
});
