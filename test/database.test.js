import chai from 'chai';
import Rx from 'rxjs';
import { save, __RewireAPI__ as ToDosRewireAPI } from '../src/database.js';

const expect = chai.expect;

export function validateMetadata(metadata) {
  expect(metadata).to.have.property('portalId');
  expect(metadata).to.have.property('name');
  expect(metadata).to.have.property('portalDatasetId');
  expect(metadata).to.have.property('created');
  expect(metadata).to.have.property('updated');
  expect(metadata).to.have.property('description');
  expect(metadata).to.have.property('url');
  expect(metadata).to.have.property('license');
  expect(metadata).to.have.property('publisher');
  expect(metadata).to.have.property('tags');
  expect(metadata).to.have.property('categories');
  expect(metadata).to.have.property('raw');
  expect(metadata).to.have.property('files');

  expect(metadata.portalId).to.be.a('number');
  expect(metadata.name).to.be.a('string');
  expect(metadata.updated).to.be.a('date');
  expect(metadata.portalDatasetId).to.be.a('string');
  expect(metadata.url).to.be.a('string');
  expect(metadata.tags).to.be.an('array');
  expect(metadata.categories).to.be.an('array');
  expect(metadata.raw).to.be.an('object');
  expect(metadata.categories).to.be.an('array');
  expect(metadata.files).to.be.an('array');
}

describe('database.js', () => {

  it('save() should succefully construct the query to save metadata.', (done) => {
    ToDosRewireAPI.__Rewire__('getDB', () => {
      return {
        query: (sql) => Rx.Observable.of(sql)
      };
    });

    let metadatas = [{
      portalId: 1,
      portalDatasetId: 1,
      uuid: 'test-uuid',
      name: 'test',
      description: null,
      created: null,
      updated: new Date(Date.UTC(2017, 1, 1)),
      url: 'http://localhost',
      publisher: 'my portal',
      tags: ['of no use'],
      categories: ['ToDelete'],
      raw: {},
      data: [],
      version: 1,
      versionPeriod: '[,)'
    }];

    let expectedSQL = `
    INSERT INTO view_latest_dataset (
      portal_id,
      portal_dataset_id,
      uuid,
      name,
      description,
      created,
      updated,
      url,
      publisher,
      tags,
      categories,
      raw,
      version,
      version_period,
      region,
      files
    ) VALUES
  (
      1,
      1,
      'test-uuid',
      'test',
      NULL,
      NULL,
      '2017-02-01T00:00:00.000Z',
      'http://localhost',
      'my portal',
      ARRAY['of no use']::text[],
      ARRAY['ToDelete']::text[],
      '{}',
      1,
      '[,)',
      NULL,
      NULL::json[]
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
