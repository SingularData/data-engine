import chai from 'chai';
import Rx from 'rxjs';
import { save, __RewireAPI__ as ToDosRewireAPI } from '../src/database.js';

const expect = chai.expect;

export function validateMetadata(metadata) {
  expect(metadata).to.have.property('portalId');
  expect(metadata).to.have.property('title');
  expect(metadata).to.have.property('issued');
  expect(metadata).to.have.property('modified');
  expect(metadata).to.have.property('description');
  expect(metadata).to.have.property('landingPage');
  expect(metadata).to.have.property('license');
  expect(metadata).to.have.property('publisher');
  expect(metadata).to.have.property('keyword');
  expect(metadata).to.have.property('theme');
  expect(metadata).to.have.property('raw');
  expect(metadata).to.have.property('distribution');

  expect(metadata.portalId).to.be.a('number');
  expect(metadata.title).to.be.a('string');
  expect(metadata.modified).to.be.a('date');
  expect(metadata.landingPage).to.be.a('string');
  expect(metadata.keyword).to.be.an('array');
  expect(metadata.theme).to.be.an('array');
  expect(metadata.raw).to.be.an('object');
  expect(metadata.distribution).to.be.an('array');
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
      identifier: 'test-identifier',
      title: 'test',
      description: null,
      issued: null,
      modified: new Date(Date.UTC(2017, 1, 1)),
      landingPage: 'http://localhost',
      publisher: 'my portal',
      keyword: ['of no use'],
      theme: ['ToDelete'],
      raw: {},
      distribution: [],
      version: 1,
      versionPeriod: '[,)'
    }];

    let expectedSQL = `
    INSERT INTO view_latest_dataset (
      portal_id,
      identifier,
      title,
      description,
      issued,
      modified,
      landing_page,
      publisher,
      keyword,
      theme,
      raw,
      version,
      version_period,
      spatial,
      distribution
    ) VALUES
  (
      1,
      'test-identifier',
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
      ARRAY[]::json[]
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
