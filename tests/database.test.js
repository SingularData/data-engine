import { save, __RewireAPI__ as ToDosRewireAPI } from '../src/database.js';
import chai from 'chai';

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

  it('should succefully construct the query', () => {
    ToDosRewireAPI.__Rewire__('getDB', () => {
      return {
        none: (sql) => {
          expect(sql).to.be.a('string');
          return Promise.resolve();
        }
      };
    });

    let metadatas = [{
      portal_id: 1,
      portal_dataset_id: 1,
      name: 'test',
      description: null,
      created_time: null,
      updated_time: new Date(2017, 1, 1),
      portal_link: 'http://localhost',
      data_link: null,
      publisher: 'my portal',
      tags: ['of no use'],
      categories: ['ToDelete'],
      raw: {}
    }];

    save(metadatas);
  });

});
