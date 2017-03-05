import chai from 'chai';

chai.use(require('chai-as-promised'));

const expect = chai.expect;

export function validateMetadata(metadata) {
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

  expect(metadata.name).to.be.a('string');
  expect(metadata.updatedTime).to.be.a('date');
  expect(metadata.portalDatasetID).to.be.a('string');
  expect(metadata.portalLink).to.be.a('string');
  expect(metadata.tags).to.be.an('array');
  expect(metadata.categories).to.be.an('array');
  expect(metadata.raw).to.be.an('object');
}
