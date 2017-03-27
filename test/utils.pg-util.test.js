import chai from 'chai';
import * as pgUtil from '../src/utils/pg-util';

const expect = chai.expect;

describe('utils/pg-util.js', () => {

  it('valueToString() should convert null.', () => {
    expect(pgUtil.valueToString(null)).to.equal('NULL');
  });

  it('valueToString() should convert undefined.', () => {
    expect(pgUtil.valueToString(undefined)).to.equal('NULL');
  });

  it('valueToString() should convert date.', () => {
    expect(pgUtil.valueToString(new Date(Date.UTC(2017, 1, 1)))).to.equal("'2017-02-01T00:00:00.000Z'");
  });

  it('valueToString() should convert array.', () => {
    expect(pgUtil.valueToString(['1', '2'])).to.equal("ARRAY['1','2']");
  });

  it('valueToString() should convert object.', () => {
    expect(pgUtil.valueToString({ name: 'test' })).to.equal('\'{"name":"test"}\'');
  });

});
