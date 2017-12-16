import { harvest } from "../../src/harvesters/geonode";
import { expect } from "chai";

describe("harvesters/geocode.ts", function() {
  this.timeout(10000);

  it("harvest() should harvest GeoNode portal.", done => {
    harvest({ url: "http://geonode.state.gov" })
      .first()
      .subscribe(
        (data: any) => {
          expect(data.dcat).to.be.an("object");
          expect(data.checksum).to.be.a("string");
          expect(data.original).to.be.an("object");
        },
        err => done(err),
        () => done()
      );
  });
});
