import { harvest } from "../../src/harvesters/socrata";
import { expect } from "chai";

describe("harvesters/socrata.ts", function() {
  this.timeout(10000);

  it("harvest() should harvest Socrata network.", done => {
    harvest()
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
