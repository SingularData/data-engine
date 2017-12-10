import { harvest } from "../../src/harvesters/opendatasoft";
import { expect } from "chai";

describe("OpenDataSoft harvester", function() {
  this.timeout(10000);

  it("should harvest OpenDataSoft network.", done => {
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
