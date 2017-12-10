import { harvest } from "../../src/harvesters/ckan";
import { expect } from "chai";

describe("CKAN harvester", function() {
  this.timeout(10000);

  it("should harvest CKAN portal.", done => {
    harvest("http://catalog.data.gov")
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
