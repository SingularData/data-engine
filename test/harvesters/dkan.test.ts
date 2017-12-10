import { harvest } from "../../src/harvesters/dkan";
import { expect } from "chai";

describe("DKAN harvester", function() {
  this.timeout(10000);

  it("should harvest DKAN portal.", done => {
    harvest("http://demo.getdkan.com")
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
