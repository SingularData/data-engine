import { harvest } from "../../../src/harvesters/dkan";
import { expect } from "chai";

describe("harvesters/dkan.ts", function() {
  this.timeout(10000);

  it("harvest() should harvest DKAN portal.", done => {
    harvest({ url: "https://data.ca.gov" })
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
