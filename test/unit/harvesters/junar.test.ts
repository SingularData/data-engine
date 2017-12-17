import { harvest } from "../../../src/harvesters/junar";
import { expect } from "chai";

describe("harvesters/junar.ts", function() {
  this.timeout(10000);

  it("harvest() should harvest Junar portal.", done => {
    harvest({
      apiUrl: "http://saccounty.cloudapi.junar.com",
      apiKey: "47242a5ca37d49fc19a2b8440942865f6e82486b"
    })
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
