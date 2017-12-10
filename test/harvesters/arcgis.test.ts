import { harvest } from "../../src/harvesters/arcgis";
import { expect } from "chai";

describe("ArcGIS harvester", function() {
  this.timeout(10000);

  it("should harvest ArcGIS Open Data network.", done => {
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
