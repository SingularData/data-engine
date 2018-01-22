import {
  getPageUrls,
  fetchPage
} from "../../../../src/fetcher/source-handler/arcgis";
import { expect } from "chai";

describe("fetcher/source-handler/arcgis.ts", function() {
  this.timeout(10000);

  it("should harvest ArcGIS Open Data network.", done => {
    getPageUrls()
      .then(urls => fetchPage({ url: urls[0] }))
      .then((datasets: any) => {
        expect(datasets).to.have.length.above(0);
        expect(datasets[0].dcat).to.be.an("object");
        expect(datasets[0].checksum).to.be.a("string");
        expect(datasets[0].original).to.be.an("object");
        done();
      })
      .catch(err => done(err));
  });
});
