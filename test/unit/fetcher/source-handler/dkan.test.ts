import { fetchPage } from "../../../../src/fetcher/source-handler/dkan";
import { expect } from "chai";

describe("fetcher/source-handler/dkan.ts", function() {
  this.timeout(10000);

  it("should harvest DKAN portal.", done => {
    fetchPage({ url: "https://data.ca.gov" })
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
