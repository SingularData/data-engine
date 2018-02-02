import {
  getPageUrls,
  fetchPage
} from "../../../../src/fetcher/source-handler/junar";
import { expect } from "chai";

describe("fetcher/source-handler/junar.ts", function() {
  this.timeout(10000);

  it("should harvest Junar portal.", done => {
    const source = {
      apiUrl: "http://saccounty.cloudapi.junar.com",
      apiKey: "47242a5ca37d49fc19a2b8440942865f6e82486b"
    };

    getPageUrls(source)
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
