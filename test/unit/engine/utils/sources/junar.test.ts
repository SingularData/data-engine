import {
  getSourceUrls,
  getDatasets
} from "../../../../../src/engine/utils/sources/junar";
import { expect } from "chai";

describe("engine/utils/sources/junar", function() {
  this.timeout(10000);

  it("should harvest Junar portal.", async () => {
    const source = {
      apiUrl: "http://saccounty.cloudapi.junar.com",
      apiKey: "47242a5ca37d49fc19a2b8440942865f6e82486b"
    };

    const urls = await getSourceUrls(source);
    const datasets = await getDatasets({ url: urls[0] });

    expect(datasets).to.have.length.above(0);
    expect(datasets[0].dcat).to.be.an("object");
    expect(datasets[0].checksum).to.be.a("string");
    expect(datasets[0].original).to.be.an("object");
  });
});
