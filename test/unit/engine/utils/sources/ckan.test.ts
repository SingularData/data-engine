import {
  getSourceUrls,
  getDatasets
} from "../../../../../src/engine/utils/sources/ckan";
import { expect } from "chai";

describe("engine/utils/sources/ckan", function() {
  this.timeout(20000);

  it("should harvest CKAN portal.", async () => {
    const urls = await getSourceUrls({ url: "http://catalog.data.gov" });
    const datasets = await getDatasets({ url: urls[0] });

    expect(datasets).to.have.length.above(0);
    expect(datasets[0].dcat).to.be.an("object");
    expect(datasets[0].checksum).to.be.a("string");
    expect(datasets[0].original).to.be.an("object");
  });
});
