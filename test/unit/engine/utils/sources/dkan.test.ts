import { getDatasets } from "../../../../../src/engine/utils/sources/dkan";
import { expect } from "chai";

describe("engine/utils/sources/dkan", function() {
  this.timeout(10000);

  it("should harvest DKAN portal.", async () => {
    const datasets = await getDatasets({ url: "https://data.ca.gov" });

    expect(datasets).to.have.length.above(0);
    expect(datasets[0].dcat).to.be.an("object");
    expect(datasets[0].checksum).to.be.a("string");
    expect(datasets[0].original).to.be.an("object");
  });
});
