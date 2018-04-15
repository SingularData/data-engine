import { getDatasets } from "../../../../../src/engine/utils/sources/geonode";
import { expect } from "chai";

describe("engine/utils/sources/geocode", function() {
  this.timeout(10000);

  it("should harvest GeoNode portal.", async () => {
    const datasets = await getDatasets({ url: "https://geonode.wfp.org" });

    expect(datasets).to.have.length.above(0);
    expect(datasets[0].dcat).to.be.an("object");
    expect(datasets[0].checksum).to.be.a("string");
    expect(datasets[0].original).to.be.an("object");
  });
});
