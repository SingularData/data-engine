import { expect } from "chai";
import fse = require("fs-extra");

describe("Data Source", () => {
  it("should have required properties.", () => {
    const sources = fse.readJsonSync(__dirname + "/../../data/sources.json");

    for (const source of sources) {
      expect(source.name).to.be.a("string");
      expect(source.url).to.be.a("string");
      expect(source.type).to.be.a("string");

      if (source.type === "Junar") {
        expect(source.apiUrl).to.be.a("string");
        expect(source.apiKey).to.be.a("string");
      }
    }
  });
});
