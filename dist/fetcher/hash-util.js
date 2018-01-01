"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const crypto_1 = require("crypto");
function sha256(data) {
    const hash = crypto_1.createHash("sha256");
    hash.update(data);
    return hash.digest("base64");
}
exports.sha256 = sha256;
