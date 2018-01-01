"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const node_fetch_1 = require("node-fetch");
const util_1 = require("../util");
function getPageUrls(source) {
    return Promise.resolve([source.url]);
}
exports.getPageUrls = getPageUrls;
function fetchPage(source) {
    return node_fetch_1.default(`${source.url}/api/base`)
        .then(res => res.json())
        .then(res => res.objects.map(d => util_1.wrapDataset("GeoNode", d)));
}
exports.fetchPage = fetchPage;
