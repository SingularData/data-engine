"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const node_fetch_1 = require("node-fetch");
const util_1 = require("../util");
function getPageUrls(source) {
    return Promise.resolve([source.url]);
}
exports.getPageUrls = getPageUrls;
function fetchPage(source) {
    return node_fetch_1.default(`${source.url}/data.json`)
        .then(res => res.json())
        .then(res => {
        const data = Array.isArray(res) ? res : res.dataset;
        return data.map(d => util_1.wrapDataset("DKAN", d));
    });
}
exports.fetchPage = fetchPage;
