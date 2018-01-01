"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const node_fetch_1 = require("node-fetch");
const util_1 = require("../util");
const requestSize = 500;
function getPageUrls(source) {
    return node_fetch_1.default(createUrl(source.url, 0, 0))
        .then(res => res.json())
        .then(res => {
        const urls = [];
        const count = Math.ceil(res.result.count / requestSize);
        for (let i = 0; i < count; i++) {
            urls.push(createUrl(source.url, i * requestSize, requestSize));
        }
        return urls;
    });
}
exports.getPageUrls = getPageUrls;
function fetchPage(source) {
    return node_fetch_1.default(source.url)
        .then(res => res.json())
        .then(res => res.result.results.map(d => util_1.wrapDataset("CKAN", d)));
}
exports.fetchPage = fetchPage;
function createUrl(portalUrl, start, rows) {
    return `${portalUrl}/api/3/action/package_search?start=${start}&rows=${rows}`;
}
