"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const node_fetch_1 = require("node-fetch");
const util_1 = require("../util");
const requestSize = 100;
function getPageUrls() {
    return node_fetch_1.default(createUrl(0, 0))
        .then(res => res.json())
        .then(res => {
        const urls = [];
        const count = Math.ceil(res.total_count / requestSize);
        for (let i = 0; i < count; i++) {
            urls.push(createUrl(i * requestSize, requestSize));
        }
        return urls;
    });
}
exports.getPageUrls = getPageUrls;
function fetchPage(source) {
    return node_fetch_1.default(source.url)
        .then(res => res.json())
        .then(res => res.datasets.map(d => util_1.wrapDataset("OpenDataSoft", d)));
}
exports.fetchPage = fetchPage;
function createUrl(start, rows) {
    return `https://data.opendatasoft.com/api/v2/catalog/datasets?rows=${rows}&start=${start}`;
}
