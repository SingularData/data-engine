"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const node_fetch_1 = require("node-fetch");
const util_1 = require("../util");
const requestSize = 100;
const regions = ["us", "eu"];
function getPageUrls() {
    const urls = [];
    const tasks = [];
    for (let region of regions) {
        const task = node_fetch_1.default(createUrl(region, 0, 0))
            .then(res => res.json())
            .then(res => {
            const count = Math.ceil(res.resultSetSize / requestSize);
            for (let i = 0; i < count; i++) {
                urls.push(createUrl(region, i * requestSize, requestSize));
            }
        });
        tasks.push(task);
    }
    return Promise.all(tasks).then(() => urls);
}
exports.getPageUrls = getPageUrls;
function fetchPage(source) {
    return node_fetch_1.default(source.url)
        .then(res => res.json())
        .then(res => res.results.map(d => util_1.wrapDataset("Socrata", d)));
}
exports.fetchPage = fetchPage;
function createUrl(region, offset, limit) {
    return `http://api.${region}.socrata.com/api/catalog/v1?offset=${offset}&limit=${limit}`;
}
