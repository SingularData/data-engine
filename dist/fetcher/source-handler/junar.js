"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const node_fetch_1 = require("node-fetch");
const util_1 = require("../util");
const requestSize = 100;
function getPageUrls(source) {
    return node_fetch_1.default(createUrl(source.apiUrl, source.apiKey, 0, 1))
        .then(res => res.json())
        .then(res => {
        const urls = [];
        const count = Math.ceil(res.count / requestSize);
        for (let i = 0; i < count; i++) {
            urls.push(createUrl(source.apiUrl, source.apiKey, i * requestSize, requestSize));
        }
        return urls;
    });
}
exports.getPageUrls = getPageUrls;
function fetchPage(source) {
    return node_fetch_1.default(source.url)
        .then(res => res.json())
        .then(res => res.results.map(d => util_1.wrapDataset("Junar", d)));
}
exports.fetchPage = fetchPage;
function createUrl(apiUrl, apiKey, offset, limit) {
    return `${apiUrl}/api/v2/datasets/?auth_key=${apiKey}&offset=${offset}&limit=${limit}`;
}
