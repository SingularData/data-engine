"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const node_fetch_1 = require("node-fetch");
const util_1 = require("../util");
const requestSize = 100;
function getPageUrls() {
    return node_fetch_1.default(createUrl(1, 0))
        .then(res => res.json())
        .then(res => {
        const urls = [];
        const count = Math.ceil(res.meta.stats.totalCount / requestSize);
        for (let i = 1; i <= count; i++) {
            urls.push(createUrl(i, requestSize));
        }
        return urls;
    });
}
exports.getPageUrls = getPageUrls;
function fetchPage(source) {
    return node_fetch_1.default(source.url)
        .then(res => res.json())
        .then(res => res.data.map(d => util_1.wrapDataset("ArcGIS", d)));
}
exports.fetchPage = fetchPage;
function createUrl(pageNumber, pageSize) {
    return `https://opendata.arcgis.com/api/v2/datasets?page[number]=${pageNumber}&page[size]=${pageSize}`;
}
