import _ from 'lodash';
import rp from 'request-promise';
import config from 'config';

export function crawlAll() {

  let rows = config.get('harvester.opendatasoft.rows');

  return rp({
    uri: 'https://data.opendatasoft.com/api/v2/catalog/datasets?rows=0&start=0',
    method: 'GET',
    json: true
  })
  .then(result => {
    let tasks = [];

    for (let i = 0; i <= result.total_count; i += rows) {
      let request = crawl(`https://data.opendatasoft.com/api/v2/catalog/datasets?rows=${rows}&start=${i}`);
      tasks.push(request);
    }

    return tasks;
  });
}

export function crawl(url) {
  return rp({
    uri: url,
    method: 'GET',
    json: true
  })
  .then(data => {
    let datasets = _.map(data.datasets, item => {
      let metas = item.dataset.metas.default;

      return {
        name: metas.title,
        portalDatasetID: item.dataset.dataset_id,
        updatedTime: metas.modified,
        description: metas.description,
        webpageLink: createLink(metas.source_domain_address, metas.source_dataset),
        dataLink: null,
        license: metas.license,
        publisher: metas.publisher,
        tags: metas.keyword,
        categories: metas.theme,
        raw: item
      };
    });

    return datasets;
  });
}

function createLink(domain, dataset) {
  return `https://${domain}/explore/dataset/${dataset}`;
}
