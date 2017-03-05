import _ from 'lodash';
import rp from 'request-promise';
import config from 'config';

/**
 * Get a list harvesting Jobs.
 * @return {Promise<[]>}    An array of harvesting jobs.
 */
export function harvestAll() {

  let rows = config.get('harvester.opendatasoft.rows');

  return rp({
    uri: 'https://data.opendatasoft.com/api/v2/catalog/datasets?rows=0&start=0',
    method: 'GET',
    json: true
  })
  .then(result => {
    let tasks = [];

    for (let i = 0; i <= result.total_count; i += rows) {
      let request = harvest(`https://data.opendatasoft.com/api/v2/catalog/datasets?rows=${rows}&start=${i}`);
      tasks.push(request);
    }

    return tasks;
  });
}

/**
 * Harvest the open data network of OpenDataSoft with a given url.
 * @param  {String}             url   OpenDataSoft data network API url
 * @return {Promise<Object[]>}        an array of dataset metadata
 */
export function harvest(url) {
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
        createdTime: null,
        updatedTime: new Date(metas.modified),
        description: metas.description,
        portalLink: createLink(metas.source_domain_address, metas.source_dataset),
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

/**
 * Get the site of the dataset
 * @param  {String} domain  portal domain
 * @param  {String} dataset dataset id
 * @return {String}         site url
 */
function createLink(domain, dataset) {
  return `https://${domain}/explore/dataset/${dataset}`;
}
