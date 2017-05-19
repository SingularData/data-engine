import _ from 'lodash';
import config from 'config';
import log4js from 'log4js';
import { Observable } from 'rxjs';
import { RxHR } from "@akanass/rx-http-request";
import { getDB } from '../database';
import { toUTC } from '../utils/pg-util';

const userAgents = config.get('harvester.user_agents');
const logger = log4js.getLogger('ArcGIS Open Data');

/**
 * Get a list of harvesting Jobs.
 * @return {Observable}        harvest job
 */
export function downloadAll() {

  let sql = `
    SELECT portal.id, portal.url FROM portal
    LEFT JOIN platform ON platform.id = portal.platform_id
    WHERE platform.name = $1::text
  `;

  return getDB()
    .query(sql, ['ArcGIS Open Data'])
    .mergeMap((portal) => download(portal.id, portal.url));
}

/**
 * Harvest the given ArcGIS Open Data portal.
 * @param  {Number}             portalID    portal ID
 * @param  {String}             portalUrl   portal Url
 * @return {Observable}                  harvest job
 */
export function download(portalID, portalUrl) {
  return RxHR.get(`${portalUrl}/data.json`, {
    json: true,
    headers: {
      'User-Agent': _.sample(userAgents)
    }
  })
  .mergeMap((result) => {

    if (_.isString(result.body)) {
      return Observable.throw(new Error(`The target portal doesn't provide APIs: ${portalUrl}`));
    }

    let datasets = [];
    let data = result.body.dataset;

    for (let j = 0, m = data.length; j < m; j++) {
      let dataset = data[j];
      let dataFiles = _.map(dataset.distribution, (file) => {
        return {
          name: file.title || file.format,
          format: _.toLower(file.format),
          link: file.downloadURL || file.accessURL
        };
      });

      datasets.push({
        portalID: portalID,
        name: dataset.title,
        portalDatasetID: dataset.identifier,
        createdTime: dataset.issued ? toUTC(new Date(dataset.issued)) : null,
        updatedTime: toUTC(new Date(dataset.modified)),
        description: dataset.description,
        portalLink: dataset.landingPage,
        license: dataset.license,
        publisher: dataset.publisher.name,
        tags: dataset.keyword,
        categories: [],
        raw: dataset,
        region: bboxToGeoJSON(dataset.spatial),
        data: dataFiles
      });
    }

    return Observable.of(...datasets);
  })
  .catch((error) => {
    logger.error(`Unable to download data from ${portalUrl}. Message: ${error.message}.`);
    return Observable.empty();
  });
}

function bboxToGeoJSON(bboxString) {
  if (!bboxString) {
    return null;
  }

  let coords = bboxString.split(',').map((value) => +value);

  return {
    type: 'MultiPolygon',
    coordinates: [
      [[[coords[0], coords[1]], [coords[0], coords[3]], [coords[2], coords[3]], [coords[2], coords[1]], [coords[0], coords[1]]]]
    ]
  };
}
