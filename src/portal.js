import { getDB } from './database';
import { valueToString } from './utils/pg-util';

/**
 * Add new portals into the database. This function will skip the already existing
 * portal in the database.
 * @param {Portal[]}  portals             an array of portal information
 * @param {string}    portal.name         portal name
 * @param {string}    portal.platform     portal platform name
 * @param {string}    portal.url          portal url
 * @param {string}    portal.description  portal description
 * @param {string}    [portal.locationName]  portal location name
 * @param {GeoJSON}   [portal.location]       portal location (Point)
 * @returns {Observable} no return
 */
export function addPortals(portals) {
  let db = getDB();
  let inserPortal = db.tx((t) => {
    let sql = `
      INSERT INTO view_portal
        (name, url, description, platform, location_name, location)
      VALUES
    `;
    let values = portals.map((portal) => `(
      ${valueToString(portal.name)},
      ${valueToString(portal.url)},
      ${valueToString(portal.description)},
      ${valueToString(portal.platform)},
      ${valueToString(portal.locationName)},
      ${valueToString(portal.location)}
    )`);

    return t.query(sql + values.join(','));
  });

  return inserPortal;
}
