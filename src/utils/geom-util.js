import { Geometry } from 'wkx';

export function wktToGeoJSON(polygon) {
  if (!polygon || !polygon.startsWith('POLYGON')) {
    return null;
  }

  let multi = Geometry.parse(polygon).toGeoJSON();
  multi.type = 'MultiPolygon';
  multi.coordinates = [multi.coordinates];

  return multi;
}
