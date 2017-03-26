// source: https://github.com/brianc/node-postgres/blob/master/lib/utils.js

import _ from 'lodash';

function escape(value) {
  let escaped = value
    .replace(/"/g, '\"')
    .replace(/'/g, "''");

  return escaped;
}

export function arrayToString(array) {

  let values = [];

  for (let value of array) {
    if(_.isNil(value)) {
      values.push('NULL');
    } else if(_.isArray(value)) {
      values.push(arrayToString(value));
    } else {
      values.push(valueToString(value));
    }
  }

  return 'ARRAY[' + values.join(',') + ']';
}

export function dateToString(date) {
  return "'" + date.toISOString() + "'";
}

export function objectToString(value) {
  return "'" + escape(JSON.stringify(value)) + "'";
}

export function valueToString(value) {
  if (_.isBuffer(value)) {
    return value;
  } else if (_.isDate(value)) {
    return dateToString(value);
  } else if (_.isArray(value)) {
    return arrayToString(value);
  } else if (_.isNil(value)) {
    return 'NULL';
  } else if (_.isString(value)) {
    return "'" + escape(value) + "'";
  } else if (_.isObjectLike(value)) {
    return objectToString(value);
  }

  return _.toString(value);
}
