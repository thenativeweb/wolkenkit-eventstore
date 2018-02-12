'use strict';

var _ = require('lodash');

var omitByDeep = function omitByDeep(obj, predicate) {
  if (!predicate) {
    throw new Error('Predicate is missing.');
  }

  if (!_.isObject(obj) || _.isArray(obj)) {
    return obj;
  }

  return _(obj).omitBy(predicate).mapValues(function (value) {
    return omitByDeep(value, predicate);
  }).value();
};

module.exports = omitByDeep;