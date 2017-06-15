'use strict';

const assert = require('assertthat');

const omitByDeep = require('../../lib/omitByDeep');

suite('omitByDeep', () => {
  test('is a function.', done => {
    assert.that(omitByDeep).is.ofType('function');
    done();
  });

  test('throws an error if predicate is missing.', done => {
    assert.that(() => {
      omitByDeep(23);
    }).is.throwing('Predicate is missing.');
    done();
  });

  test('returns the value if it is not an object.', done => {
    assert.that(omitByDeep(23, value => value)).is.equalTo(23);
    done();
  });

  test('returns the value even for falsy values.', done => {
    assert.that(omitByDeep(0, value => value)).is.equalTo(0);
    done();
  });

  test('returns the value even for undefined.', done => {
    assert.that(omitByDeep(undefined, value => value)).is.undefined();
    done();
  });

  test('returns the value if it is an object.', done => {
    assert.that(omitByDeep({ foo: 'bar' }, value => value === undefined)).is.equalTo({ foo: 'bar' });
    done();
  });

  test('omits properties that fulfill the predicate.', done => {
    assert.that(omitByDeep({ foo: 'bar', bar: 'baz' }, value => value === 'bar')).is.equalTo({ bar: 'baz' });
    done();
  });

  test('omits undefined, but not null, if predicate checks for undefined.', done => {
    assert.that(omitByDeep({ foo: null, bar: undefined }, value => value === undefined)).is.equalTo({ foo: null });
    done();
  });

  test('correctly handles empty arrays.', done => {
    assert.that(omitByDeep({ bar: 'baz', foo: []}, value => value === undefined)).is.equalTo({ bar: 'baz', foo: []});
    done();
  });
});
