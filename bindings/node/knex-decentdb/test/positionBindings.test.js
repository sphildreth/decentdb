'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const { positionBindings } = require('../src/positionBindings');

test('positionBindings rewrites ? to $N', () => {
  assert.equal(positionBindings('select ? as a, ? as b'), 'select $1 as a, $2 as b');
});
