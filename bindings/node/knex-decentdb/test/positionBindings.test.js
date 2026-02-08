'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const { positionBindings } = require('../src/positionBindings');

test('positionBindings rewrites ? to $N', () => {
  assert.equal(positionBindings('select ? as a, ? as b'), 'select $1 as a, $2 as b');
});

test('positionBindings ignores ? inside single-quoted strings', () => {
  assert.equal(positionBindings("select 'what?' as q, ? as v"), "select 'what?' as q, $1 as v");
});

test('positionBindings ignores ? inside double-quoted strings', () => {
  assert.equal(positionBindings('select "col?" as q, ? as v'), 'select "col?" as q, $1 as v');
});

test('positionBindings ignores ? in line comments', () => {
  assert.equal(positionBindings('select ? -- is this?\nfrom t'), 'select $1 -- is this?\nfrom t');
});

test('positionBindings handles escaped quotes', () => {
  assert.equal(positionBindings("select 'it''s a ?' as q, ? as v"), "select 'it''s a ?' as q, $1 as v");
});

test('positionBindings returns non-string unchanged', () => {
  assert.equal(positionBindings(42), 42);
  assert.equal(positionBindings(null), null);
});

test('positionBindings handles no placeholders', () => {
  assert.equal(positionBindings('select 1'), 'select 1');
});
