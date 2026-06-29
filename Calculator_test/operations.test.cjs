const test = require('node:test');
const assert = require('node:assert/strict');

const { OPERATIONS, calculate, formatResult } = require('./operations.js');

test('adds negative and decimal numbers', () => {
  assert.equal(calculate('add', -2.5, 4), 1.5);
});

test('multiplies whole numbers', () => {
  assert.equal(calculate('multiply', 6, 7), 42);
});

test('keeps operation definitions merge-friendly', () => {
  assert.deepEqual(Object.keys(OPERATIONS), ['add', 'multiply']);
  assert.equal(OPERATIONS.add.symbol, '+');
  assert.equal(OPERATIONS.multiply.symbol, 'x');
  assert.equal(typeof OPERATIONS.add.apply, 'function');
  assert.equal(typeof OPERATIONS.multiply.apply, 'function');
});

test('rejects unsupported operations clearly', () => {
  assert.throws(() => calculate('subtract', 3, 1), /Unsupported operation/);
});

test('rejects non-numeric input clearly', () => {
  assert.throws(() => calculate('add', 'hello', 1), /Numbers only/);
});

test('formats floating point noise for display', () => {
  assert.equal(formatResult(0.1 + 0.2), '0.3');
});
