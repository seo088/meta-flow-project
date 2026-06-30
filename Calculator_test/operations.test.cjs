const test = require('node:test');
const assert = require('node:assert/strict');

const { OPERATIONS, calculate, formatResult } = require('./operations.js');

test('adds negative and decimal numbers', () => {
  assert.equal(calculate('add', -2.5, 4), 1.5);
});

test('subtracts decimal numbers', () => {
  assert.equal(calculate('subtract', 10.5, 3.25), 7.25);
});

test('multiplies whole numbers', () => {
  assert.equal(calculate('multiply', 6, 7), 42);
});

test('divides whole numbers', () => {
  assert.equal(calculate('divide', 12, 3), 4);
});

test('rejects division by zero clearly', () => {
  assert.throws(() => calculate('divide', 12, 0), /Cannot divide by zero/);
});

test('keeps operation definitions merge-friendly', () => {
  assert.deepEqual(Object.keys(OPERATIONS), ['add', 'subtract', 'multiply', 'divide']);
  assert.equal(OPERATIONS.add.symbol, '+');
  assert.equal(OPERATIONS.subtract.symbol, '-');
  assert.equal(OPERATIONS.multiply.symbol, 'x');
  assert.equal(OPERATIONS.divide.symbol, '/');
  assert.equal(typeof OPERATIONS.add.apply, 'function');
  assert.equal(typeof OPERATIONS.subtract.apply, 'function');
  assert.equal(typeof OPERATIONS.multiply.apply, 'function');
  assert.equal(typeof OPERATIONS.divide.apply, 'function');
});

test('rejects unsupported operations clearly', () => {
  assert.throws(() => calculate('modulo', 3, 1), /Unsupported operation/);
});

test('rejects non-numeric input clearly', () => {
  assert.throws(() => calculate('add', 'hello', 1), /Numbers only/);
});

test('formats floating point noise for display', () => {
  assert.equal(formatResult(0.1 + 0.2), '0.3');
});
