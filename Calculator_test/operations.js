(function attachCalculatorOperations(root) {
  const OPERATIONS = {
    add: {
      label: 'Add',
      symbol: '+',
      apply: (left, right) => left + right,
    },
    multiply: {
      label: 'Multiply',
      symbol: 'x',
      apply: (left, right) => left * right,
    },
  };

  function calculate(operationKey, leftValue, rightValue) {
    const operation = OPERATIONS[operationKey];

    if (!operation) {
      throw new Error(`Unsupported operation: ${operationKey}`);
    }

    const left = Number(leftValue);
    const right = Number(rightValue);

    if (!Number.isFinite(left) || !Number.isFinite(right)) {
      throw new Error('Numbers only');
    }

    return operation.apply(left, right);
  }

  function formatResult(value) {
    if (!Number.isFinite(value)) {
      return 'Not a number';
    }

    return Number.parseFloat(value.toFixed(10)).toString();
  }

  const api = {
    OPERATIONS,
    calculate,
    formatResult,
  };

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = api;
    return;
  }

  root.CalculatorOperations = api;
})(globalThis);
