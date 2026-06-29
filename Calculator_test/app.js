(function bootCalculatorApp() {
  const { OPERATIONS, calculate, formatResult } = window.CalculatorOperations;

  const firstInput = document.querySelector('#first-number');
  const secondInput = document.querySelector('#second-number');
  const operationButtons = document.querySelector('#operation-buttons');
  const form = document.querySelector('#calculator-form');
  const resultValue = document.querySelector('#result-value');
  const resultLabel = document.querySelector('#result-label');
  const clearButton = document.querySelector('#clear-button');

  let selectedOperation = 'add';

  function setOperation(operationKey) {
    selectedOperation = operationKey;

    for (const button of operationButtons.querySelectorAll('button')) {
      const isActive = button.dataset.operation === operationKey;
      button.classList.toggle('is-active', isActive);
      button.setAttribute('aria-pressed', String(isActive));
    }

    updateResultLabel();
  }

  function updateResultLabel() {
    const operation = OPERATIONS[selectedOperation];
    resultLabel.textContent = `${operation.label} result`;
  }

  function showResult() {
    try {
      const value = calculate(selectedOperation, firstInput.value, secondInput.value);
      resultValue.textContent = formatResult(value);
      resultValue.classList.remove('is-error');
    } catch (error) {
      resultValue.textContent = error.message;
      resultValue.classList.add('is-error');
    }
  }

  function clearCalculator() {
    firstInput.value = '';
    secondInput.value = '';
    resultValue.textContent = '0';
    resultValue.classList.remove('is-error');
    firstInput.focus();
  }

  function renderOperationButtons() {
    for (const [key, operation] of Object.entries(OPERATIONS)) {
      const button = document.createElement('button');
      button.type = 'button';
      button.className = 'operation-button';
      button.dataset.operation = key;
      button.setAttribute('aria-pressed', 'false');
      button.textContent = operation.symbol;
      button.title = operation.label;
      button.addEventListener('click', () => setOperation(key));
      operationButtons.append(button);
    }
  }

  renderOperationButtons();
  setOperation(selectedOperation);

  form.addEventListener('submit', (event) => {
    event.preventDefault();
    showResult();
  });

  clearButton.addEventListener('click', clearCalculator);
})();
