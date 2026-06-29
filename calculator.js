const firstNumberInput = document.querySelector("#firstNumber");
const secondNumberInput = document.querySelector("#secondNumber");
const subtractButton = document.querySelector("#subtractButton");
const clearButton = document.querySelector("#clearButton");
const resultOutput = document.querySelector("#result");

function subtract(firstNumber, secondNumber) {
  return firstNumber - secondNumber;
}

function renderResult() {
  const firstNumber = Number(firstNumberInput.value);
  const secondNumber = Number(secondNumberInput.value);
  const result = subtract(firstNumber, secondNumber);

  resultOutput.textContent = `Result: ${result}`;
}

function clearCalculator() {
  firstNumberInput.value = "";
  secondNumberInput.value = "";
  resultOutput.textContent = "Result:";
}

subtractButton.addEventListener("click", renderResult);
clearButton.addEventListener("click", clearCalculator);
