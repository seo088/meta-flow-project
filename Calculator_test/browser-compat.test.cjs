const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const assert = require('node:assert/strict');

const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');

test('uses classic scripts so index.html works from file protocol', () => {
  assert.match(html, /<script src="\.\/operations\.js"><\/script>/);
  assert.match(html, /<script src="\.\/app\.js"><\/script>/);
  assert.doesNotMatch(html, /type="module"/);
  assert.ok(html.indexOf('./operations.js') < html.indexOf('./app.js'));
});
