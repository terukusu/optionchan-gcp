// static/theme.js（配色。既定はダーク）のテスト。DOM に触る部分は試さず、覚えている値の読み方（normalize）だけ:
//   cd apps/viewer && node --test tests/js/*.test.js
'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');

const ViewerTheme = require(path.join(__dirname, '..', '..', 'src', 'optionchan_viewer', 'static', 'theme.js'));

test('既定はダーク', () => {
  assert.equal(ViewerTheme.DEFAULT, 'dark');
});

test('覚えている値: light・dark はそのまま、それ以外（無い・壊れた値）は既定', () => {
  assert.equal(ViewerTheme.normalize('light'), 'light');
  assert.equal(ViewerTheme.normalize('dark'), 'dark');
  for (const v of [null, undefined, '', 'Light', 'auto', 'system']) {
    assert.equal(ViewerTheme.normalize(v), 'dark', String(v));
  }
});
