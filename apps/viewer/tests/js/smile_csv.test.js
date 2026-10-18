// static/smile_csv.js（/smile_snapshot の CSV の読み取り。#79・#123）のテスト。値は合成:
//   cd apps/viewer && node --test tests/js/*.test.js
'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');

const SmileCsv = require(path.join(__dirname, '..', '..', 'src', 'optionchan_viewer', 'static', 'smile_csv.js'));
const { COLUMNS, parseSnapshot } = SmileCsv;

const ROWS = [
  '1,2,24875,,,,19.0,20.0,140,150,19.5,1',
  '1,1,25000,18.25,300,978580000,18.0,,295,305,,1',
  '2,1,25500,17.0,600,,,,,,,0',
  '3,1,25000,1,1,1,1,1,1,1,1,1',  // 3 限月は読まない
  '1,2,24750',  // 列の数が違う行は飛ばす
].join('\n');

test('parseSnapshot: 6 列の 1 行目を受け付け、source を返す', () => {
  const snap = parseSnapshot('978580800,2001-01-11,25000,2001-02-08,,fill114\n' + ROWS + '\n');
  assert.equal(snap.s, 978580800);
  assert.equal(snap.source, 'fill114');
  assert.deepEqual(snap.months, { 1: { ltd: '2001-01-11', atm: 25000 }, 2: { ltd: '2001-02-08', atm: null } });
  assert.equal(parseSnapshot('978580800,2001-01-11,25000,2001-02-08,25500,\n' + ROWS).source, null);
});

test('parseSnapshot: 2 行目以降の値は source で変わらない', () => {
  const a = parseSnapshot('978580800,2001-01-11,25000,2001-02-08,,fill114\n' + ROWS);
  const b = parseSnapshot('978580800,2001-01-11,25000,2001-02-08,,\n' + ROWS);
  assert.deepEqual(a.byMonth, b.byMonth);
  assert.equal(a.byMonth[1].length, 2);
  assert.equal(a.byMonth[2].length, 1);
  assert.deepEqual(a.byMonth[1][1], { month: 1, type: 1, target_price: 25000, iv: 18.25, price: 300,
    price_time: 978580000, bid_iv: 18.0, ask_iv: null, bid: 295, ask: 305, mid_iv: null, otm: 1 });
  assert.equal(a.hasQuote, true);
  assert.equal(a.lo, 24875);
  assert.equal(a.hi, 25500);
  assert.equal(COLUMNS.length, 12);
  // 気配の無いスナップショット
  assert.equal(parseSnapshot('978580800,2001-01-11,,2001-01-11,,fill114\n2,1,25500,17.0,600,,,,,,,0').hasQuote, false);
});

test('parseSnapshot: 5 列・7 列・数字でない 1 列目は invalid response、空の本文は null', () => {
  for (const head of ['978580800,2001-01-11,25000,2001-02-08,', '978580800,2001-01-11,25000,2001-02-08,,,',
    'abc,2001-01-11,25000,2001-02-08,,', '<!doctype html>']) {
    assert.throws(() => parseSnapshot(head + '\n' + ROWS), /invalid response/, head);
  }
  assert.equal(parseSnapshot(''), null);
  assert.equal(parseSnapshot('\n'), null);
});
