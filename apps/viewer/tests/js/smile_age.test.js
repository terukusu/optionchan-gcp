// static/smile_age.js（#82: 約定 IV の鮮度の規則）のテスト。node の標準のテストだけを使う（依存なし）:
//   cd apps/viewer && node --test tests/js/*.test.js
'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');

const SmileAge = require(path.join(__dirname, '..', '..', 'src', 'optionchan_viewer', 'static', 'smile_age.js'));
const { ageSeconds, tierOf, agoText, TIERS } = SmileAge;

// JST の日時 → UTC の UNIX 秒
function jst(y, mo, d, h, mi) {
  return Date.UTC(y, mo - 1, d, h, mi) / 1000 - 9 * 3600;
}

const SNAP = jst(2026, 10, 15, 13, 59);

test('経過はスナップショット時刻 − price_time（秒）', () => {
  assert.equal(ageSeconds(SNAP, jst(2026, 10, 15, 13, 52)), 7 * 60);
  assert.equal(ageSeconds(SNAP, SNAP), 0);
});

test('price_time が NULL なら経過は null', () => {
  assert.equal(ageSeconds(SNAP, null), null);
  assert.equal(ageSeconds(SNAP, undefined), null);
});

test('price_time がスナップショットより後（未来）なら経過は null', () => {
  assert.equal(ageSeconds(SNAP, SNAP + 1), null);
  assert.equal(ageSeconds(SNAP, SNAP + 86400), null);  // 日付ずれ（#4）の名残
});

test('段の区切りは 5 分 / 30 分 / 2 時間（ちょうどの値はその段に入る）', () => {
  assert.deepEqual(TIERS.map((t) => t.max), [300, 1800, 7200, Infinity]);
  assert.equal(tierOf(0), 0);
  assert.equal(tierOf(300), 0);
  assert.equal(tierOf(301), 1);
  assert.equal(tierOf(1800), 1);
  assert.equal(tierOf(1801), 2);
  assert.equal(tierOf(7200), 2);
  assert.equal(tierOf(7201), 3);
  assert.equal(tierOf(3 * 86400), 3);
});

test('経過が null（NULL・未来）は最後の段', () => {
  assert.equal(tierOf(null), TIERS.length - 1);
  assert.equal(tierOf(ageSeconds(SNAP, null)), 3);
  assert.equal(tierOf(ageSeconds(SNAP, SNAP + 60)), 3);
});

test('「何分前」: 1 時間未満は分（切り捨て）', () => {
  assert.equal(agoText(0), '0分前');
  assert.equal(agoText(59), '0分前');
  assert.equal(agoText(60), '1分前');
  assert.equal(agoText(59 * 60 + 59), '59分前');
});

test('「何分前」: 1 時間以上は「H時間M分前」', () => {
  assert.equal(agoText(3600), '1時間0分前');
  assert.equal(agoText(2 * 3600 + 5 * 60), '2時間5分前');
  assert.equal(agoText(86400 - 1), '23時間59分前');
});

test('「何分前」: 24 時間以上は日数も付ける', () => {
  assert.equal(agoText(86400), '1日0時間0分前');
  assert.equal(agoText(3 * 86400 + 4 * 3600 + 5 * 60 + 30), '3日4時間5分前');
});

test('暦日をまたいでも 24 時間未満なら日数は付けない（夜間の約定を翌朝に見る）', () => {
  const morning = jst(2026, 10, 16, 8, 46);
  const night = jst(2026, 10, 15, 23, 30);
  assert.equal(agoText(ageSeconds(morning, night)), '9時間16分前');
  assert.equal(tierOf(ageSeconds(morning, night)), 3);
});

test('週末をまたぐ（金曜の夜間 → 月曜の朝）', () => {
  const monday = jst(2026, 10, 19, 8, 46);
  const friday = jst(2026, 10, 17, 5, 59);  // 土曜の朝 6 時前に終わる夜間取引
  assert.equal(agoText(ageSeconds(monday, friday)), '2日2時間47分前');
});

test('null の経過は文字を出さない', () => {
  assert.equal(agoText(null), null);
});
