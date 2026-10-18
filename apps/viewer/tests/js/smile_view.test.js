// static/smile_view.js（/smile の注記の文と横の表示範囲。#127）のテスト。値は合成:
//   cd apps/viewer && node --test tests/js/*.test.js
'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');

const SmileView = require(path.join(__dirname, '..', '..', 'src', 'optionchan_viewer', 'static', 'smile_view.js'));
const SmileCsv = require(path.join(__dirname, '..', '..', 'src', 'optionchan_viewer', 'static', 'smile_csv.js'));
const { SOURCE_NOTE, NO_QUOTE_NOTE, snapshotNote, margin, bounds, clamp, drawnRange, initialView } = SmileView;

const MIN_WIDTH = 500;

// 合成のスナップショット（parseSnapshot を通す）。rows は [month, type, target_price, iv, mid_iv, bid_iv, ask_iv, otm]
function snap(rows, opts) {
  const o = opts || {};
  const head = ['978580800', '2001-01-11', o.atm1 === undefined ? '50000' : o.atm1, '2001-02-08',
    o.atm2 === undefined ? '50000' : o.atm2, o.source || ''].join(',');
  const body = rows.map(([month, type, k, iv, mid, bid, ask, otm]) => [month, type, k, v(iv), '', '', v(bid), v(ask), '',
    '', v(mid), otm].join(','));
  return SmileCsv.parseSnapshot(head + '\n' + body.join('\n') + '\n');
}

function v(x) {
  return x === null || x === undefined ? '' : String(x);
}

// 行使価格 lo〜hi（step 刻み）の OTM 合成の行（put は ATM 未満、call は ATM 以上が otm=1。反対側は otm=0）
function strikes(month, lo, hi, step, atm, quote) {
  const out = [];
  for (let k = lo; k <= hi; k += step) {
    const iv = 20 + Math.abs(k - atm) / 1000;
    const q = quote ? [iv, iv - 0.5, iv + 0.5] : [null, null, null];
    out.push([month, 2, k, iv, ...q, k < atm ? 1 : 0]);
    out.push([month, 1, k, iv, ...q, k >= atm ? 1 : 0]);
  }
  return out;
}

const NORMAL = snap(strikes(1, 30000, 80000, 500, 50000, true).concat(strikes(2, 25000, 90000, 500, 50000, true)));
const FILL = snap(strikes(1, 30000, 80000, 500, 50000, false), { source: 'fill114' });

test('snapshotNote: fill114 は作り直した旨の文（Issue 番号を出さない）', () => {
  assert.equal(SOURCE_NOTE, 'この時点は取得できなかったため、別に購入した約定データから作り直しています。' +
    '売り気配・買い気配は無く、点（約定の IV）だけを表示しています');
  assert.equal(FILL.hasQuote, false);
  assert.equal(snapshotNote(FILL), SOURCE_NOTE);
  assert.ok(!snapshotNote(FILL).includes('#'));
  // 気配があっても fill114 の文（取引時間外の文は出さない）
  assert.equal(snapshotNote({ source: 'fill114', hasQuote: true }), SOURCE_NOTE);
});

test('snapshotNote: 気配が無い通常の時点は取引時間外の文、気配があれば・時点が無ければ null', () => {
  assert.equal(NO_QUOTE_NOTE, '気配がありません（取引時間外）。約定の IV の点だけを表示しています');
  assert.equal(snapshotNote({ source: null, hasQuote: false }), NO_QUOTE_NOTE);
  assert.equal(snapshotNote(NORMAL), null);
  assert.equal(snapshotNote(null), null);
  assert.ok(!NO_QUOTE_NOTE.includes('#'));
});

test('margin: 下限 250 円、幅の 2%', () => {
  assert.equal(margin(30000, 31000), 250);
  assert.equal(margin(25000, 90000), 1300);
});

test('bounds: 表示と固定の行使価格の最小〜最大 + margin。null は無視', () => {
  assert.deepEqual(bounds([NORMAL, null]), { min: 25000 - 1300, max: 90000 + 1300 });
  assert.deepEqual(bounds([null, FILL]), { min: 30000 - 1000, max: 80000 + 1000 });
  assert.deepEqual(bounds([FILL, NORMAL]), { min: 23700, max: 91300 });
  assert.equal(bounds([null, null]), null);
  assert.equal(bounds([{ lo: null, hi: null }]), null);
});

test('clamp: 幅は minWidth〜bounds の幅、bounds の中へ寄せる。bounds が無ければそのまま', () => {
  const b = { min: 1000, max: 5000 };
  assert.deepEqual(clamp(2000, 3000, b, MIN_WIDTH), { min: 2000, max: 3000 });
  assert.deepEqual(clamp(2000, 2100, b, MIN_WIDTH), { min: 1800, max: 2300 });
  assert.deepEqual(clamp(0, 9000, b, MIN_WIDTH), { min: 1000, max: 5000 });
  assert.deepEqual(clamp(500, 1500, b, MIN_WIDTH), { min: 1000, max: 2000 });
  assert.deepEqual(clamp(4800, 5800, b, MIN_WIDTH), { min: 4000, max: 5000 });
  assert.deepEqual(clamp(1, 2, null, MIN_WIDTH), { min: 1, max: 2 });
});

test('drawnRange: 描いている限月・表示の、値のある行の行使価格の最小〜最大', () => {
  assert.deepEqual(drawnRange([NORMAL, null], [1, 2], true), { lo: 25000, hi: 90000 });
  assert.deepEqual(drawnRange([NORMAL, null], [1], true), { lo: 30000, hi: 80000 });
  assert.deepEqual(drawnRange([NORMAL, null], [2], false), { lo: 25000, hi: 90000 });
  assert.equal(drawnRange([NORMAL, null], [], true), null);
  // 値が全部 null の行（10000）は描かれないので入らない
  const empty = snap([[1, 2, 10000, null, null, null, null, 1]].concat(strikes(1, 30000, 40000, 500, 35000, true)));
  assert.deepEqual(drawnRange([empty], [1], true), { lo: 30000, hi: 40000 });
  assert.equal(empty.lo, 10000);
  // OTM 合成では otm=0 の行だけにある行使価格は入らない。call・put 別では入る
  const side = snap([[1, 1, 20000, 30, null, null, null, 0]].concat(strikes(1, 30000, 40000, 500, 35000, true)));
  assert.deepEqual(drawnRange([side], [1], true), { lo: 30000, hi: 40000 });
  assert.deepEqual(drawnRange([side], [1], false), { lo: 20000, hi: 40000 });
  // 約定の IV だけ・bid だけの行も描かれる
  const partial = snap([[1, 2, 21000, 30, null, null, null, 1], [1, 1, 61000, null, null, null, 12, 1]]);
  assert.deepEqual(drawnRange([partial], [1], true), { lo: 21000, hi: 61000 });
  // 固定の範囲も入る、null は無視
  const pinned = snap(strikes(1, 20000, 45000, 500, 35000, true));
  assert.deepEqual(drawnRange([FILL, pinned], [1], true), { lo: 20000, hi: 80000 });
  assert.deepEqual(drawnRange([null, null], [1, 2], true), null);
});

test('initialView: 描いている行使価格の最小〜最大を含み、bounds の中', () => {
  for (const [snaps, months, otm] of [[[NORMAL, null], [1, 2], true], [[NORMAL, null], [1], true],
    [[NORMAL, null], [2], false], [[FILL, null], [1, 2], true], [[FILL, NORMAL], [1], true]]) {
    const r = drawnRange(snaps, months, otm);
    const view = initialView(snaps, months, otm, MIN_WIDTH);
    const b = bounds(snaps);
    assert.ok(view.min <= r.lo && view.max >= r.hi, JSON.stringify([months, otm, view]));
    assert.ok(view.min >= b.min && view.max <= b.max, JSON.stringify([months, otm, view]));
  }
  // 1 限月だけ: 30000〜80000 + margin 1000（2 限月の端までは広げない）
  assert.deepEqual(initialView([NORMAL, null], [1], true, MIN_WIDTH), { min: 29000, max: 81000 });
  assert.deepEqual(initialView([NORMAL, null], [1, 2], true, MIN_WIDTH), { min: 23700, max: 91300 });
});

test('initialView: 旧来の ATM ±10% より広い（回帰）', () => {
  const view = initialView([NORMAL, null], [1], true, MIN_WIDTH);
  assert.ok(view.min < 50000 * 0.9 && view.max > 50000 * 1.1);
});

test('initialView: 描ける行が無ければ bounds、時点が無ければ null、狭いデータは minWidth に広げて bounds で頭打ち', () => {
  assert.deepEqual(initialView([NORMAL, null], [], true, MIN_WIDTH), bounds([NORMAL, null]));
  assert.equal(initialView([null, null], [1, 2], true, MIN_WIDTH), null);
  const one = snap([[1, 2, 30000, 20, 20, 19.5, 20.5, 1]]);
  // 1 行: 範囲 30000〜30000 + margin 250 → 幅 500 = minWidth = bounds の幅
  assert.deepEqual(initialView([one, null], [1], true, MIN_WIDTH), { min: 29750, max: 30250 });
  assert.deepEqual(initialView([one, null], [1], true, 2000), { min: 29750, max: 30250 });
});
