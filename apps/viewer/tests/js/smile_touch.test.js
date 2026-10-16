// static/smile_touch.js（#84: タッチ操作の規則）のテスト。node の標準のテストだけを使う（依存なし）:
//   cd apps/viewer && node --test tests/js/*.test.js
'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');

const SmileTouch = require(path.join(__dirname, '..', '..', 'src', 'optionchan_viewer', 'static', 'smile_touch.js'));
const { classify, nearest, clampPx, valueAt, edgeFactor, edgeDelta, shiftView, SLOP_PX, EDGE_PX, EDGE_RATE } = SmileTouch;

test('縦か横かの判定: SLOP_PX 未満は待つ、横（同じ量を含む）はなぞり、縦はスクロール', () => {
  assert.equal(SLOP_PX, 6);
  assert.equal(classify(0, 0, SLOP_PX), 'pending');
  assert.equal(classify(5, -5, SLOP_PX), 'pending');
  assert.equal(classify(6, 0, SLOP_PX), 'cursor');
  assert.equal(classify(-6, 5, SLOP_PX), 'cursor');
  assert.equal(classify(6, 6, SLOP_PX), 'cursor');
  assert.equal(classify(5, 6, SLOP_PX), 'scroll');
  assert.equal(classify(1, -20, SLOP_PX), 'scroll');
});

test('吸い付き: 一番近い行使価格、同じ距離なら先の値、空なら null', () => {
  const strikes = [24000, 24250, 24500, 24750];
  assert.equal(nearest(strikes, 24100), 24000);
  assert.equal(nearest(strikes, 24130), 24250);
  assert.equal(nearest(strikes, 24125), 24000);  // ちょうど中間は先の値（既存の nearestStrike と同じ）
  assert.equal(nearest(strikes, 10000), 24000);  // データの外は端の値
  assert.equal(nearest(strikes, 99999), 24750);
  assert.equal(nearest([24500, 24000, 24500], 24400), 24500);  // 順不同・重複あり
  assert.equal(nearest([], 24000), null);
});

test('px → 値は表示範囲に比例、指は描画領域に収める', () => {
  const view = { min: 24000, max: 26000 };
  assert.equal(valueAt(50, 50, 250, view), 24000);
  assert.equal(valueAt(250, 50, 250, view), 26000);
  assert.equal(valueAt(150, 50, 250, view), 25000);
  assert.equal(clampPx(10, 50, 250), 50);
  assert.equal(clampPx(300, 50, 250), 250);
  assert.equal(clampPx(120, 50, 250), 120);
});

test('端の帯: 帯の外は 0、端に近いほど強く、端より外は ±1', () => {
  assert.equal(EDGE_PX, 32);
  const L = 40;
  const R = 360;
  assert.equal(edgeFactor(200, L, R, EDGE_PX), 0);
  assert.equal(edgeFactor(L + EDGE_PX, L, R, EDGE_PX), 0);
  assert.equal(edgeFactor(R - EDGE_PX, L, R, EDGE_PX), 0);
  assert.equal(edgeFactor(L + 16, L, R, EDGE_PX), -0.5);
  assert.equal(edgeFactor(R - 8, L, R, EDGE_PX), 0.75);
  assert.equal(edgeFactor(L, L, R, EDGE_PX), -1);
  assert.equal(edgeFactor(R, L, R, EDGE_PX), 1);
  assert.equal(edgeFactor(0, L, R, EDGE_PX), -1);
  assert.equal(edgeFactor(500, L, R, EDGE_PX), 1);
});

test('送りの量 = 強さ × EDGE_RATE × 表示幅 × 秒', () => {
  assert.equal(EDGE_RATE, 0.6);
  assert.equal(edgeDelta(1, 5000, 1), 3000);
  assert.equal(edgeDelta(-0.5, 5000, 0.016), -0.5 * 0.6 * 5000 * 0.016);
  assert.equal(edgeDelta(0, 5000, 0.016), 0);
});

test('表示範囲の送り: 幅を保つ、データの端で止まる（止まったら同じオブジェクト）', () => {
  const bounds = { min: 20000, max: 30000 };
  assert.deepEqual(shiftView({ min: 24000, max: 26000 }, 100, bounds), { min: 24100, max: 26100 });
  assert.deepEqual(shiftView({ min: 24000, max: 26000 }, -100, bounds), { min: 23900, max: 25900 });
  assert.deepEqual(shiftView({ min: 27900, max: 29900 }, 500, bounds), { min: 28000, max: 30000 });
  assert.deepEqual(shiftView({ min: 20200, max: 22200 }, -500, bounds), { min: 20000, max: 22000 });
  const atEnd = { min: 28000, max: 30000 };
  assert.equal(shiftView(atEnd, 500, bounds), atEnd);
  const atStart = { min: 20000, max: 22000 };
  assert.equal(shiftView(atStart, -1, bounds), atStart);
  assert.deepEqual(shiftView(atEnd, -10, bounds), { min: 27990, max: 29990 });  // 反対向きには送れる
  const v = { min: 24000, max: 26000 };
  assert.equal(shiftView(v, 100, null), v);
  const wide = { min: 19000, max: 31000 };
  assert.equal(shiftView(wide, 100, bounds), wide);
});

test('なぞりを続けると端の行使価格まで届き、そこで止まる（送り + 吸い付きの組み合わせ）', () => {
  const strikes = [];
  for (let k = 20000; k <= 30000; k += 250) {
    strikes.push(k);
  }
  const bounds = { min: 19750, max: 30250 };
  let view = { min: 24000, max: 26000 };
  const L = 40;
  const R = 360;
  const px = R;  // 右端に指を置いたまま
  let steps = 0;
  for (;;) {
    const next = shiftView(view, edgeDelta(edgeFactor(px, L, R, EDGE_PX), view.max - view.min, 0.016), bounds);
    if (next === view) {
      break;
    }
    view = next;
    steps++;
    assert.ok(steps < 10000);
  }
  assert.equal(view.max, bounds.max);
  assert.equal(nearest(strikes, valueAt(px, L, R, view)), 30000);
});
