// static/atm_series.js（#101: /atm_iv の系列・期近つなぎの足の列）のテスト。node の標準のテストだけを使う（依存なし）:
//   cd apps/viewer && node --test tests/js/*.test.js
'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');

const AtmSeries = require(path.join(__dirname, '..', '..', 'src', 'optionchan_viewer', 'static', 'atm_series.js'));
const { SERIES, DEFAULT_SERIES, seriesOf, barsUrl, parseCsv, mergeBars, resample, monthLabel, monthLegend,
  rollMarkers, jumpView } = AtmSeries;

const T0 = Date.UTC(2026, 8, 30, 6, 0) / 1000;  // 2026-09-30 06:00 UTC（15:00 JST）

function bar(time, v, month) {
  const b = { time: time, open: v, high: v + 1, low: v - 1, close: v };
  if (month !== undefined) {
    b.month = month;
  }
  return b;
}

test('系列: value は一意、既定は 0、限月の列を読むのは期近つなぎだけ', () => {
  const values = SERIES.map((s) => s.value);
  assert.deepEqual(values, ['0', '1', 'roll', 'cm']);
  assert.equal(new Set(values).size, values.length);
  assert.equal(DEFAULT_SERIES, '0');
  assert.deepEqual(SERIES.filter((s) => s.months).map((s) => s.value), ['roll']);
  assert.equal(seriesOf('roll').label, '期近つなぎ');
  assert.equal(seriesOf('cm').label, '30日固定');
  assert.deepEqual(SERIES.filter((s) => s.fixedIv).map((s) => s.value), ['cm']);
  assert.equal(seriesOf('2').value, '0');  // 無い値は既定
});

test('barsUrl: n=0/1 は今と同じ文字列、期近つなぎは n=roll', () => {
  assert.equal(barsUrl(seriesOf('0'), '1h', 'trade', null, null), '/atm_iv_bars?tf=1h&n=0');
  assert.equal(barsUrl(seriesOf('1'), '1m', 'mid', 100, 200), '/atm_iv_bars?tf=1m&n=1&iv=mid&start=100&end=200');
  assert.equal(barsUrl(seriesOf('roll'), '1d', 'trade', 100, 200), '/atm_iv_bars?tf=1d&n=roll&start=100&end=200');
  // 30日固定（#100）は IV を選べないので iv を付けない
  assert.equal(barsUrl(seriesOf('cm'), '1h', 'mid', null, null), '/atm_iv_bars?tf=1h&n=cm');
});

test('parseCsv: 6 列の CSV は限月（month）を読み、5 列の CSV には month が無い', () => {
  const six = parseCsv('18.0,19.0,17.5,18.5,2026-09-30 06:00:00+00:00,2026-10-08\n' +
    ',,,,2026-09-30 07:00:00+00:00,2026-10-08\n17.0,17.5,16.5,17.25,2026-09-30 08:00:00+00:00,2026-11-12\n');
  assert.deepEqual(six, [
    { time: T0, open: 18.0, high: 19.0, low: 17.5, close: 18.5, month: '2026-10-08' },
    { time: T0 + 7200, open: 17.0, high: 17.5, low: 16.5, close: 17.25, month: '2026-11-12' },
  ]);
  const five = parseCsv('18.0,19.0,17.5,18.5,2026-09-30 06:00:00+00:00\n');
  assert.deepEqual(five, [{ time: T0, open: 18.0, high: 19.0, low: 17.5, close: 18.5 }]);
  assert.equal('month' in five[0], false);
});

test('resample: 同じ限月は今と同じ結果、違う限月は同じ区間でも 1 本にしない', () => {
  const same = [bar(T0, 20), bar(T0 + 60, 22), bar(T0 + 300, 21)];
  assert.deepEqual(resample(same, 300), [
    { time: T0, open: 20, high: 23, low: 19, close: 22 },
    { time: T0 + 300, open: 21, high: 22, low: 20, close: 21 },
  ]);
  const mixed = [bar(T0, 20, '2026-10-08'), bar(T0 + 60, 30, '2026-11-12'), bar(T0 + 120, 31, '2026-11-12')];
  assert.deepEqual(resample(mixed, 900), [
    { time: T0, open: 20, high: 21, low: 19, close: 20, month: '2026-10-08' },
    { time: T0, open: 30, high: 32, low: 29, close: 31, month: '2026-11-12' },
  ]);
  assert.equal(resample(mixed, 60), mixed);
});

test('rollMarkers: 限月が変わった最初の足だけ。限月の無い列は空', () => {
  const bars = [bar(T0, 20, '2026-10-08'), bar(T0 + 3600, 21, '2026-10-08'), bar(T0 + 7200, 18, '2026-11-12'),
    bar(T0 + 10800, 19, '2026-11-12'), bar(T0 + 14400, 17, '2026-12-10')];
  assert.deepEqual(rollMarkers(bars), [
    { time: T0 + 7200, month: '2026-11-12', text: '11月限' },
    { time: T0 + 14400, month: '2026-12-10', text: '12月限' },
  ]);
  assert.deepEqual(rollMarkers([bar(T0, 20), bar(T0 + 60, 21)]), []);
  assert.deepEqual(rollMarkers([]), []);
});

test('rollMarkers: 左・右に読み足してつないだ列でも、つなぎ目に余計な印が付かない', () => {
  const all = [bar(T0, 20, '2026-10-08'), bar(T0 + 3600, 21, '2026-10-08'), bar(T0 + 7200, 18, '2026-11-12'),
    bar(T0 + 10800, 19, '2026-11-12')];
  // 最初に後ろの 2 本（11 月限だけ）、左に読み足して前の 2 本、右に読み足して重なる 1 本
  let joined = all.slice(2);
  assert.deepEqual(rollMarkers(joined), []);
  joined = mergeBars(all.slice(0, 2), joined);
  joined = mergeBars(joined, all.slice(3));
  assert.deepEqual(joined, all);
  assert.deepEqual(rollMarkers(joined), rollMarkers(all));
  assert.equal(rollMarkers(joined).length, 1);
});

test('monthLabel・monthLegend', () => {
  assert.equal(monthLabel('2026-12-10'), '12月限');
  assert.equal(monthLabel('2027-01-14'), '1月限');
  assert.equal(monthLegend('2026-12-10'), '12月限（最終取引日 2026-12-10）');
});

test('jumpView: その日の 12:00 JST を中心に、足ごとの幅の範囲。不正・未来・2010 年より前は null', () => {
  const now = Date.UTC(2026, 9, 17, 3) / 1000;  // 2026-10-17 12:00 JST
  const noon = Date.UTC(2020, 2, 16, 3) / 1000;  // 2020-03-16 12:00 JST
  assert.deepEqual(jumpView('2020-03-16', '1h', now), { from: noon - 3 * 86400, to: noon + 3 * 86400, atRight: false });
  assert.deepEqual(jumpView('2020-03-16', '1d', now), { from: noon - 60 * 86400, to: noon + 60 * 86400, atRight: false });
  assert.equal(jumpView('2020-03-16', '1m', now).to - jumpView('2020-03-16', '1m', now).from, 4 * 3600);
  assert.ok(jumpView('2026-10-17', '1h', now));  // 今日は可
  for (const v of ['', '2026-10-18', '2009-12-31', '2020-02-30', '2020-3-16', '2020-03-16x']) {
    assert.equal(jumpView(v, '1h', now), null, v);
  }
  assert.equal(jumpView('2020-03-16', '2h', now), null);
});

test('足が 0 本の説明: 0・1 は期近つなぎへ、mid は約定へ、ほかは説明だけ', () => {
  const { emptyHint } = AtmSeries;
  assert.deepEqual(SERIES.filter((s) => s.fromToday).map((s) => s.value), ['0', '1']);
  for (const iv of ['mid', 'trade']) {
    assert.equal(emptyHint(seriesOf('0'), iv).action, 'roll');
    assert.match(emptyHint(seriesOf('1'), iv).text, /限月「1」/);
  }
  assert.equal(emptyHint(seriesOf('roll'), 'mid').action, 'trade');
  assert.equal(emptyHint(seriesOf('roll'), 'trade').action, null);
  // 30日固定は IV を選べない（mid として扱う）ので、約定へは誘わない
  assert.equal(emptyHint(seriesOf('cm'), 'mid').action, null);
  assert.equal(emptyHint(seriesOf('cm'), 'mid').text, 'データがありません');
});

test('emptyHint: 欠けの区間（#123）を渡すと mid・30日固定で期間の文が付き、約定・区間なしでは今の文のまま', () => {
  const { emptyHint } = AtmSeries;
  // 合成の区間（DataGaps.parse の形）
  const g1 = { prevAt: 0, nextAt: 3600, cause: 'unknown', status: 'none', period: '2001-01-04 10:00〜2001-01-04 11:00' };
  const g2 = { prevAt: 7200, nextAt: 9000, cause: 'bug_format', status: 'filled', period: '2001-01-04 12:00〜2001-01-04 12:30' };
  const base = {};
  for (const [n, iv] of [['0', 'mid'], ['roll', 'mid'], ['cm', 'mid'], ['cm', 'trade'], ['roll', 'trade'], ['0', 'trade']]) {
    base[n + iv] = emptyHint(seriesOf(n), iv);
    // 省略・空は今と同じ
    assert.deepEqual(emptyHint(seriesOf(n), iv, []), base[n + iv]);
    assert.deepEqual(emptyHint(seriesOf(n), iv, undefined), base[n + iv]);
  }
  const suffix = '。この期間は欠け（2001-01-04 10:00〜2001-01-04 11:00）にかかっています';
  assert.deepEqual(emptyHint(seriesOf('roll'), 'mid', [g1]), { text: base.rollmid.text + suffix, action: 'trade' });
  assert.deepEqual(emptyHint(seriesOf('0'), 'mid', [g1]), { text: base['0mid'].text + suffix, action: 'roll' });
  assert.equal(emptyHint(seriesOf('cm'), 'mid', [g1]).text, 'データがありません' + suffix);
  assert.equal(emptyHint(seriesOf('cm'), 'trade', [g1]).text, 'データがありません' + suffix);
  assert.equal(emptyHint(seriesOf('cm'), 'mid', [g1]).action, null);
  // 約定は区間があっても今の文のまま
  assert.deepEqual(emptyHint(seriesOf('roll'), 'trade', [g1]), base.rolltrade);
  assert.deepEqual(emptyHint(seriesOf('0'), 'trade', [g1, g2]), base['0trade']);
  // 2 件なら「ほか 1 件」
  assert.equal(emptyHint(seriesOf('roll'), 'mid', [g1, g2]).text,
    base.rollmid.text + '。この期間は欠け（2001-01-04 10:00〜2001-01-04 11:00 ほか 1 件）にかかっています');
});
