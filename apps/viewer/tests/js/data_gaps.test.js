// static/data_gaps.js（#123: /atm_iv の過去データの欠けの帯・印）のテスト。入力は合成した時刻だけ:
//   cd apps/viewer && node --test tests/js/*.test.js
'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');

const DataGaps = require(path.join(__dirname, '..', '..', 'src', 'optionchan_viewer', 'static', 'data_gaps.js'));
const AtmSeries = require(path.join(__dirname, '..', '..', 'src', 'optionchan_viewer', 'static', 'atm_series.js'));
const { parse, modeOf, spans, assign, overlapping, legendText, mergeMarkers } = DataGaps;

const H = 3600;
const DAY = 86400;

// JST の日時 → UTC 秒（合成。2001-01-01 は月曜）
function jst(m, d, h, mi) {
  return Date.UTC(2001, m - 1, d, h, mi || 0) / 1000 - 9 * H;
}

// 合成の区間（parse を通す）
function gaps(...rows) {
  return parse(rows.map((r) => r.join(',')).join('\n') + '\n');
}

function bars(times) {
  return times.map((t) => ({ time: t, open: 1, high: 2, low: 0.5, close: 1.5 }));
}

// 1 時間足: 2001-01-04（木）09:00〜15:00 JST
const HOURLY = bars([9, 10, 11, 12, 13, 14, 15].map((h) => jst(1, 4, h)));

test('parse: 4 列・数字・prevAt < nextAt の行だけ、prevAt 昇順、period は JST', () => {
  const text = [
    `${jst(1, 4, 12)},${jst(1, 4, 13)},unknown,none`,
    `${jst(1, 4, 10, 30)},${jst(1, 4, 12)},bug_format,filled`,
    `${jst(1, 4, 10)},${jst(1, 4, 11)},unknown,none,extra`,  // 5 列
    `${jst(1, 4, 10)},${jst(1, 4, 11)},unknown`,  // 3 列
    `x,${jst(1, 4, 11)},unknown,none`,  // 数字でない
    `-1,${jst(1, 4, 11)},unknown,none`,
    `${jst(1, 4, 11)},${jst(1, 4, 11)},unknown,none`,  // prevAt = nextAt
    `${jst(1, 4, 12)},${jst(1, 4, 11)},unknown,none`,  // 逆
    '',
    `${jst(1, 4, 12)},${jst(1, 4, 14)},,`,  // 空の cause・status は文字列のまま
  ].join('\n');
  const got = parse(text);
  assert.deepEqual(got.map((g) => [g.prevAt, g.cause, g.status]), [
    [jst(1, 4, 10, 30), 'bug_format', 'filled'],
    [jst(1, 4, 12), 'unknown', 'none'],
    [jst(1, 4, 12), '', ''],
  ]);
  assert.equal(got[0].nextAt, jst(1, 4, 12));
  assert.equal(got[0].period, '2001-01-04 10:30〜2001-01-04 12:00');
  // 日付をまたぐ（UTC と JST で日が違う）
  assert.equal(gaps([jst(1, 5, 23, 5), jst(1, 6, 2), 'unknown', 'none'])[0].period, '2001-01-05 23:05〜2001-01-06 02:00');
  assert.deepEqual(parse(''), []);
  assert.deepEqual(parse(null), []);
  assert.deepEqual(parse(undefined), []);
});

test('modeOf: 約定（0・1・期近つなぎ）は trade、mid・30日固定は quote', () => {
  assert.equal(modeOf(AtmSeries.seriesOf('0'), 'trade'), 'trade');
  assert.equal(modeOf(AtmSeries.seriesOf('roll'), 'trade'), 'trade');
  assert.equal(modeOf(AtmSeries.seriesOf('1'), 'mid'), 'quote');
  assert.equal(modeOf(AtmSeries.seriesOf('cm'), 'trade'), 'quote');
  assert.equal(modeOf(AtmSeries.seriesOf('cm'), 'mid'), 'quote');
});

test('spans: 分足・1 時間足は [time, time + 秒)、日足は 16:00 JST が境目（先頭は time − 8 時間）', () => {
  assert.deepEqual(spans(HOURLY.slice(0, 2), H), [
    { time: jst(1, 4, 9), from: jst(1, 4, 9), to: jst(1, 4, 10) },
    { time: jst(1, 4, 10), from: jst(1, 4, 10), to: jst(1, 4, 11) },
  ]);
  assert.deepEqual(spans(bars([jst(1, 4, 9, 5)]), 300), [{ time: jst(1, 4, 9, 5), from: jst(1, 4, 9, 5), to: jst(1, 4, 9, 10) }]);
  // 日足: 木・金・月（土日を前の足で越える）
  const daily = bars([jst(1, 4, 0), jst(1, 5, 0), jst(1, 8, 0)]);
  assert.deepEqual(spans(daily, DAY), [
    { time: jst(1, 4, 0), from: jst(1, 3, 16), to: jst(1, 4, 16) },
    { time: jst(1, 5, 0), from: jst(1, 4, 16), to: jst(1, 5, 16) },
    { time: jst(1, 8, 0), from: jst(1, 5, 16), to: jst(1, 8, 16) },
  ]);
  assert.deepEqual(spans([], H), []);
});

test('1 時間足: 区間が 1 本の足の中に収まる → その足に印 1 つ', () => {
  const r = assign(HOURLY, H, gaps([jst(1, 4, 11, 10), jst(1, 4, 11, 40), 'unknown', 'none']), 'quote');
  assert.deepEqual(r.marks.map((m) => m.time), [jst(1, 4, 11)]);
  assert.equal(r.marks[0].gaps.length, 1);
  assert.deepEqual(r.bands, []);
});

test('足ごと詰められた区間（区間の間に足が無い）→ next_at より後で最初の足に印', () => {
  // 11:00・12:00 の足が無い（詰められている）。欠けは 11:01〜12:59
  const packed = HOURLY.filter((b) => b.time !== jst(1, 4, 11) && b.time !== jst(1, 4, 12));
  const g = gaps([jst(1, 4, 11), jst(1, 4, 13), 'unknown', 'none']);
  assert.deepEqual(assign(packed, H, g, 'quote').marks.map((m) => m.time), [jst(1, 4, 13)]);
  assert.deepEqual(assign(packed, H, g, 'trade').marks.map((m) => m.time), [jst(1, 4, 13)]);
  // 約定の filled は帯の役なので、当たる足が無ければ何も出さない
  const filled = gaps([jst(1, 4, 11), jst(1, 4, 13), 'bug_format', 'filled']);
  assert.deepEqual(assign(packed, H, filled, 'trade'), { bands: [], marks: [] });
  assert.deepEqual(assign(packed, H, filled, 'quote').marks.map((m) => m.time), [jst(1, 4, 13)]);
});

test('1 本の足に 2 区間 → 印 1 つ、凡例は「区間 2 件: 」', () => {
  const g = gaps([jst(1, 4, 11, 30), jst(1, 4, 11, 50), 'bug_comma', 'none'],
    [jst(1, 4, 11, 5), jst(1, 4, 11, 20), 'unknown', 'none']);
  const r = assign(HOURLY, H, g, 'quote');
  assert.equal(r.marks.length, 1);
  assert.equal(r.marks[0].time, jst(1, 4, 11));
  // prevAt 順
  assert.deepEqual(r.marks[0].gaps.map((x) => x.cause), ['unknown', 'bug_comma']);
  const text = legendText(r.marks[0].gaps, 'quote');
  assert.match(text, /^区間 2 件: /);
  assert.equal(text, '区間 2 件: 停止・取得の失敗（欠け）2001-01-04 11:05〜2001-01-04 11:20');
});

test('日足: 金曜 17:00〜土曜 02:00 JST → 月曜の足、木曜 10:00〜11:00 JST → 木曜の足', () => {
  const daily = bars([jst(1, 3, 0), jst(1, 4, 0), jst(1, 5, 0), jst(1, 8, 0), jst(1, 9, 0)]);
  const fri = gaps([jst(1, 5, 17), jst(1, 6, 2), 'unknown', 'none']);
  assert.deepEqual(assign(daily, DAY, fri, 'quote').marks.map((m) => m.time), [jst(1, 8, 0)]);
  const thu = gaps([jst(1, 4, 10), jst(1, 4, 11), 'unknown', 'none']);
  assert.deepEqual(assign(daily, DAY, thu, 'quote').marks.map((m) => m.time), [jst(1, 4, 0)]);
  // 先頭の足は前日 16:00 JST から: 水曜 0 時の足に 火曜 17:00〜18:00 の区間が当たる
  const tue = gaps([jst(1, 2, 17), jst(1, 2, 18), 'unknown', 'none']);
  assert.deepEqual(assign(daily, DAY, tue, 'quote').marks.map((m) => m.time), [jst(1, 3, 0)]);
  // 先頭の足より前（火曜 15:00〜15:30）は出さない
  assert.deepEqual(assign(daily, DAY, gaps([jst(1, 2, 15), jst(1, 2, 15, 30), 'unknown', 'none']), 'quote').marks, []);
  // 金曜夜間の帯（約定・filled）は月曜の足
  const band = assign(daily, DAY, gaps([jst(1, 5, 17), jst(1, 6, 2), 'bug_format', 'filled']), 'trade');
  assert.deepEqual(band.bands.map((b) => b.time), [jst(1, 8, 0)]);
});

test('日足: displayBars を通した足に、補正なしで取引日の足へ帯・印が付く（#124）', () => {
  const daily = AtmSeries.displayBars(bars([jst(1, 3, 0), jst(1, 4, 0), jst(1, 5, 0), jst(1, 8, 0), jst(1, 9, 0)]), DAY);
  // 足の time は取引日の 00:00 JST のまま
  assert.deepEqual(daily.map((b) => b.time), [jst(1, 3, 0), jst(1, 4, 0), jst(1, 5, 0), jst(1, 8, 0), jst(1, 9, 0)]);
  // 金曜 17:00〜土曜 02:00 JST の欠け → 月曜の足に印
  const fri = gaps([jst(1, 5, 17), jst(1, 6, 2), 'unknown', 'none']);
  assert.deepEqual(assign(daily, DAY, fri, 'quote').marks.map((m) => m.time), [jst(1, 8, 0)]);
  // 木曜 10:00〜11:00 JST → 木曜の足
  const thu = gaps([jst(1, 4, 10), jst(1, 4, 11), 'unknown', 'none']);
  assert.deepEqual(assign(daily, DAY, thu, 'quote').marks.map((m) => m.time), [jst(1, 4, 0)]);
  // 作った区間（約定・filled）: 木曜 15:00〜金曜 10:00 JST → 木曜・金曜の足に帯（16:00 JST の境目）
  const band = assign(daily, DAY, gaps([jst(1, 4, 15), jst(1, 5, 10), 'bug_format', 'filled']), 'trade');
  assert.deepEqual(band.bands.map((b) => b.time), [jst(1, 4, 0), jst(1, 5, 0)]);
  assert.deepEqual(band.marks, []);
});

test('5 分・15 分足・期近つなぎでも足の時刻と長さだけで同じ規則', () => {
  const g = gaps([jst(1, 4, 10, 7), jst(1, 4, 10, 21), 'unknown', 'none']);  // 欠けは 10:08〜10:20
  const five = bars([0, 5, 10, 15, 20, 25].map((m) => jst(1, 4, 10, m)));
  assert.deepEqual(assign(five, 300, g, 'quote').marks.map((m) => m.time), [jst(1, 4, 10, 5)]);
  const fifteen = bars([0, 15, 30].map((m) => jst(1, 4, 10, m)));
  assert.deepEqual(assign(fifteen, 900, g, 'quote').marks.map((m) => m.time), [jst(1, 4, 10, 0)]);
  // 帯は当たる足すべて: 5 分足は 10:05・10:10・10:15・10:20、15 分足は 10:00・10:15
  const filled = gaps([jst(1, 4, 10, 7), jst(1, 4, 10, 21), 'unknown', 'filled']);
  assert.deepEqual(assign(five, 300, filled, 'trade').bands.map((b) => b.time),
    [5, 10, 15, 20].map((m) => jst(1, 4, 10, m)));
  assert.deepEqual(assign(fifteen, 900, filled, 'trade').bands.map((b) => b.time), [jst(1, 4, 10, 0), jst(1, 4, 10, 15)]);
  // 期近つなぎ（限月の付いた足）: month は使わない
  const roll = HOURLY.map((b, i) => Object.assign({ month: i < 3 ? '2001-01-11' : '2001-02-08' }, b));
  const r = assign(roll, H, gaps([jst(1, 4, 11, 10), jst(1, 4, 12, 30), 'unknown', 'none']), 'trade');
  assert.deepEqual(r.marks.map((m) => m.time), [jst(1, 4, 11)]);
  // 1 分足
  const one = bars([0, 1, 2, 3].map((m) => jst(1, 4, 10, m)));
  assert.deepEqual(assign(one, 60, gaps([jst(1, 4, 10, 1), jst(1, 4, 10, 3), 'unknown', 'none']), 'quote').marks
    .map((m) => m.time), [jst(1, 4, 10, 2)]);
});

test('filled: 約定は帯（印なし）、mid・30日固定は印（帯なし）。none はどれでも印。partial は約定で帯', () => {
  const span = [jst(1, 4, 10, 30), jst(1, 4, 12)];
  const filled = gaps([...span, 'bug_format', 'filled']);
  let r = assign(HOURLY, H, filled, 'trade');
  assert.deepEqual(r.bands.map((b) => b.time), [jst(1, 4, 10), jst(1, 4, 11)]);
  assert.deepEqual(r.marks, []);
  r = assign(HOURLY, H, filled, 'quote');
  assert.deepEqual(r.bands, []);
  assert.deepEqual(r.marks.map((m) => m.time), [jst(1, 4, 10)]);
  for (const mode of ['trade', 'quote']) {
    r = assign(HOURLY, H, gaps([...span, 'bug_format', 'none']), mode);
    assert.deepEqual(r.bands, [], mode);
    assert.deepEqual(r.marks.map((m) => m.time), [jst(1, 4, 10)], mode);
  }
  r = assign(HOURLY, H, gaps([...span, 'bug_comma', 'partial']), 'trade');
  assert.deepEqual(r.bands.map((b) => b.time), [jst(1, 4, 10), jst(1, 4, 11)]);
  assert.deepEqual(r.marks, []);
  // 未知の status は印の役
  r = assign(HOURLY, H, gaps([...span, 'bug_comma', 'other']), 'trade');
  assert.deepEqual(r.bands, []);
  assert.equal(r.marks.length, 1);
});

test('境界: 1 時間足で 10:30〜12:00 JST は 10:00・11:00 の足に当たり、12:00 の足には当たらない', () => {
  const g = gaps([jst(1, 4, 10, 30), jst(1, 4, 12), 'unknown', 'none']);
  const r = assign(HOURLY, H, g, 'quote');
  assert.deepEqual(r.marks.map((m) => m.time), [jst(1, 4, 10)]);
  const band = assign(HOURLY, H, gaps([jst(1, 4, 10, 30), jst(1, 4, 12), 'unknown', 'filled']), 'trade');
  assert.deepEqual(band.bands.map((b) => b.time), [jst(1, 4, 10), jst(1, 4, 11)]);
  // prev_at・next_at ちょうどの時刻は欠けではない: 11:59〜12:01 は 12:00 の足だけ
  assert.deepEqual(assign(HOURLY, H, gaps([jst(1, 4, 11, 59), jst(1, 4, 12, 1), 'unknown', 'filled']), 'trade').bands
    .map((b) => b.time), [jst(1, 4, 12)]);
  // 分の時刻が 1 つも無い区間（隣り合う 2 分）は無視
  assert.deepEqual(assign(HOURLY, H, gaps([jst(1, 4, 11), jst(1, 4, 11, 1), 'unknown', 'none']), 'quote'),
    { bands: [], marks: [] });
});

test('描いている足の範囲の外の区間は帯・印とも 0', () => {
  for (const status of ['none', 'filled']) {
    for (const mode of ['trade', 'quote']) {
      const before = gaps([jst(1, 4, 6), jst(1, 4, 8), 'unknown', status]);
      const after = gaps([jst(1, 4, 17), jst(1, 4, 18), 'unknown', status]);
      assert.deepEqual(assign(HOURLY, H, before, mode), { bands: [], marks: [] });
      assert.deepEqual(assign(HOURLY, H, after, mode), { bands: [], marks: [] });
    }
  }
  assert.deepEqual(assign([], H, gaps([jst(1, 4, 10), jst(1, 4, 11), 'unknown', 'none']), 'quote'), { bands: [], marks: [] });
  assert.deepEqual(assign(HOURLY, H, [], 'quote'), { bands: [], marks: [] });
});

test('assign: 帯・印は time 昇順で一意、time は足の time のどれか', () => {
  const g = gaps([jst(1, 4, 9, 10), jst(1, 4, 9, 20), 'unknown', 'none'],
    [jst(1, 4, 9, 30), jst(1, 4, 12, 30), 'bug_format', 'filled'],
    [jst(1, 4, 11, 5), jst(1, 4, 11, 8), 'unknown', 'none'],
    [jst(1, 4, 14, 10), jst(1, 4, 14, 20), 'bug_comma', 'partial']);
  const r = assign(HOURLY, H, g, 'trade');
  const times = new Set(HOURLY.map((b) => b.time));
  for (const list of [r.bands, r.marks]) {
    const ts = list.map((x) => x.time);
    assert.deepEqual(ts, [...ts].sort((a, b) => a - b));
    assert.equal(new Set(ts).size, ts.length);
    assert.ok(ts.every((t) => times.has(t)));
  }
  assert.deepEqual(r.bands.map((b) => b.time), [9, 10, 11, 12, 14].map((h) => jst(1, 4, h)));
  assert.deepEqual(r.marks.map((m) => m.time), [jst(1, 4, 9), jst(1, 4, 11)]);
});

test('mergeMarkers: 乗り換えの印と欠けの印を合わせて time 昇順、同じ time は乗り換えが先', () => {
  const roll = [{ time: 10, text: 'a' }, { time: 30, text: 'b' }];
  const gap = [{ time: 5, text: 'x' }, { time: 10, text: 'y' }, { time: 40, text: 'z' }];
  assert.deepEqual(mergeMarkers(roll, gap).map((m) => m.text), ['x', 'a', 'y', 'b', 'z']);
  assert.deepEqual(mergeMarkers([], gap), gap);
  assert.deepEqual(mergeMarkers(roll, []), roll);
  // 期近つなぎの足から作った印（AtmSeries.rollMarkers）と assign の印
  const rollBars = HOURLY.map((b, i) => Object.assign({ month: i < 3 ? '2001-01-11' : '2001-02-08' }, b));
  const rm = AtmSeries.rollMarkers(rollBars);
  const gm = assign(rollBars, H, gaps([jst(1, 4, 9, 10), jst(1, 4, 9, 20), 'unknown', 'none'],
    [jst(1, 4, 12, 10), jst(1, 4, 12, 20), 'unknown', 'none'], [jst(1, 4, 14, 10), jst(1, 4, 14, 20), 'unknown', 'none']),
  'trade').marks;
  const merged = mergeMarkers(rm, gm);
  assert.deepEqual(merged.map((m) => m.time), [jst(1, 4, 9), jst(1, 4, 12), jst(1, 4, 12), jst(1, 4, 14)]);
  assert.equal(merged[1].month, '2001-02-08');  // 同じ time は乗り換えの印が先
});

test('overlapping: 欠けの分が [start, end) にかかる区間（status を問わない）', () => {
  const g = gaps([jst(1, 4, 10), jst(1, 4, 11), 'unknown', 'none'], [jst(1, 4, 12), jst(1, 4, 13), 'bug_format', 'filled'],
    [jst(1, 4, 15), jst(1, 4, 15, 1), 'unknown', 'none']);
  assert.deepEqual(overlapping(g, jst(1, 4, 10, 30), jst(1, 4, 12, 30)).map((x) => x.cause), ['unknown', 'bug_format']);
  // 欠けは 10:01〜10:59: end = 10:01 なら当たらない、start = 10:59 なら当たる、start = 11:00 なら当たらない
  assert.deepEqual(overlapping(g, jst(1, 4, 9), jst(1, 4, 10, 1)), []);
  assert.equal(overlapping(g, jst(1, 4, 9), jst(1, 4, 10, 2)).length, 1);
  assert.equal(overlapping(g, jst(1, 4, 10, 59), jst(1, 4, 11, 30)).length, 1);
  assert.deepEqual(overlapping(g, jst(1, 4, 11), jst(1, 4, 11, 30)), []);
  assert.deepEqual(overlapping(g, jst(1, 4, 14), jst(1, 4, 16)), []);  // 分の時刻が無い区間
  assert.deepEqual(overlapping([], 0, 1e10), []);
});

test('legendText: 種類・状態の文・期間', () => {
  const [f1, c3, u, n, x] = gaps([jst(1, 4, 10), jst(1, 4, 11), 'bug_format', 'filled'],
    [jst(1, 4, 11), jst(1, 4, 12), 'bug_comma', 'partial'], [jst(1, 4, 12), jst(1, 4, 13), 'unknown', 'none'],
    [jst(1, 4, 13), jst(1, 4, 14), 'no_change', 'filled'], [jst(1, 4, 14), jst(1, 4, 15), 'other', 'none']);
  assert.equal(legendText([f1], 'trade'), '価格の表示形式の変更による欠け（別に購入した約定データから作り直した区間・気配なし）2001-01-04 10:00〜2001-01-04 11:00');
  assert.equal(legendText([c3], 'trade'), '大きい前日比による欠け（別に購入した約定データから作り直した区間・気配なし・一部欠け）2001-01-04 11:00〜2001-01-04 12:00');
  assert.equal(legendText([f1], 'quote'), '価格の表示形式の変更による欠け（気配なし・別に購入した約定データから作り直した区間）2001-01-04 10:00〜2001-01-04 11:00');
  assert.equal(legendText([c3], 'quote'), '大きい前日比による欠け（気配なし・別に購入した約定データから作り直した区間）2001-01-04 11:00〜2001-01-04 12:00');
  assert.equal(legendText([u], 'trade'), '停止・取得の失敗（欠け）2001-01-04 12:00〜2001-01-04 13:00');
  assert.equal(legendText([n], 'trade'), '値の変化なし（別に購入した約定データから作り直した区間・気配なし）2001-01-04 13:00〜2001-01-04 14:00');
  assert.equal(legendText([x], 'quote'), '不明（欠け）2001-01-04 14:00〜2001-01-04 15:00');
  assert.equal(legendText([u, f1, c3], 'quote'), '区間 3 件: 停止・取得の失敗（欠け）2001-01-04 12:00〜2001-01-04 13:00');
  assert.equal(legendText([], 'trade'), '');
  // 画面に出る文に Issue 番号を出さない（#127）
  for (const g of [f1, c3, u, n, x]) {
    for (const mode of ['trade', 'quote']) {
      assert.ok(!/#[0-9]/.test(legendText([g], mode)), legendText([g], mode));
    }
  }
});
