// ATM IV のローソク足（#68・#77。TradingView Lightweight Charts 5）。足（1分・5分・15分・1時間・日足）と限月を選ぶ。
// 期間の選択は無く、読み込む範囲はチャートの表示範囲で決まる（#77）:
// - 最初は /atm_iv_bars?tf=&n=（start・end なし）でサーバーの既定の範囲（分足は最新のスナップショットの前日の UTC の 0 時〜、
//   1 時間足は 10 日、日足は 100 日）を読み、右端（最新）から足に応じた本数を見せる
// - スクロール・拡縮で表示範囲が読み込んだ範囲の端に近づいたら、その外側を 1 回分（CHUNK_DAYS）だけ追加で読む
//   （/atm_iv_bars?tf=&n=&start=&end=。UTC の UNIX 秒。応答のヘッダ X-Bars-Start・X-Bars-End が読んだ範囲）。
//   連続するイベントはまとめ（DEBOUNCE_MS）、同時に読むのは 1 本だけ。足・限月ごとに読んだ範囲と足を覚え、同じ範囲は取りに行かない
// - 取りすぎ防止: 1 回に読む日数・足ごとに覚える日数（MAX_DAYS）・分足をこの画面で読む日数の合計（MINUTE_BUDGET_DAYS）に上限。
//   データの無い範囲が続いたら（EMPTY_LIMIT 回）それより前は無いとみなす
// - 足・限月を切り替えても表示していた時間の範囲を保つ（その範囲を読み直す。分足は 1 回の上限まで）
// - 5分・15分足は 1 分足を読んでこの画面で束ねる（1 分・5 分・15 分の切り替えで BigQuery を読み直さない）
// - IV は mid（気配の中値。&iv=mid。既定。分足は #93、1 時間足・日足は 1 時間足 atm_mid_iv から #98）か約定。どの足でも選べる
// - サーバーの 1 日のクエリ量の上限（#95）に達すると 429 が返る。そのときは上限に達したことを出す
// - 限月の選択肢は static/atm_series.js の系列の表（AtmSeries.SERIES）から作る。「期近つなぎ」（n=roll。#101）は
//   取引日ごとに期近の最終取引日の 5 営業日前で 2 限月へ乗り換えてつないだ足で、乗り換えた足に印（新しい限月）を付け、
//   凡例にカーソルの足の限月を出す。「30日固定」（n=cm。#100）は満期 30 日に補間した IV で、IV の選択は使わない（選べなくする）。
//   CSV・束ね・つなぎ・印の純粋関数は atm_series.js
// - 表示はローソク足か折れ線（終値を結ぶ）。切り替えは描き直すだけで読み直さない
// - 足が 0 本の間はスクロール・拡縮を止め、選び直し・移動のたびに縦の範囲を自動に戻す
// - 足が 0 本なら、チャートの真ん中に理由と、表示できる選び方のボタン（期近つなぎ / 約定。AtmSeries.emptyHint）を出す。
//   0・1 は今日から数えた限月なので、過去の日付へ移動すると 0 本になる
// - 日付の欄で、選んだ時点で（他の選択と同じく。ボタンは無い）選んでいる足のままその日（JST）のまわりへ移動する（AtmSeries.jumpView の範囲を select で読み直す。そこから
//   左右へスクロールすると続きを読む。1 時間足の 400 日・分足の 31 日の上限はそのまま）
// - 配色は theme.js（既定はダーク）。チャートの色は CHART_THEME から選び、切り替えたら applyOptions で塗り直す
// - 過去データの欠け（#123）: ページを開いたときに /data_gaps を 1 回だけ読み（足・限月・IV の切り替え・スクロールでは読まない）、
//   描いている足ごとに、約定で #114 が作った区間は帯（右の値の軸を変えない別の価格軸のヒストグラム。ローソク足の後ろ）、
//   それ以外は欠けの印（■。乗り換えの印と 1 つの配列）を付け、凡例にカーソルの足の区間を出す。足が 0 本の説明にも区間を足す。
//   区間の時刻はこのファイルに持たない（表 data_gaps の値）。読み込みに失敗・空なら帯・印を出さずに今どおり描く。
//   足と区間の対応・文は data_gaps.js の DataGaps
// 読み込み中はチャートを薄くしてスピナー（最初・切り替え）か、端にバッジ（追加の読み込み）を出す。失敗したら「再試行」を出す。
// 時刻は UTC の UNIX 秒で持ち、チャートには 9 時間を足して渡す（ライブラリは UTC として表示するので、目盛り・ラベルが JST になる。
// 日本に夏時間は無い）。足は時刻の並び（データのある足だけ）で描かれるので、取引の無い時間・週末・休日は詰まる。
// 日足の time は取引日の 00:00 JST（サーバーの日足のまま。束ねない。#124）なので、日付の表記が取引日になる。
var JST_OFFSET = 9 * 3600;
var DAY = 86400;
// 足 → src: 読む足（API の tf）、seconds: 足の長さ、chunkDays: 1 回に読む日数（サーバーの settings.BARS_RANGE の 1 回の最大以下）、
// maxDays: 足・限月ごとに覚える日数の上限（null は上限なし。BARS_EPOCH まで）、floorDays: 遡れる日数（サーバーの遡れる上限の内側）、
// initialBars: 最初に見せる右端の本数
var TF = {
  '1m': { src: '1m', seconds: 60, chunkDays: 3, maxDays: 9, floorDays: 30, initialBars: 240 },
  '5m': { src: '1m', seconds: 300, chunkDays: 3, maxDays: 9, floorDays: 30, initialBars: 150 },
  '15m': { src: '1m', seconds: 900, chunkDays: 3, maxDays: 9, floorDays: 30, initialBars: 120 },
  '1h': { src: '1h', seconds: 3600, chunkDays: 60, maxDays: 400, floorDays: null, initialBars: 120 },
  '1d': { src: '1d', seconds: DAY, chunkDays: 365, maxDays: null, floorDays: null, initialBars: 100 },
};
var EPOCH = Date.UTC(2010, 0, 1) / 1000;  // サーバーの BARS_EPOCH
var EMPTY_LIMIT = 3;  // データの無い 1 回分がこの回数続いたら、それより前は無いとみなす
var MINUTE_BUDGET_DAYS = 24;  // 分足をこの画面で読む日数の合計の上限（1 日 約 90 MiB のスキャン）
var EDGE_BARS = 10;  // 表示範囲の端と読み込んだ範囲の端の距離（本）がこれ未満なら追加で読む
var DEBOUNCE_MS = 250;
var RIGHT_OFFSET = 3;  // 最新の足の右の余白（本）
// テーマごとのチャートの色（背景・文字・目盛りの線・軸の枠）と折れ線・乗り換えの印の色（チャートの canvas に描く）。
// ライトは Lightweight Charts の既定に近い値、ダークは viewer.css の --bg に合わせる
// band・gap は過去データの欠け（#123）の帯（半透明）・印
var CHART_THEME = {
  dark: { bg: '#12141c', text: '#c3c8d4', grid: '#222634', border: '#2c3040', line: '#5c8dff', marker: '#5c8dff',
    band: 'rgba(240, 180, 60, 0.16)', gap: '#f0a830' },
  light: { bg: '#ffffff', text: '#191919', grid: '#eef0f4', border: '#d6dcde', line: '#2962ff', marker: '#2962ff',
    band: 'rgba(240, 160, 0, 0.14)', gap: '#d98a00' },
};

var chart = null;
var series = null;
var seriesStyle = null;  // 'candle' / 'line'（series の種類）
var markers = null;  // 乗り換えの印・欠けの印（createSeriesMarkers）
var monthAt = new Map();  // チャートの time → その足の限月（期近つなぎのときだけ）
var gaps = [];  // 過去データの欠けの区間（#123。DataGaps.parse の値。読めなければ空）
var band = null;  // 帯（#114 で作った区間の足の背景）のヒストグラム
var gapAt = new Map();  // チャートの time → その足の区間の説明（凡例）
var barAt = new Map();  // チャートの time → 足（凡例。折れ線の点は OHLC を持たないので、ここから引く）
var shown = [];  // 描いている足（UTC 秒の time）
var currentTf = null;
var shownTf = '1h';  // 描いている足（読み込み中は currentTf と違う）
var currentN = null;
var shownN = null;
var currentIv = 'mid';
var shownIv = 'mid';
var entries = {};  // "src|<系列の value>|iv" → { bars, start, end, leftDone, rightDone, empty, note }
var inflight = null;  // { ctrl, gen, mode }
var gen = 0;  // 足・限月を選び直すたびに増やす（古い応答を捨てる）
var retry = null;  // 失敗した読み込みをやり直す関数
var minuteDays = 0;  // この画面で分足を読んだ日数の合計
var debounceTimer = null;

function el(id) {
  return document.getElementById(id);
}

function pad(x) {
  return (x < 10 ? '0' : '') + x;
}

function utcDay(t) {
  return Math.floor(t / DAY) * DAY;
}

function nowSec() {
  return Math.floor(Date.now() / 1000);
}

// time（チャートの時刻 = UTC 秒 + 9 時間）→ "YYYY-MM-DD HH:MM"（日足は日付だけ）
function formatTime(time) {
  var d = new Date(time * 1000);
  var date = d.getUTCFullYear() + '-' + pad(d.getUTCMonth() + 1) + '-' + pad(d.getUTCDate());
  if (shownTf === '1d') {
    return date;
  }
  return date + ' ' + pad(d.getUTCHours()) + ':' + pad(d.getUTCMinutes());
}

function showLegend(bar) {
  if (!bar) {
    el('legend').textContent = '';
    return;
  }
  var month = monthAt.get(bar.time);
  var kind = AtmSeries.seriesOf(shownN).fixedIv ? '30日  ' : (shownIv === 'mid' ? 'mid  ' : '');
  var gap = gapAt.get(bar.time);
  el('legend').textContent = kind + formatTime(bar.time) + '  O ' + bar.open.toFixed(2) + '  H ' + bar.high.toFixed(2) +
    '  L ' + bar.low.toFixed(2) + '  C ' + bar.close.toFixed(2) + (month ? '  ' + AtmSeries.monthLegend(month) : '') +
    (gap ? '  ' + gap : '');
}

function toChart(bar) {
  return { time: bar.time + JST_OFFSET, open: bar.open, high: bar.high, low: bar.low, close: bar.close };
}

function toSeries(bar) {
  return seriesStyle === 'line' ? { time: bar.time + JST_OFFSET, value: bar.close } : toChart(bar);
}

function entryKey(tf, n, iv) {
  return TF[tf].src + '|' + n + '|' + iv;
}

function currentEntry() {
  return entries[entryKey(currentTf, currentN, currentIv)];
}

// 読み込みの失敗の表示。429 はサーバーの 1 日の上限（#95）
function failText(reason) {
  if (reason === 'HTTP 429') {
    return '今日の読み込みの上限に達しました（日本時間の 0 時に戻ります）';
  }
  return 'データを取得できませんでした (' + reason + ')';
}

// ---- 読み込み中・エラーの表示 ----

function setBusy(mode) {
  // mode: 'main'（チャートを薄くしてスピナー）、'left' / 'right'（端のバッジ）、null（消す）
  el('chart-wrap').setAttribute('aria-busy', mode ? 'true' : 'false');
  el('chart-busy').hidden = mode !== 'main';
  var edge = el('chart-edge');
  edge.hidden = mode !== 'left' && mode !== 'right';
  edge.classList.toggle('chart-edge-right', mode === 'right');
  el('chart-edge-text').textContent = mode === 'right' ? '後のデータを読み込み中…' : '前のデータを読み込み中…';
}

function showError(message, retryFn, dim) {
  retry = retryFn;
  el('chart-error-text').textContent = message;
  el('chart-error').hidden = false;
  el('chart-error').classList.toggle('chart-error-dim', !!dim);
}

function clearError() {
  retry = null;
  el('chart-error').hidden = true;
}

function note(text) {
  el('message').textContent = text || '';
}

var EMPTY_ACTIONS = {
  roll: { label: '期近つなぎで表示', id: 'nmonth', value: 'roll' },
  trade: { label: '約定で表示', id: 'ivkind', value: 'trade' },
};
var emptyAction = null;
var lastView = null;  // 最後に select で読みに行った時間の範囲（足が 0 本で表示範囲が取れないときに選び直しで使う）

// 足が 0 本のときの説明と、選び直すボタン（チャートの真ん中に出す）
function noteEmpty() {
  var entry = currentEntry();
  var hint = AtmSeries.emptyHint(AtmSeries.seriesOf(currentN), currentIv,
    entry ? DataGaps.overlapping(gaps, entry.start, entry.end) : []);
  note('');
  emptyAction = hint.action ? EMPTY_ACTIONS[hint.action] : null;
  el('chart-empty-text').textContent = hint.text;
  el('empty-action').hidden = !emptyAction;
  if (emptyAction) {
    el('empty-action').textContent = emptyAction.label;
  }
  el('chart-empty').hidden = false;
}

// ---- 描画 ----

function displayBars(entry) {
  return entry ? AtmSeries.displayBars(entry.bars, TF[currentTf].seconds) : [];
}

// 期近つなぎの乗り換えの印と凡例の限月、過去データの欠け（#123）の帯・印と凡例の区間。描く足の全体（左・右に読み足して
// つないだ後）から作り直す
function renderMarks() {
  var t = chartTheme();
  monthAt = new Map();
  gapAt = new Map();
  var roll = [];
  if (AtmSeries.seriesOf(shownN).months) {
    shown.forEach(function(b) {
      if (b.month !== undefined) {
        monthAt.set(b.time + JST_OFFSET, b.month);
      }
    });
    roll = AtmSeries.rollMarkers(shown).map(function(m) {
      return { time: m.time + JST_OFFSET, position: 'aboveBar', shape: 'arrowDown', color: t.marker, text: m.text };
    });
  }
  var mode = DataGaps.modeOf(AtmSeries.seriesOf(shownN), shownIv);
  var r = DataGaps.assign(shown, TF[shownTf].seconds, gaps, mode);
  // 乗り換えの印（矢印・文字あり）と見分けられるよう、欠けの印は足の下の ■（文字なし）
  var marks = r.marks.map(function(m) {
    return { time: m.time + JST_OFFSET, position: 'belowBar', shape: 'square', color: t.gap };
  });
  markers.setMarkers(DataGaps.mergeMarkers(roll, marks));
  band.setData(r.bands.map(function(b) { return { time: b.time + JST_OFFSET, value: 1 }; }));
  r.bands.concat(r.marks).forEach(function(x) {
    gapAt.set(x.time + JST_OFFSET, DataGaps.legendText(x.gaps, mode));
  });
}

// 表示している時間の範囲（UTC 秒）。atRight は最新の足が見えているか
function visibleTimeRange() {
  if (!shown.length) {
    return null;
  }
  var lr = chart.timeScale().getVisibleLogicalRange();
  if (!lr) {
    return null;
  }
  var last = shown.length - 1;
  var from = shown[Math.min(last, Math.max(0, Math.ceil(lr.from)))].time;
  var to = shown[Math.min(last, Math.max(0, Math.floor(lr.to)))].time + TF[shownTf].seconds;
  var entry = entries[entryKey(shownTf, shownN, shownIv)];
  return { from: from, to: to, atRight: lr.to >= last && !!(entry && entry.rightDone) };
}

// 時間の範囲 view（null なら右端から initialBars 本）を見せる
function applyView(view) {
  var ts = chart.timeScale();
  if (!shown.length) {
    return;
  }
  var last = shown.length - 1;
  if (!view) {
    var count = TF[currentTf].initialBars;
    if (shown.length > count) {
      ts.setVisibleLogicalRange({ from: shown.length - count, to: last + RIGHT_OFFSET });
    } else {
      ts.fitContent();
    }
    return;
  }
  var from = 0;
  while (from < last && shown[from].time + TF[currentTf].seconds <= view.from) {
    from++;
  }
  var to = last;
  while (to > from && shown[to].time >= view.to) {
    to--;
  }
  var right = view.atRight ? last + RIGHT_OFFSET : to;
  if (right - from < 20) {
    from = Math.max(0, right - 20);
  }
  ts.setVisibleLogicalRange({ from: from - 0.5, to: right + 0.5 });
}

// 足を描き直す。keep: 表示している足を同じ位置に保つ（前に足を足したときはその本数だけずらす）
function render(view, keep) {
  var lr = keep ? chart.timeScale().getVisibleLogicalRange() : null;
  var oldFirst = shown.length ? shown[0].time : null;
  var entry = currentEntry();
  shown = displayBars(entry);
  shownTf = currentTf;
  shownN = currentN;
  shownIv = currentIv;
  // 足が 0 本の間はスクロール・拡縮を止める（軸をドラッグすると縦の自動の範囲が切れ、足を読んだ後も潰れたままになる）
  var has = shown.length > 0;
  chart.applyOptions({ timeScale: { timeVisible: currentTf !== '1d' }, handleScroll: has, handleScale: has });
  if (!keep) {
    // 選び直し・移動のたびに縦の範囲を自動に戻す（前の表示で軸を動かしていても、新しい足に合わせる）
    series.priceScale().applyOptions({ autoScale: true });
  }
  barAt = new Map(shown.map(function(b) { return [b.time + JST_OFFSET, toChart(b)]; }));
  series.setData(shown.map(toSeries));
  renderMarks();
  if (lr && oldFirst !== null) {
    var shift = 0;
    while (shift < shown.length && shown[shift].time < oldFirst) {
      shift++;
    }
    chart.timeScale().setVisibleLogicalRange({ from: lr.from + shift, to: lr.to + shift });
  } else {
    applyView(view);
  }
  showLegend(shown.length ? toChart(shown[shown.length - 1]) : undefined);
  el('chart-empty').hidden = true;
  if (!entry) {
    note('');
  } else if (!shown.length && !entry.note) {
    // 前を読み続けている間（データの無い週末・休日）も出す（読み終えて見つかれば描き直しで消える）
    noteEmpty();
  } else {
    note(entry.note);
  }
}

// ---- 読み込み ----

// /atm_iv_bars を読む。成功で onData(bars, start, end)、失敗で onFail(message)。中断（新しい選択）なら何もしない
function request(mode, src, n, iv, start, end, onData, onFail) {
  if (inflight) {
    inflight.ctrl.abort();
  }
  var ctrl = new AbortController();
  var mine = { ctrl: ctrl, gen: gen, mode: mode };
  inflight = mine;
  clearError();
  setBusy(mode);
  if (src === '1m' && start !== null) {
    minuteDays += (end - start) / DAY;
  }
  fetch(AtmSeries.barsUrl(AtmSeries.seriesOf(n), src, iv, start, end), { signal: ctrl.signal, cache: 'no-store', credentials: 'same-origin' })
    .then(function(res) {
      if (!res.ok) {
        throw new Error('HTTP ' + res.status);
      }
      return res.text().then(function(text) {
        return {
          bars: AtmSeries.parseCsv(text),
          start: Number(res.headers.get('X-Bars-Start')),
          end: Number(res.headers.get('X-Bars-End')),
        };
      });
    })
    .then(function(data) {
      if (inflight !== mine) {
        return;
      }
      inflight = null;
      setBusy(null);
      if (src === '1m' && start === null) {
        minuteDays += (data.end - data.start) / DAY;  // 既定の範囲（サーバーが決める）
      }
      onData(data.bars, data.start, data.end);
    })
    .catch(function(err) {
      if (inflight !== mine) {
        return;  // 中断された（後から選び直した分を描く）
      }
      inflight = null;
      setBusy(null);
      onFail(err && err.message && err.message.indexOf('HTTP ') === 0 ? err.message : 'network');
    });
}

// 足・限月・IV を選んだとき・最初: view（保つ時間の範囲）を読み込み済みなら描くだけ、無ければ読む。
function select(view) {
  gen++;
  lastView = view;
  if (inflight) {
    inflight.ctrl.abort();
    inflight = null;
  }
  setBusy(null);
  clearError();
  var tf = el('tf').value;
  var n = el('nmonth').value;
  // 30日固定は IV を選べない（読み込み済みの足を覚えるキーは mid に揃える）
  var fixedIv = !!AtmSeries.seriesOf(n).fixedIv;
  el('ivkind').disabled = fixedIv;
  var iv = fixedIv ? 'mid' : el('ivkind').value;
  var conf = TF[tf];
  var key = entryKey(tf, n, iv);
  var entry = entries[key];
  var start = null;
  var end = null;
  var msg = '';
  if (view) {
    // 表示していた範囲の右端から 1 回分まで（分足は遡れる範囲の中）
    end = view.atRight ? utcDay(nowSec()) + DAY : view.to;
    start = utcDay(Math.max(view.from, end - conf.chunkDays * DAY));
    if (end - start > conf.chunkDays * DAY) {
      start += DAY;
    }
    var floor = conf.floorDays === null ? EPOCH : utcDay(nowSec()) - conf.floorDays * DAY;
    if (start < floor) {
      start = floor;
    }
    if (end <= start) {
      // 遡れる範囲より前を見ていた（分足）。既定の範囲（最新）を見せる
      msg = '分足は直近 ' + conf.floorDays + ' 日分まで表示できます';
      view = null;
      start = null;
      end = null;
    }
  }
  currentTf = tf;
  currentN = n;
  currentIv = iv;
  if (entry && (start === null ? entry.rightDone :
    entry.start <= start && (entry.end >= Math.min(end, nowSec() - 60) || entry.rightDone))) {
    render(view, false);
    if (msg) {
      note(msg);
    }
    scheduleCheck();
    return;
  }
  if (conf.src === '1m' && start !== null && minuteDays + (end - start) / DAY > MINUTE_BUDGET_DAYS) {
    shown = [];
    series.setData([]);
    renderMarks();
    showLegend(undefined);
    el('chart-empty').hidden = true;
    note('分足の読み込みの上限（この画面で ' + MINUTE_BUDGET_DAYS + ' 日分）に達しました。ページを読み直してください');
    return;
  }
  var myGen = gen;
  // 読んでいる間は前の足を薄く見せたまま（失敗したら消す）
  request('main', conf.src, n, iv, start, end, function(bars, s, e) {
    // 選んだ範囲（日付へ移動・選び直し）に足が 0 本なら、前を探しに行かない（遠くの足が出て、移動した日付から外れるため）。
    // 理由と選び直しのボタン（noteEmpty）を出して止まる
    entries[key] = {
      bars: bars, start: s, end: e, leftDone: s <= EPOCH || (view !== null && !bars.length),
      rightDone: start === null || e < end,
      empty: bars.length ? 0 : 1, note: '',
    };
    render(view, false);
    if (msg) {
      note(msg);
    }
    // ライブラリは表示範囲の変更を次の描画（requestAnimationFrame）で反映するので、すぐには端を調べない
    scheduleCheck();
  }, function(reason) {
    if (myGen !== gen) {
      return;
    }
    shown = [];
    series.setData([]);
    renderMarks();
    showLegend(undefined);
    el('chart-empty').hidden = true;
    note('');
    showError(failText(reason), function() { select(view); }, true);
  });
}

// 表示範囲が読み込んだ範囲の端に近ければ、その外側を 1 回分読む
function checkEdges() {
  var entry = currentEntry();
  if (!entry || inflight || retry) {
    return;
  }
  var lr = shown.length ? chart.timeScale().getVisibleLogicalRange() : null;
  if (shown.length && !lr) {
    return;
  }
  var needLeft = !entry.leftDone && (!shown.length || lr.from < EDGE_BARS);
  var needRight = !entry.rightDone && shown.length > 0 && lr.to > shown.length - 1 - EDGE_BARS;
  if (needLeft) {
    loadMore(entry, 'left');
  } else if (needRight) {
    loadMore(entry, 'right');
  }
}

function scheduleCheck(ms) {
  clearTimeout(debounceTimer);
  debounceTimer = setTimeout(checkEdges, ms === undefined ? DEBOUNCE_MS : ms);
}

function stop(entry, side, text) {
  if (side === 'left') {
    entry.leftDone = true;
  } else {
    entry.rightDone = true;
  }
  entry.note = text;
  note(text);
}

function loadMore(entry, side) {
  var conf = TF[currentTf];
  var chunk = conf.chunkDays * DAY;
  var start;
  var end;
  if (side === 'left') {
    end = entry.start;
    start = utcDay(end - chunk);
    if (end - start > chunk) {
      start += DAY;
    }
    // 遡れる範囲（分足）と、この足で覚える日数の上限
    var limit = conf.floorDays === null ? EPOCH : utcDay(nowSec()) - conf.floorDays * DAY;
    var reason = conf.floorDays === null ? '' : '分足は直近 ' + conf.floorDays + ' 日分まで表示できます';
    if (conf.maxDays !== null && entry.end - conf.maxDays * DAY > limit) {
      limit = entry.end - conf.maxDays * DAY;
      reason = 'この足で読み込むのは ' + conf.maxDays + ' 日分までです（もっと前は 1 時間足・日足で）';
    }
    start = Math.max(start, limit);
    if (start >= end) {
      stop(entry, side, reason);
      return;
    }
  } else {
    start = entry.end;
    // 今日の終わりまで（サーバーは now を超えた分を now に縮める）
    end = Math.min(start + chunk, utcDay(nowSec()) + DAY);
    if (conf.maxDays !== null && end - entry.start > conf.maxDays * DAY) {
      stop(entry, side, 'この足で読み込むのは ' + conf.maxDays + ' 日分までです（足・限月を選び直すと最新から読み直します）');
      return;
    }
  }
  if (conf.src === '1m' && minuteDays + (end - start) / DAY > MINUTE_BUDGET_DAYS) {
    stop(entry, side, '分足の読み込みの上限（この画面で ' + MINUTE_BUDGET_DAYS + ' 日分）に達しました');
    return;
  }
  var key = entryKey(currentTf, currentN, currentIv);
  request(side, conf.src, currentN, currentIv, start, end, function(bars, s, e) {
    if (entries[key] !== entry) {
      return;
    }
    entry.bars = AtmSeries.mergeBars(entry.bars, bars);
    if (side === 'left') {
      entry.start = Math.min(entry.start, s);
      entry.empty = bars.length ? 0 : entry.empty + 1;
      if (entry.start <= EPOCH || entry.empty >= EMPTY_LIMIT) {
        entry.leftDone = true;
      }
    } else {
      entry.end = Math.max(entry.end, e);
      if (e < end) {
        entry.rightDone = true;  // サーバーが now に縮めた = 最新まで読んだ
      }
    }
    if (bars.length) {
      render(null, true);
    }
    // データの無い範囲（週末・休日）なら続けて次を読む（EMPTY_LIMIT 回まで）
    scheduleCheck(bars.length ? DEBOUNCE_MS : 0);
  }, function(reason) {
    showError(failText(reason), function() {
      clearError();
      checkEdges();
    }, false);
  });
}

function chartTheme() {
  return CHART_THEME[ViewerTheme.get()];
}

// テーマの色の options（createChart と切り替えの applyOptions で使う）
function themeOptions() {
  var t = chartTheme();
  return {
    layout: { background: { type: 'solid', color: t.bg }, textColor: t.text },
    grid: { vertLines: { color: t.grid }, horzLines: { color: t.grid } },
    rightPriceScale: { borderColor: t.border },
    timeScale: { borderColor: t.border },
  };
}

function createChart() {
  var opts = themeOptions();
  // attributionLogo は <style> 要素を足すので CSP（style-src 'self'）に反する。表記はページの下に置く
  opts.layout.attributionLogo = false;
  opts.autoSize = true;
  opts.localization = { locale: 'ja-JP', timeFormatter: formatTime };
  Object.assign(opts.timeScale, { timeVisible: true, secondsVisible: false, rightOffset: RIGHT_OFFSET });
  chart = LightweightCharts.createChart(el('chart'), opts);
  // 帯（#123）: 別の価格軸（表示しない・0〜1 に固定・上下の余白なし）のヒストグラムで足の背景を全高で塗る。右の値の軸の範囲・
  // クロスヘアはローソク足だけで決まる。setStyle で作り直す series より先に足すので、常にその後ろに描かれる
  band = chart.addSeries(LightweightCharts.HistogramSeries, {
    priceScaleId: 'gap-band', color: chartTheme().band, base: 0, lastValueVisible: false, priceLineVisible: false,
    crosshairMarkerVisible: false,
    autoscaleInfoProvider: function() { return { priceRange: { minValue: 0, maxValue: 1 } }; },
  });
  chart.priceScale('gap-band').applyOptions({ visible: false, scaleMargins: { top: 0, bottom: 0 } });
  setStyle(el('style').value);
  chart.subscribeCrosshairMove(function(param) {
    var bar = param.time !== undefined ? barAt.get(param.time) : null;
    showLegend(bar || (shown.length ? toChart(shown[shown.length - 1]) : undefined));
  });
  chart.timeScale().subscribeVisibleLogicalRangeChange(function() {
    scheduleCheck();
  });
}

// 表示（ローソク足 / 折れ線）の series を作り直す。印も付け直し、描いている足を描き直す（表示範囲は時刻の軸なので保たれる）
function setStyle(style) {
  if (series) {
    chart.removeSeries(series);
  }
  seriesStyle = style === 'line' ? 'line' : 'candle';
  var opts = { priceFormat: { type: 'price', precision: 2, minMove: 0.01 } };
  series = seriesStyle === 'line' ?
    chart.addSeries(LightweightCharts.LineSeries, Object.assign({ lineWidth: 2, color: chartTheme().line }, opts)) :
    chart.addSeries(LightweightCharts.CandlestickSeries, opts);
  markers = LightweightCharts.createSeriesMarkers(series, []);
  series.setData(shown.map(toSeries));
  renderMarks();
}

// ライセンス（Apache-2.0）の帰属表記のリンク。テンプレートに外部の URL を書かないため、ここで設定する
el('attribution-link').href = 'https://www.tradingview.com/';

// 限月の選択肢を系列の表から作る（選択肢の定義は atm_series.js の SERIES だけ）
function fillSeriesOptions() {
  var select = el('nmonth');
  AtmSeries.SERIES.forEach(function(s) {
    var o = document.createElement('option');
    o.value = s.value;
    o.textContent = s.label;
    select.appendChild(o);
  });
  select.value = AtmSeries.DEFAULT_SERIES;
}

// 足が 0 本（日付へ移動した先に足が無かった）なら、移動した範囲のまま選び直す
function onSelect() {
  select(visibleTimeRange() || lastView);
}

el('tf').addEventListener('change', onSelect);
el('nmonth').addEventListener('change', onSelect);
el('ivkind').addEventListener('change', onSelect);
el('style').addEventListener('change', function() {
  setStyle(el('style').value);
});
ViewerTheme.onChange(function() {
  chart.applyOptions(themeOptions());
  if (seriesStyle === 'line') {
    series.applyOptions({ color: chartTheme().line });
  }
  band.applyOptions({ color: chartTheme().band });
  renderMarks();  // 印・帯の色
});
function jump() {
  var view = AtmSeries.jumpView(el('jump-date').value, el('tf').value, nowSec());
  if (!view) {
    note('日付を 2018-10-19〜今日の範囲で入れてください');
    return;
  }
  select(view);
}

// 日付の欄は選んだ時点で移動する。キーボードで打つと年の桁ごとに change が来る（0002 → 0020 → 0202 → 2020）ので、
// 少し待って最後の値だけで移動する（Enter はすぐ）
var JUMP_DEBOUNCE_MS = 600;
var jumpTimer = null;
el('jump-date').addEventListener('change', function() {
  clearTimeout(jumpTimer);
  jumpTimer = setTimeout(jump, JUMP_DEBOUNCE_MS);
});
el('jump-date').addEventListener('keydown', function(e) {
  if (e.key === 'Enter') {
    clearTimeout(jumpTimer);
    jump();
  }
});
// 選べる日付の上限は今日（JST）
el('jump-date').max = new Date((nowSec() + JST_OFFSET) * 1000).toISOString().slice(0, 10);
// 選び直して、見ていた時間の範囲を読み直す（日付へ移動した直後なら、その日のまわり）
el('empty-action').addEventListener('click', function() {
  if (!emptyAction) {
    return;
  }
  el(emptyAction.id).value = emptyAction.value;
  el('chart-empty').hidden = true;
  select(visibleTimeRange() || lastView);
});
el('chart-retry').addEventListener('click', function() {
  if (retry) {
    retry();
  }
});

// 過去データの欠けの区間（#123）をページを開いたときに 1 回だけ読む。失敗（HTTP エラー・上限・ネットワーク）・空なら
// 帯・印を出さない（エラーにしない）
function loadGaps() {
  fetch('/data_gaps', { cache: 'no-store', credentials: 'same-origin' })
    .then(function(res) {
      return res.ok ? res.text() : '';
    })
    .then(function(text) {
      gaps = DataGaps.parse(text);
      if (!gaps.length) {
        return;
      }
      el('gap-help').hidden = false;
      renderMarks();
      showLegend(shown.length ? toChart(shown[shown.length - 1]) : undefined);
      if (!el('chart-empty').hidden) {
        noteEmpty();  // 足が 0 本の説明に区間を足す
      }
    })
    .catch(function() {});
}

fillSeriesOptions();
createChart();
select(null);
loadGaps();
