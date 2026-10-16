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
// - IV は約定（既定）か mid（気配の中値。&iv=mid。#93）。mid は分足だけ（1 時間足・日足の集計に mid が無い）なので、
//   mid のときは 1 時間・日足を選べなくし、1 時間・日足を見ているときに mid を選ぶと 15 分足に切り替える
// - サーバーの 1 日のクエリ量の上限（#95）に達すると 429 が返る。そのときは上限に達したことを出す
// 読み込み中はチャートを薄くしてスピナー（最初・切り替え）か、端にバッジ（追加の読み込み）を出す。失敗したら「再試行」を出す。
// 時刻は UTC の UNIX 秒で持ち、チャートには 9 時間を足して渡す（ライブラリは UTC として表示するので、目盛り・ラベルが JST になる。
// 日本に夏時間は無い）。足は時刻の並び（データのある足だけ）で描かれるので、取引の無い時間・週末・休日は詰まる。
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

var chart = null;
var series = null;
var shown = [];  // 描いている足（UTC 秒の time）
var currentTf = null;
var shownTf = '1h';  // 描いている足（読み込み中は currentTf と違う）
var currentN = null;
var shownN = null;
var currentIv = 'trade';
var shownIv = 'trade';
var entries = {};  // "src|n|iv" → { bars, start, end, leftDone, rightDone, empty, note }
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
  el('legend').textContent = (shownIv === 'mid' ? 'mid  ' : '') + formatTime(bar.time) + '  O ' + bar.open.toFixed(2) + '  H ' + bar.high.toFixed(2) +
    '  L ' + bar.low.toFixed(2) + '  C ' + bar.close.toFixed(2);
}

function toChart(bar) {
  return { time: bar.time + JST_OFFSET, open: bar.open, high: bar.high, low: bar.low, close: bar.close };
}

function parseCsv(text) {
  var out = [];
  var lines = text.split('\n');
  for (var i = 0; i < lines.length; i++) {
    var cells = lines[i].split(',');
    if (cells.length < 5 || cells[1] === '') {
      continue;
    }
    out.push({
      // started_at は "YYYY-MM-DD HH:MM:SS+00:00"。ISO 8601 の形にしてから読む
      time: Date.parse(cells[4].replace(' ', 'T')) / 1000,
      open: Number(cells[0]),
      high: Number(cells[1]),
      low: Number(cells[2]),
      close: Number(cells[3]),
    });
  }
  return out;
}

// 時刻順の 2 つの足の列をつなぐ（同じ時刻は b を使う）
function mergeBars(a, b) {
  var map = new Map();
  a.forEach(function(x) { map.set(x.time, x); });
  b.forEach(function(x) { map.set(x.time, x); });
  return Array.from(map.values()).sort(function(x, y) { return x.time - y.time; });
}

// 1 分足を seconds 秒の足に束ねる（サーバーの bars.resample と同じ: UNIX 秒で区切る。open / close = 最初 / 最後）
function resample(bars, seconds) {
  if (seconds === 60) {
    return bars;
  }
  var out = [];
  bars.forEach(function(b) {
    var t = Math.floor(b.time / seconds) * seconds;
    var last = out[out.length - 1];
    if (last && last.time === t) {
      last.high = Math.max(last.high, b.high);
      last.low = Math.min(last.low, b.low);
      last.close = b.close;
    } else {
      out.push({ time: t, open: b.open, high: b.high, low: b.low, close: b.close });
    }
  });
  return out;
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

// ---- 描画 ----

function displayBars(entry) {
  return entry ? resample(entry.bars, TF[currentTf].seconds) : [];
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
  chart.applyOptions({ timeScale: { timeVisible: currentTf !== '1d' } });
  series.setData(shown.map(toChart));
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
  if (!entry) {
    note('');
  } else if (!shown.length && !entry.note) {
    // 前を読み続けている間（データの無い週末・休日）は出さない
    note(entry.leftDone ? 'データがありません' : '');
  } else {
    note(entry.note);
  }
}

// ---- 読み込み ----

function barsUrl(src, n, iv, start, end) {
  var url = '/atm_iv_bars?tf=' + encodeURIComponent(src) + '&n=' + encodeURIComponent(n);
  if (iv === 'mid') {
    url += '&iv=mid';
  }
  if (start !== null) {
    url += '&start=' + start + '&end=' + end;
  }
  return url;
}

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
  fetch(barsUrl(src, n, iv, start, end), { signal: ctrl.signal, cache: 'no-store', credentials: 'same-origin' })
    .then(function(res) {
      if (!res.ok) {
        throw new Error('HTTP ' + res.status);
      }
      return res.text().then(function(text) {
        return {
          bars: parseCsv(text),
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
// first: 最初に出す説明（IV の切り替えで足を変えたとき）
function select(view, first) {
  gen++;
  if (inflight) {
    inflight.ctrl.abort();
    inflight = null;
  }
  setBusy(null);
  clearError();
  var tf = el('tf').value;
  var n = el('nmonth').value;
  var iv = el('ivkind').value;
  var conf = TF[tf];
  var key = entryKey(tf, n, iv);
  var entry = entries[key];
  var start = null;
  var end = null;
  var msg = first || '';
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
    showLegend(undefined);
    note('分足の読み込みの上限（この画面で ' + MINUTE_BUDGET_DAYS + ' 日分）に達しました。ページを読み直してください');
    return;
  }
  var myGen = gen;
  // 読んでいる間は前の足を薄く見せたまま（失敗したら消す）
  request('main', conf.src, n, iv, start, end, function(bars, s, e) {
    entries[key] = {
      bars: bars, start: s, end: e, leftDone: s <= EPOCH, rightDone: start === null || e < end,
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
    showLegend(undefined);
    note('');
    showError(failText(reason), function() { select(view, first); }, true);
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
    entry.bars = mergeBars(entry.bars, bars);
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

function createChart() {
  chart = LightweightCharts.createChart(el('chart'), {
    autoSize: true,
    // attributionLogo は <style> 要素を足すので CSP（style-src 'self'）に反する。表記はページの下に置く
    layout: { attributionLogo: false },
    localization: { locale: 'ja-JP', timeFormatter: formatTime },
    timeScale: { timeVisible: true, secondsVisible: false, rightOffset: RIGHT_OFFSET },
  });
  series = chart.addSeries(LightweightCharts.CandlestickSeries, {
    priceFormat: { type: 'price', precision: 2, minMove: 0.01 },
  });
  chart.subscribeCrosshairMove(function(param) {
    var bar = param.time !== undefined ? param.seriesData.get(series) : null;
    showLegend(bar || (shown.length ? toChart(shown[shown.length - 1]) : undefined));
  });
  chart.timeScale().subscribeVisibleLogicalRangeChange(function() {
    scheduleCheck();
  });
}

// ライセンス（Apache-2.0）の帰属表記のリンク。テンプレートに外部の URL を書かないため、ここで設定する
el('attribution-link').href = 'https://www.tradingview.com/';

function onSelect() {
  select(visibleTimeRange());
}

// mid のときは 1 時間・日足を選べなくする（mid は分足だけ）
function updateTfOptions() {
  var mid = el('ivkind').value === 'mid';
  Array.prototype.forEach.call(el('tf').options, function(o) {
    o.disabled = mid && TF[o.value].seconds >= 3600;
  });
}

// mid で 1 時間・日足なら 15 分足にする（ブラウザが前の選択を戻したときも）。変えたら説明を返す
function fixTf() {
  if (el('ivkind').value === 'mid' && TF[el('tf').value].seconds >= 3600) {
    el('tf').value = '15m';
    return 'mid は分足（1分・5分・15分）だけです。15分足に切り替えました';
  }
  return '';
}

function onIvChange() {
  var first = fixTf();
  updateTfOptions();
  select(visibleTimeRange(), first);
}

el('tf').addEventListener('change', onSelect);
el('nmonth').addEventListener('change', onSelect);
el('ivkind').addEventListener('change', onIvChange);
el('chart-retry').addEventListener('click', function() {
  if (retry) {
    retry();
  }
});

createChart();
var initialNote = fixTf();
updateTfOptions();
select(null, initialNote);
