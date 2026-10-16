// スマイルカーブ（#79。Chart.js 4 の canvas）。/smile_snapshot?at=（UTC の UNIX 秒。省略で最新）で 1 時点を読んで描く。
// - 曲線の規則（OTM 合成・mid IV・IV の 0 の扱い）はサーバー（smile.py）が CSV の列（mid_iv・otm）にする。ここは列で絞るだけ
// - 横軸は行使価格（値に比例）。拡大縮小・スクロール（ホイール・ドラッグ・ピンチ・スワイプ）はこのファイルの pointer events で
//   横軸の min / max を書き換えるだけで、サーバーに要求しない。縦軸は表示範囲の点から Chart.js が決める
// - 時点の移動（最新・前後のステップ・日時の入力）は DEBOUNCE_MS でまとめてから 1 回だけ要求し、前の要求は AbortController で
//   中断する。読んだスナップショットと「at → どのスナップショットか」の区間を覚え、同じ時点ではサーバーに要求しない
// - 取りすぎ防止: この画面で出す要求は MAX_REQUESTS 回まで（1 回 約 130 MiB のスキャン。中断・再試行も数える）
// - 表示の切り替えは hidden 属性・classList・disabled だけ（CSP の style-src 'self'）
// - 約定 IV の点は約定からの経過（smile_age.js の SmileAge。#82）で段ごとに塗り・枠・大きさを変える（Chart.js の点ごとの配列）。
//   「経過時間」をオンにすると経過（分）を右軸に描く（サーバーに要求しない）
// 時刻は UTC の UNIX 秒で持ち、表示は JST（日本に夏時間は無いので 9 時間を足す）。
var JST_OFFSET = 9 * 3600;
var DAY = 86400;
var SEARCH_DAYS = 14;  // サーバーが at から遡って探す日数（settings.SEARCH_DAYS）
var DEBOUNCE_MS = 250;
var MAX_REQUESTS = 20;
var LATEST_FRESH_MS = 60 * 1000;  // 「最新」を読み直さずに使う時間（毎分のスナップショットを見られるように）
var INITIAL_WIDTH = 0.10;  // 初期の表示範囲: ATM ±10%
var MIN_WIDTH = 500;  // 横の表示幅の最小（円）
var WHEEL_FACTOR = 1.2;
var CALL = 1;
var PUT = 2;
// 線の色（viewer.css の .sw-* と同じ値）。2 限月は薄い色
var COLORS = {
  otm: { 1: '#1a237e', 2: '#7986cb' },
  call: { 1: '#c62828', 2: '#ef9a9a' },
  put: { 1: '#1565c0', 2: '#90caf9' },
  pin: { 1: '#9e9e9e', 2: '#cfcfcf' },
};
// 約定 IV の点の段ごとの見た目（SmileAge.TIERS の順。fill / border は曲線の色の不透明度、null は白）
var TIER_STYLE = [
  { fill: 1, border: 1, radius: 3.5 },
  { fill: 0.45, border: 1, radius: 3 },
  { fill: null, border: 1, radius: 2.5 },
  { fill: null, border: 0.35, radius: 2.5 },
];
var COLUMNS = ['month', 'type', 'target_price', 'iv', 'price', 'price_time', 'bid_iv', 'ask_iv', 'bid', 'ask', 'mid_iv',
  'otm'];

var chart = null;
var snapshots = {};  // スナップショットの時刻 → 解析した 1 時点
var known = [];  // { lo, hi, s }: at が [lo, hi] なら答えは s（null はデータなし）
var latest = null;  // { s, fetched }: 最後に「最新」を読んだ結果と、読んだ時刻（ms）
var shown = null;  // 表示中の 1 時点（null は未表示・データなし）
var position = null;  // 時点の移動の基準（表示中のスナップショットの時刻。データなしなら要求した at）
// 最後に見せた時点として求めた at（最新は求めた時刻）。+ 方向の移動は max(position, asked) から足す: スナップショットの
// 無い時間帯（日中の引け〜夜間、夜間の引け〜日中、週末・休業日）の手前では、at を進めても同じスナップショット S が返るので、
// S から足すと同じ時点に戻り続けて先に進めない
var asked = null;
var limitReached = false;  // 要求の上限に達した（理由を出し続ける）
var EPOCH = Date.UTC(2010, 0, 1) / 1000;  // サーバーの at の下限（settings.BARS_EPOCH）
var pinned = null;  // 比較用に固定した 1 時点
var view = null;  // 横の表示範囲 { min, max }（null は初期の範囲）
var cursorStrike = null;
var inflight = null;  // { ctrl }
var requests = 0;
var retry = null;
var pending;  // まとめている時点（undefined: なし、null: 最新、数値: at）
var debounceTimer = null;
var pointers = new Map();
var gesture = null;

function el(id) {
  return document.getElementById(id);
}

function pad(x) {
  return (x < 10 ? '0' : '') + x;
}

function nowSec() {
  return Math.floor(Date.now() / 1000);
}

// UTC の UNIX 秒 → "YYYY-MM-DD HH:MM"（JST）
function formatJst(t) {
  var d = new Date((t + JST_OFFSET) * 1000);
  return d.getUTCFullYear() + '-' + pad(d.getUTCMonth() + 1) + '-' + pad(d.getUTCDate()) + ' ' +
    pad(d.getUTCHours()) + ':' + pad(d.getUTCMinutes());
}

// datetime-local の値（JST として読む）→ UTC の UNIX 秒。不正なら null
function parseJstInput(value) {
  var m = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})/.exec(value || '');
  if (!m) {
    return null;
  }
  return Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3]), Number(m[4]), Number(m[5])) / 1000 - JST_OFFSET;
}

function toJstInput(t) {
  return formatJst(t).replace(' ', 'T');
}

function num(x) {
  return x === '' || x === undefined ? null : Number(x);
}

function fmtIv(v) {
  return v === null ? '—' : v.toFixed(2);
}

function fmtNum(v) {
  return v === null ? '—' : String(v);
}

// '#rrggbb' と不透明度 → 'rgba(...)'（1 はそのまま）
function alpha(hex, a) {
  if (a === 1) {
    return hex;
  }
  var n = parseInt(hex.slice(1), 16);
  return 'rgba(' + (n >> 16) + ', ' + ((n >> 8) & 255) + ', ' + (n & 255) + ', ' + a + ')';
}

// 約定時刻の表示: 'YYYY-MM-DD HH:MM（N分前）'。スナップショットより後は経過を出さない
function fmtPriceTime(snapshot, priceTime) {
  if (priceTime === null) {
    return '—';
  }
  var ago = SmileAge.agoText(SmileAge.ageSeconds(snapshot, priceTime));
  return formatJst(priceTime) + '（' + (ago === null ? '時点より後' : ago) + '）';
}

// ---- CSV ----

// /smile_snapshot の CSV → { s, months: {1: {ltd, atm}, 2: ...}, byMonth: {1: [行], 2: [行]}, hasQuote, lo, hi }。
// 本文が空（データなし）なら null
function parseSnapshot(text) {
  var lines = text.split('\n').filter(function(line) { return line !== ''; });
  if (!lines.length) {
    return null;
  }
  var meta = lines[0].split(',');
  // 想定の CSV でない応答（HTML 等）はエラーにする（snapshots に NaN の時刻を入れない）
  if (meta.length !== 5 || !/^[0-9]+$/.test(meta[0])) {
    throw new Error('invalid response');
  }
  var snap = {
    s: Number(meta[0]),
    months: {
      1: { ltd: meta[1] || null, atm: num(meta[2]) },
      2: { ltd: meta[3] || null, atm: num(meta[4]) },
    },
    byMonth: { 1: [], 2: [] },
    hasQuote: false,
    lo: null,
    hi: null,
  };
  for (var i = 1; i < lines.length; i++) {
    var cells = lines[i].split(',');
    if (cells.length !== COLUMNS.length) {
      continue;
    }
    var row = {};
    COLUMNS.forEach(function(c, j) { row[c] = num(cells[j]); });
    if (!snap.byMonth[row.month]) {
      continue;
    }
    snap.byMonth[row.month].push(row);
    if (row.bid_iv !== null || row.ask_iv !== null) {
      snap.hasQuote = true;
    }
    snap.lo = snap.lo === null ? row.target_price : Math.min(snap.lo, row.target_price);
    snap.hi = snap.hi === null ? row.target_price : Math.max(snap.hi, row.target_price);
  }
  return snap;
}

// ---- 読み込み中・エラーの表示（#77 と同じ） ----

function setBusy(on) {
  el('chart-wrap').setAttribute('aria-busy', on ? 'true' : 'false');
  el('chart-busy').hidden = !on;
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

// lines: 情報の表示（データなし・気配なし等。エラーの見た目にしない）、errors: 上限・入力の誤り（エラーの見た目）
function note(lines, errors) {
  var all = lines.concat(errors || []).filter(function(x) { return x; });
  el('message').textContent = all.join(' / ');
  el('message').classList.toggle('note-info', !(errors && errors.length));
}

// ---- 表示の設定 ----

function shownMonths() {
  var out = [];
  if (el('show-m1').checked) {
    out.push(1);
  }
  if (el('show-m2').checked) {
    out.push(2);
  }
  return out;
}

function mode() {
  return el('mode').value;
}

function showAge() {
  return el('show-age').checked;
}

// 描く曲線のまとまり: OTM 合成は限月ごとに 1 本（otm の行）、call・put 別は限月 × type
function groups(snap) {
  var out = [];
  if (!snap) {
    return out;
  }
  shownMonths().forEach(function(m) {
    var rows = snap.byMonth[m];
    if (mode() === 'otm') {
      out.push({ month: m, type: null, color: COLORS.otm[m], rows: rows.filter(function(r) { return r.otm === 1; }) });
    } else {
      out.push({ month: m, type: CALL, color: COLORS.call[m], rows: rows.filter(function(r) { return r.type === CALL; }) });
      out.push({ month: m, type: PUT, color: COLORS.put[m], rows: rows.filter(function(r) { return r.type === PUT; }) });
    }
  });
  return out;
}

function points(rows, key) {
  var out = [];
  rows.forEach(function(r) {
    if (r[key] !== null) {
      out.push({ x: r.target_price, y: r[key] });
    }
  });
  return out;
}

// 約定 IV の点: 点ごとに経過の段の見た目にする
function tradeDataset(snap, g) {
  var data = [];
  var fill = [];
  var border = [];
  var radius = [];
  g.rows.forEach(function(r) {
    if (r.iv === null) {
      return;
    }
    var t = TIER_STYLE[SmileAge.tierOf(SmileAge.ageSeconds(snap.s, r.price_time))];
    data.push({ x: r.target_price, y: r.iv });
    fill.push(t.fill === null ? '#ffffff' : alpha(g.color, t.fill));
    border.push(alpha(g.color, t.border));
    radius.push(t.radius);
  });
  return { type: 'line', data: data, showLine: false, borderColor: g.color, backgroundColor: '#ffffff',
    pointBackgroundColor: fill, pointBorderColor: border, pointRadius: radius, pointStyle: 'circle', borderWidth: 1.2,
    order: 0 };
}

// 右軸の経過（分）。約定の時刻が分かる行（経過が null でない行）だけ。
// 軸は対数（前日の約定が 1 本あっても直近の差が潰れないように）。0 分は描けないので AGE_AXIS_MIN 分に寄せる
var AGE_AXIS_MIN = 0.5;
// 右軸の目盛り（分）。対数軸の既定の目盛り（1〜9 × 10^n、小数つき）は上の方で重なるので、読みやすい区切りだけにする
var AGE_TICKS = [1, 5, 10, 30, 60, 120, 300, 600, 1440, 2880, 7200, 14400];

function ageTicks(axis) {
  axis.ticks = AGE_TICKS.filter(function(v) { return v >= axis.min && v <= axis.max; })
    .map(function(v) { return { value: v }; });
}

function ageDataset(snap, g) {
  var data = [];
  g.rows.forEach(function(r) {
    var age = SmileAge.ageSeconds(snap.s, r.price_time);
    if (age !== null) {
      data.push({ x: r.target_price, y: Math.max(age / 60, AGE_AXIS_MIN) });
    }
  });
  return { type: 'line', yAxisID: 'age', data: data, borderColor: alpha(g.color, 0.6), backgroundColor: g.color,
    borderDash: [2, 3], borderWidth: 1, pointRadius: 1.5, tension: 0, order: 4 };
}

function datasets() {
  var out = [];
  groups(shown).forEach(function(g) {
    var bold = g.month === 1;
    out.push({ type: 'line', data: points(g.rows, 'mid_iv'), borderColor: g.color, backgroundColor: g.color,
      borderWidth: bold ? 2 : 1.5, pointRadius: 0, tension: 0, order: 1 });
    ['bid_iv', 'ask_iv'].forEach(function(key) {
      out.push({ type: 'line', data: points(g.rows, key), showLine: false, borderColor: g.color,
        backgroundColor: g.color, pointStyle: 'line', pointRadius: 4, borderWidth: 1, order: 2 });
    });
    out.push(tradeDataset(shown, g));
    if (showAge()) {
      out.push(ageDataset(shown, g));
    }
  });
  if (pinned) {
    groups(pinned).forEach(function(g) {
      out.push({ type: 'line', data: points(g.rows, 'mid_iv'), borderColor: COLORS.pin[g.month],
        backgroundColor: COLORS.pin[g.month], borderDash: [6, 4], borderWidth: 1.5, pointRadius: 0, tension: 0,
        order: 3 });
    });
  }
  return out;
}

// ---- 横の表示範囲 ----

function bounds() {
  var lo = null;
  var hi = null;
  [shown, pinned].forEach(function(s) {
    if (s && s.lo !== null) {
      lo = lo === null ? s.lo : Math.min(lo, s.lo);
      hi = hi === null ? s.hi : Math.max(hi, s.hi);
    }
  });
  if (lo === null) {
    return null;
  }
  var margin = Math.max(250, (hi - lo) * 0.02);
  return { min: lo - margin, max: hi + margin };
}

function initialView() {
  var b = bounds();
  if (!b || !shown) {
    return null;
  }
  var months = shownMonths();
  var m = months.indexOf(1) >= 0 || !months.length ? 1 : 2;
  var atm = shown.months[m].atm !== null ? shown.months[m].atm : shown.months[m === 1 ? 2 : 1].atm;
  if (atm === null) {
    return b;
  }
  return clampView(atm * (1 - INITIAL_WIDTH), atm * (1 + INITIAL_WIDTH));
}

function clampView(min, max) {
  var b = bounds();
  if (!b) {
    return { min: min, max: max };
  }
  var width = Math.min(Math.max(max - min, MIN_WIDTH), b.max - b.min);
  var center = (min + max) / 2;
  min = center - width / 2;
  max = center + width / 2;
  if (min < b.min) {
    max += b.min - min;
    min = b.min;
  }
  if (max > b.max) {
    min -= max - b.max;
    max = b.max;
  }
  return { min: min, max: max };
}

function applyView() {
  if (!view) {
    view = initialView();
  }
  var x = chart.options.scales.x;
  x.min = view ? view.min : undefined;
  x.max = view ? view.max : undefined;
}

function setView(min, max) {
  if (!shown) {
    return;
  }
  view = clampView(min, max);
  applyView();
  chart.update('none');
}

// ---- 描画 ----

function render() {
  chart.data.datasets = datasets();
  chart.options.scales.age.display = !!shown && showAge();
  applyView();
  chart.update('none');
  renderHeading();
  renderLegend();
  renderCursor();
  renderNotes();
}

function renderHeading() {
  if (shown) {
    el('snapshot-time').textContent = formatJst(shown.s) + ' JST 時点';
  } else if (position !== null) {
    el('snapshot-time').textContent = formatJst(position) + ' JST 以前';
  } else {
    el('snapshot-time').textContent = '';
  }
  if (position !== null && document.activeElement !== el('at-input')) {
    el('at-input').value = toJstInput(position);
  }
  el('pin').disabled = !shown;
  el('unpin').disabled = !pinned;
}

function legendItem(swatchClass, text, dashed) {
  var item = document.createElement('span');
  item.className = 'legend-item';
  var sw = document.createElement('span');
  sw.className = 'swatch ' + swatchClass + (dashed ? ' swatch-dash' : '');
  item.appendChild(sw);
  var label = document.createElement('span');
  label.textContent = text;
  item.appendChild(label);
  return item;
}

function monthLabel(snap, m) {
  var ltd = snap.months[m].ltd;
  return m + '限月' + (ltd ? '（' + ltd + '）' : '');
}

function renderLegend() {
  var box = el('legend');
  box.textContent = '';
  if (!shown) {
    return;
  }
  var current = document.createElement('span');
  current.className = 'legend-item';
  current.dataset.role = 'shown';
  current.textContent = '表示: ' + formatJst(shown.s) + ' JST';
  box.appendChild(current);
  groups(shown).forEach(function(g) {
    var kind = g.type === null ? 'otm' : g.type === CALL ? 'call' : 'put';
    var label = monthLabel(shown, g.month) + ' ' + (g.type === null ? 'OTM' : g.type === CALL ? 'call' : 'put') +
      ' mid IV';
    box.appendChild(legendItem('sw-' + kind + '-' + g.month, label, false));
  });
  if (pinned) {
    var item = legendItem('sw-pin', '固定: ' + formatJst(pinned.s) + ' JST', true);
    item.dataset.role = 'pinned';
    box.appendChild(item);
  }
  if (showAge()) {
    var age = legendItem('sw-age', '約定からの経過（右軸）', true);
    age.dataset.role = 'age';
    box.appendChild(age);
  }
  // 約定 IV の点の段（色見本は曲線の色によらない灰色の丸）
  var head = document.createElement('span');
  head.className = 'legend-item';
  head.textContent = '約定IV:';
  box.appendChild(head);
  SmileAge.TIERS.forEach(function(t, i) {
    var tier = document.createElement('span');
    tier.className = 'legend-item';
    tier.dataset.role = 'tier';
    tier.dataset.tier = String(i);
    var dot = document.createElement('span');
    dot.className = 'dot dot-t' + i;
    tier.appendChild(dot);
    var label = document.createElement('span');
    label.textContent = t.label;
    tier.appendChild(label);
    box.appendChild(tier);
  });
}

function nearestStrike(x) {
  var best = null;
  if (!shown) {
    return null;
  }
  shownMonths().forEach(function(m) {
    shown.byMonth[m].forEach(function(r) {
      if (best === null || Math.abs(r.target_price - x) < Math.abs(best - x)) {
        best = r.target_price;
      }
    });
  });
  return best;
}

function field(row, name, label, value) {
  var span = document.createElement('span');
  span.className = 'cursor-field';
  span.dataset.field = name;
  span.textContent = label + ' ' + value;
  row.appendChild(span);
}

// カーソル位置の行使価格の値（表示中の限月 × call / put ごとに 1 行）
function renderCursor() {
  var box = el('cursor');
  box.textContent = '';
  if (!shown || cursorStrike === null) {
    return;
  }
  var head = document.createElement('div');
  head.className = 'cursor-row';
  head.dataset.role = 'strike';
  head.textContent = '行使価格 ' + cursorStrike;
  box.appendChild(head);
  shownMonths().forEach(function(m) {
    [CALL, PUT].forEach(function(type) {
      var r = shown.byMonth[m].find(function(x) { return x.target_price === cursorStrike && x.type === type; });
      var row = document.createElement('div');
      row.className = 'cursor-row';
      row.dataset.month = String(m);
      row.dataset.type = type === CALL ? 'call' : 'put';
      var label = document.createElement('span');
      label.className = 'cursor-label';
      label.textContent = monthLabel(shown, m) + ' ' + (type === CALL ? 'call' : 'put') + (r && r.otm === 1 ? '（OTM）' : '');
      row.appendChild(label);
      if (!r) {
        field(row, 'none', '', '値なし');
      } else {
        field(row, 'mid_iv', 'mid', fmtIv(r.mid_iv));
        field(row, 'bid_iv', 'bid', fmtIv(r.bid_iv));
        field(row, 'ask_iv', 'ask', fmtIv(r.ask_iv));
        field(row, 'iv', '約定IV', fmtIv(r.iv));
        field(row, 'price', '約定', fmtNum(r.price));
        field(row, 'price_time', '約定時刻', fmtPriceTime(shown.s, r.price_time));
        field(row, 'bid', '気配', fmtNum(r.bid) + ' / ' + fmtNum(r.ask));
      }
      box.appendChild(row);
    });
  });
}

function renderNotes(extra) {
  var lines = [];
  if (position !== null && !shown) {
    lines.push('データがありません');
  }
  if (shown && !shown.hasQuote) {
    lines.push('気配がありません（取引時間外）。約定の IV の点だけを表示しています');
  }
  if (shown && mode() === 'otm') {
    shownMonths().forEach(function(m) {
      if (shown.months[m].atm === null && shown.byMonth[m].length) {
        lines.push(m + ' 限月の ATM が分からないので OTM 合成を描けません');
      }
    });
  }
  var errors = [];
  if (limitReached) {
    errors.push('この画面で読み込めるのは ' + MAX_REQUESTS + ' 時点までです（ページを読み直してください）');
  }
  if (extra) {
    errors.push(extra);
  }
  note(lines, errors);
}

// ATM の縦線（1 限月は実線、2 限月は点線）・カーソルの縦線・bid〜ask のひげの縦線を canvas に描く
var overlay = {
  id: 'smileOverlay',
  beforeDatasetsDraw: function(c) {
    var area = c.chartArea;
    var x = c.scales.x;
    var y = c.scales.y;
    var ctx = c.ctx;
    ctx.save();
    ctx.beginPath();
    ctx.rect(area.left, area.top, area.right - area.left, area.bottom - area.top);
    ctx.clip();
    groups(shown).forEach(function(g) {
      ctx.strokeStyle = g.color;
      ctx.lineWidth = 1;
      ctx.setLineDash([]);
      ctx.beginPath();
      g.rows.forEach(function(r) {
        if (r.bid_iv !== null && r.ask_iv !== null) {
          var px = x.getPixelForValue(r.target_price);
          ctx.moveTo(px, y.getPixelForValue(r.bid_iv));
          ctx.lineTo(px, y.getPixelForValue(r.ask_iv));
        }
      });
      ctx.stroke();
    });
    if (shown) {
      shownMonths().forEach(function(m) {
        var atm = shown.months[m].atm;
        if (atm === null) {
          return;
        }
        var px = x.getPixelForValue(atm);
        ctx.strokeStyle = m === 1 ? 'rgba(0, 0, 0, 0.55)' : 'rgba(0, 0, 0, 0.35)';
        ctx.setLineDash(m === 1 ? [] : [2, 3]);
        ctx.beginPath();
        ctx.moveTo(px, area.top);
        ctx.lineTo(px, area.bottom);
        ctx.stroke();
        ctx.fillStyle = 'rgba(0, 0, 0, 0.6)';
        ctx.fillText('ATM ' + atm + (m === 2 ? '（2限月）' : ''), px + 4, area.top + 12 + (m - 1) * 14);
      });
    }
    if (cursorStrike !== null && shown) {
      var cx = x.getPixelForValue(cursorStrike);
      ctx.strokeStyle = 'rgba(255, 111, 0, 0.8)';
      ctx.setLineDash([4, 3]);
      ctx.beginPath();
      ctx.moveTo(cx, area.top);
      ctx.lineTo(cx, area.bottom);
      ctx.stroke();
    }
    ctx.restore();
  },
};

function createChart() {
  chart = new Chart(el('chart'), {
    type: 'line',
    data: { datasets: [] },
    options: {
      animation: false,
      responsive: true,
      maintainAspectRatio: false,
      events: [],  // ホバー・ツールチップは使わない（カーソル位置の値は DOM に出す）
      plugins: { legend: { display: false }, tooltip: { enabled: false } },
      scales: {
        x: { type: 'linear', title: { display: true, text: '行使価格' } },
        y: { type: 'linear', grace: '5%', title: { display: true, text: 'IV (%)' } },
        // 約定からの経過（分）。「経過時間」がオンのときだけ見せる
        age: { type: 'logarithmic', position: 'right', display: false, min: AGE_AXIS_MIN, grid: { drawOnChartArea: false },
          afterBuildTicks: ageTicks, ticks: { callback: function(v) { return String(v); } },
          title: { display: true, text: '約定からの経過（分）' } },
      },
    },
    plugins: [overlay],
  });
}

// ---- 読み込み ----

function lookup(at) {
  for (var i = 0; i < known.length; i++) {
    if (known[i].lo <= at && at <= known[i].hi) {
      return known[i];
    }
  }
  return null;
}

function show(snap, at) {
  shown = snap;
  position = snap ? snap.s : at;
  asked = at;
  if (snap && cursorStrike !== null) {
    cursorStrike = nearestStrike(cursorStrike);
  }
  render();
}

function abortInflight() {
  if (inflight) {
    inflight.ctrl.abort();
    inflight = null;
  }
  setBusy(false);
}

// at（null は最新）の時点を見せる。読んだことがあれば要求しない
function go(at) {
  if (at !== null && at >= nowSec()) {
    at = null;  // 未来は最新（サーバーも now に縮める）
  }
  if (at === null && latest && Date.now() - latest.fetched < LATEST_FRESH_MS) {
    abortInflight();
    clearError();
    show(latest.s === null ? null : snapshots[latest.s], nowSec());
    return;
  }
  if (at !== null) {
    var hit = lookup(at);
    if (hit) {
      abortInflight();
      clearError();
      show(hit.s === null ? null : snapshots[hit.s], at);
      return;
    }
  }
  request(at);
}

function request(at) {
  abortInflight();
  clearError();
  if (requests >= MAX_REQUESTS) {
    limitReached = true;
    renderNotes();
    return;
  }
  requests++;
  var ctrl = new AbortController();
  var mine = { ctrl: ctrl };
  inflight = mine;
  setBusy(true);
  var url = '/smile_snapshot' + (at === null ? '' : '?at=' + at);
  fetch(url, { signal: ctrl.signal, cache: 'no-store', credentials: 'same-origin' })
    .then(function(res) {
      if (!res.ok) {
        throw new Error('HTTP ' + res.status);
      }
      // 解析の失敗（想定の CSV でない）は catch でエラーにする
      return res.text().then(parseSnapshot);
    })
    .then(function(snap) {
      if (inflight !== mine) {
        return;
      }
      inflight = null;
      setBusy(false);
      var when = at === null ? nowSec() : at;
      if (snap) {
        snapshots[snap.s] = snap;
        // [S, at] にはほかのスナップショットが無い（最新は at が分からないので S だけ）
        known.push({ lo: snap.s, hi: at === null ? snap.s : at, s: snap.s });
      } else {
        known.push({ lo: when - SEARCH_DAYS * DAY, hi: when, s: null });
      }
      if (at === null) {
        latest = { s: snap ? snap.s : null, fetched: Date.now() };
      }
      show(snap, when);
    })
    .catch(function(err) {
      if (inflight !== mine) {
        return;  // 中断された（後から選んだ時点を描く）
      }
      inflight = null;
      setBusy(false);
      var message = err && err.message ? err.message : '';
      var reason = message.indexOf('HTTP ') === 0 || message === 'invalid response' ? message : 'network';
      // 前のチャートを残して薄くし、その上にエラーと「再試行」（同じ時点）
      showError('データを取得できませんでした (' + reason + ')', function() { request(at); }, true);
    });
}

// 時点の移動をまとめる（DEBOUNCE_MS の間の操作は最後の時点の 1 回にする）
function schedule(at) {
  pending = at;
  clearTimeout(debounceTimer);
  debounceTimer = setTimeout(function() {
    var target = pending;
    pending = undefined;
    go(target);
  }, DEBOUNCE_MS);
}

function step(seconds) {
  var base;
  if (pending !== undefined) {
    base = pending === null ? nowSec() : pending;
  } else if (position !== null) {
    // − 方向は表示中の時刻から、+ 方向は表示中の時刻と最後に求めた at の遅い方から（スナップショットの無い時間帯を越える）
    base = seconds > 0 && asked !== null ? Math.max(position, asked) : position;
  } else {
    return;
  }
  schedule(Math.max(EPOCH, Math.min(base + seconds, nowSec())));
}

// ---- 操作（拡大縮小・スクロール。サーバーに要求しない） ----

function valueAt(px) {
  return chart.scales.x.getValueForPixel(px);
}

function canvasX(e) {
  var rect = el('chart').getBoundingClientRect();
  return e.clientX - rect.left;
}

function zoomAt(px, factor) {
  if (!shown || !view) {
    return;
  }
  var c = valueAt(px);
  setView(c - (c - view.min) * factor, c + (view.max - c) * factor);
}

function setCursorAt(px) {
  if (!shown) {
    return;
  }
  var area = chart.chartArea;
  if (px < area.left || px > area.right) {
    return;
  }
  var strike = nearestStrike(valueAt(px));
  if (strike !== cursorStrike) {
    cursorStrike = strike;
    renderCursor();
    chart.update('none');
  }
}

function startGesture() {
  var pts = Array.from(pointers.values());
  if (!view || !pts.length) {
    gesture = null;
    return;
  }
  var area = chart.chartArea;
  if (pts.length === 1) {
    gesture = { kind: 'pan', x: pts[0].x, view: { min: view.min, max: view.max }, perPx: (view.max - view.min) /
      (area.right - area.left) };
  } else {
    var mid = (pts[0].x + pts[1].x) / 2;
    gesture = { kind: 'pinch', dist: Math.max(1, Math.abs(pts[0].x - pts[1].x)), center: valueAt(mid),
      width: view.max - view.min, areaWidth: area.right - area.left };
  }
}

function onPointerDown(e) {
  if (!shown) {
    return;
  }
  el('chart').setPointerCapture(e.pointerId);
  pointers.set(e.pointerId, { x: canvasX(e) });
  if (e.pointerType !== 'mouse' || pointers.size === 1) {
    setCursorAt(canvasX(e));
  }
  el('chart').classList.add('dragging');
  startGesture();
}

function onPointerMove(e) {
  var px = canvasX(e);
  if (!pointers.has(e.pointerId)) {
    if (e.pointerType === 'mouse') {
      setCursorAt(px);
    }
    return;
  }
  pointers.get(e.pointerId).x = px;
  if (!gesture) {
    return;
  }
  var pts = Array.from(pointers.values());
  if (gesture.kind === 'pan' && pts.length === 1) {
    var dv = (px - gesture.x) * gesture.perPx;
    setView(gesture.view.min - dv, gesture.view.max - dv);
    if (e.pointerType === 'mouse') {
      setCursorAt(px);
    }
  } else if (gesture.kind === 'pinch' && pts.length >= 2) {
    var dist = Math.max(1, Math.abs(pts[0].x - pts[1].x));
    var width = gesture.width * gesture.dist / dist;
    var mid = (pts[0].x + pts[1].x) / 2;
    var area = chart.chartArea;
    var min = gesture.center - (mid - area.left) / gesture.areaWidth * width;
    setView(min, min + width);
  }
}

function onPointerUp(e) {
  if (!pointers.has(e.pointerId)) {
    return;
  }
  pointers.delete(e.pointerId);
  if (!pointers.size) {
    el('chart').classList.remove('dragging');
    gesture = null;
  } else {
    startGesture();
  }
}

function onWheel(e) {
  if (!shown) {
    return;
  }
  e.preventDefault();
  zoomAt(canvasX(e), e.deltaY > 0 ? WHEEL_FACTOR : 1 / WHEEL_FACTOR);
}

// ---- 初期化 ----

function bind() {
  var canvas = el('chart');
  canvas.addEventListener('pointerdown', onPointerDown);
  canvas.addEventListener('pointermove', onPointerMove);
  canvas.addEventListener('pointerup', onPointerUp);
  canvas.addEventListener('pointercancel', onPointerUp);
  canvas.addEventListener('wheel', onWheel, { passive: false });
  el('latest').addEventListener('click', function() { schedule(null); });
  Array.from(document.querySelectorAll('button.step')).forEach(function(b) {
    b.addEventListener('click', function() { step(Number(b.dataset.step)); });
  });
  el('at-go').addEventListener('click', function() {
    var at = parseJstInput(el('at-input').value);
    // 不正な値・サーバーの下限（2010-01-01 UTC）より前は要求しない（400 で上限の数を減らさない）
    if (at === null || !isFinite(at) || at < EPOCH) {
      renderNotes('日時は 2010-01-01 以降を入力してください');
      return;
    }
    schedule(Math.min(at, nowSec()));
  });
  el('show-m1').addEventListener('change', render);
  el('show-m2').addEventListener('change', render);
  el('mode').addEventListener('change', render);
  el('show-age').addEventListener('change', render);
  el('pin').addEventListener('click', function() {
    if (shown) {
      pinned = shown;
      render();
    }
  });
  el('unpin').addEventListener('click', function() {
    pinned = null;
    render();
  });
  el('reset').addEventListener('click', function() {
    view = null;
    render();
  });
  el('chart-retry').addEventListener('click', function() {
    if (retry) {
      retry();
    }
  });
}

createChart();
bind();
render();
go(null);
