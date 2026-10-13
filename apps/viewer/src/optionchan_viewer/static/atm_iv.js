// ATM IV のローソク足（#68。TradingView Lightweight Charts 5）。足（1分・5分・15分・1時間・日足）・期間・限月を選ぶと
// /atm_iv_bars?tf=&d=&n= の CSV（open,high,low,close,started_at。値の無い足は API が除く）を読んで描く。
// スクロール・拡縮はライブラリの中だけ（読み込んだ範囲。サーバーには取りに行かない）。
// 時刻は UTC の UNIX 秒に 9 時間を足して渡す（日本に夏時間は無い）。ライブラリは UTC として表示するので、目盛り・ラベルが JST になる。
// 足は時刻の並び（データのある足だけ）で描かれるので、取引の無い時間・週末・休日は空白にならず詰まる。
var JST_OFFSET = 9 * 3600;
// 期間（日）の選択肢と既定（サーバーの settings.BARS_D の範囲の中）。1時間足は d が小さいと週末に 0 本になるので 5 日から
var DAYS = {
  '1m': { choices: [1, 2, 3], selected: 1 },
  '5m': { choices: [1, 2, 3], selected: 1 },
  '15m': { choices: [1, 2, 3], selected: 1 },
  '1h': { choices: [5, 10, 20, 30, 60, 100], selected: 10 },
  '1d': { choices: [30, 60, 100], selected: 100 },
};
var INITIAL_BARS = 150;  // 最初に見せる右端（最新）の本数

var chart = null;
var series = null;
var bars = [];
var currentTf = '1h';
var cache = new Map();  // "tf|d|n" → 足の配列（同じ選択を何度も取りに行かない。BigQuery のスキャン量）
var requestSeq = 0;  // 古い応答で上書きしないための通し番号

function el(id) {
  return document.getElementById(id);
}

function pad(x) {
  return (x < 10 ? '0' : '') + x;
}

// time（UTC 秒 + 9 時間）→ "YYYY-MM-DD HH:MM"（日足は日付だけ）
function formatTime(time) {
  var d = new Date(time * 1000);
  var date = d.getUTCFullYear() + '-' + pad(d.getUTCMonth() + 1) + '-' + pad(d.getUTCDate());
  if (currentTf === '1d') {
    return date;
  }
  return date + ' ' + pad(d.getUTCHours()) + ':' + pad(d.getUTCMinutes());
}

function showLegend(bar) {
  if (!bar) {
    el('legend').textContent = '';
    return;
  }
  el('legend').textContent = formatTime(bar.time) + '  O ' + bar.open.toFixed(2) + '  H ' + bar.high.toFixed(2) +
    '  L ' + bar.low.toFixed(2) + '  C ' + bar.close.toFixed(2);
}

function createChart() {
  chart = LightweightCharts.createChart(el('chart'), {
    autoSize: true,
    // attributionLogo は <style> 要素を足すので CSP（style-src 'self'）に反する。表記はページの下に置く
    layout: { attributionLogo: false },
    localization: { locale: 'ja-JP', timeFormatter: formatTime },
    timeScale: { timeVisible: true, secondsVisible: false },
  });
  series = chart.addSeries(LightweightCharts.CandlestickSeries, {
    priceFormat: { type: 'price', precision: 2, minMove: 0.01 },
  });
  chart.subscribeCrosshairMove(function(param) {
    var bar = param.time !== undefined ? param.seriesData.get(series) : null;
    showLegend(bar || bars[bars.length - 1]);
  });
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
      time: Date.parse(cells[4].replace(' ', 'T')) / 1000 + JST_OFFSET,
      open: Number(cells[0]),
      high: Number(cells[1]),
      low: Number(cells[2]),
      close: Number(cells[3]),
    });
  }
  return out;
}

function draw(data) {
  bars = data;
  chart.applyOptions({ timeScale: { timeVisible: currentTf !== '1d' } });
  series.setData(data);
  if (data.length === 0) {
    el('message').textContent = 'データがありません';
  } else if (data.length > INITIAL_BARS) {
    chart.timeScale().setVisibleLogicalRange({ from: data.length - INITIAL_BARS, to: data.length + 2 });
  } else {
    chart.timeScale().fitContent();
  }
  showLegend(data[data.length - 1]);
}

// 取得に失敗したら前の足を消す（選択欄と描画が食い違わないように）
function fail(message) {
  el('loading').style.display = 'none';
  el('message').textContent = message;
  bars = [];
  series.setData([]);
  showLegend(undefined);
}

function update() {
  var tf = el('tf').value;
  var days = el('days').value;
  var nmonth = el('nmonth').value;
  var key = tf + '|' + days + '|' + nmonth;
  var seq = ++requestSeq;
  el('message').textContent = '';
  if (cache.has(key)) {
    el('loading').style.display = 'none';
    currentTf = tf;
    draw(cache.get(key));
    return;
  }
  el('loading').style.display = 'block';
  var req = new XMLHttpRequest();
  req.open('GET', '/atm_iv_bars?tf=' + encodeURIComponent(tf) + '&d=' + encodeURIComponent(days) +
    '&n=' + encodeURIComponent(nmonth), true);
  req.onload = function() {
    if (seq !== requestSeq) {
      return;  // 後から選び直した分を描く
    }
    if (req.status !== 200) {
      fail('データを取得できませんでした (' + req.status + ')');
      return;
    }
    el('loading').style.display = 'none';
    var data = parseCsv(req.responseText);
    cache.set(key, data);
    currentTf = tf;
    draw(data);
  };
  req.onerror = function() {
    if (seq === requestSeq) {
      fail('データを取得できませんでした (network)');
    }
  };
  req.send(null);
}

// 足に合わせて期間の選択肢を作り直す（既定を選ぶ）
function fillDays(tf) {
  var select = el('days');
  while (select.firstChild) {
    select.removeChild(select.firstChild);
  }
  DAYS[tf].choices.forEach(function(d) {
    var option = document.createElement('option');
    option.value = String(d);
    option.textContent = d + '日';
    option.selected = d === DAYS[tf].selected;
    select.appendChild(option);
  });
}

// ライセンス（Apache-2.0）の帰属表記のリンク。テンプレートに外部の URL を書かないため、ここで設定する
el('attribution-link').href = 'https://www.tradingview.com/';

el('tf').addEventListener('change', function() {
  // 限月（nmonth）はそのまま
  fillDays(el('tf').value);
  update();
});
el('days').addEventListener('change', update);
el('nmonth').addEventListener('change', update);

createChart();
fillDays(el('tf').value);
update();
