// /atm_iv の系列（限月の選択肢）と足の列の純粋関数（#101）。DOM・Lightweight Charts に触らない。ブラウザでは global の
// AtmSeries、node（tests/js/atm_series.test.js）では module.exports。時刻は UTC の UNIX 秒。
// - 系列: value = #nmonth の選択の値（読み込み済みの足を覚えるキーにも使う）、label = 表示、query = /atm_iv_bars に付ける
//   パラメータ、months = CSV の 6 列目（その足の限月 = 最終取引日）を読み、乗り換えの印・凡例の限月を出す
// - 期近つなぎ（n=roll）: サーバーが取引日ごとに期近の最終取引日の 5 営業日前で 2 限月へ乗り換えた足を返す。
//   印は限月が前の足と変わった最初の足に付ける（左・右に読み足してつないだ後の列全体から作り直す）
var AtmSeries = (function() {
  var SERIES = [
    { value: '0', label: '0', query: { n: '0' }, months: false },
    { value: '1', label: '1', query: { n: '1' }, months: false },
    { value: 'roll', label: '期近つなぎ', query: { n: 'roll' }, months: true },
  ];
  var DEFAULT_SERIES = '0';

  // 無い値は既定の系列
  function seriesOf(value) {
    for (var i = 0; i < SERIES.length; i++) {
      if (SERIES[i].value === value) {
        return SERIES[i];
      }
    }
    return seriesOf(DEFAULT_SERIES);
  }

  function barsUrl(series, src, iv, start, end) {
    var url = '/atm_iv_bars?tf=' + encodeURIComponent(src);
    Object.keys(series.query).forEach(function(k) {
      url += '&' + encodeURIComponent(k) + '=' + encodeURIComponent(series.query[k]);
    });
    if (iv === 'mid') {
      url += '&iv=mid';
    }
    if (start !== null) {
      url += '&start=' + start + '&end=' + end;
    }
    return url;
  }

  // CSV（open,high,low,close,started_at[,last_trading_day]）→ 足。値の無い足は飛ばす
  function parseCsv(text) {
    var out = [];
    var lines = text.split('\n');
    for (var i = 0; i < lines.length; i++) {
      var cells = lines[i].split(',');
      if (cells.length < 5 || cells[1] === '') {
        continue;
      }
      var bar = {
        // started_at は "YYYY-MM-DD HH:MM:SS+00:00"。ISO 8601 の形にしてから読む
        time: Date.parse(cells[4].replace(' ', 'T')) / 1000,
        open: Number(cells[0]),
        high: Number(cells[1]),
        low: Number(cells[2]),
        close: Number(cells[3]),
      };
      if (cells.length > 5 && cells[5] !== '') {
        bar.month = cells[5];
      }
      out.push(bar);
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

  // 1 分足を seconds 秒の足に束ねる（サーバーの bars.resample と同じ: UNIX 秒で区切る。open / close = 最初 / 最後）。
  // 限月（month）が違う足は同じ区間でも 1 本にしない
  function resample(bars, seconds) {
    if (seconds === 60) {
      return bars;
    }
    var out = [];
    bars.forEach(function(b) {
      var t = Math.floor(b.time / seconds) * seconds;
      var last = out[out.length - 1];
      if (last && last.time === t && last.month === b.month) {
        last.high = Math.max(last.high, b.high);
        last.low = Math.min(last.low, b.low);
        last.close = b.close;
      } else {
        var bar = { time: t, open: b.open, high: b.high, low: b.low, close: b.close };
        if (b.month !== undefined) {
          bar.month = b.month;
        }
        out.push(bar);
      }
    });
    return out;
  }

  // '2026-12-10' → '12月限'
  function monthLabel(month) {
    return Number(month.slice(5, 7)) + '月限';
  }

  // '2026-12-10' → '12月限（最終取引日 2026-12-10）'
  function monthLegend(month) {
    return monthLabel(month) + '（最終取引日 ' + month + '）';
  }

  // 乗り換えの印: 限月が前の足と変わった最初の足だけ（最初の足には付けない）
  function rollMarkers(bars) {
    var out = [];
    for (var i = 1; i < bars.length; i++) {
      if (bars[i].month !== undefined && bars[i].month !== bars[i - 1].month) {
        out.push({ time: bars[i].time, month: bars[i].month, text: monthLabel(bars[i].month) });
      }
    }
    return out;
  }

  return {
    SERIES: SERIES, DEFAULT_SERIES: DEFAULT_SERIES, seriesOf: seriesOf, barsUrl: barsUrl, parseCsv: parseCsv,
    mergeBars: mergeBars, resample: resample, monthLabel: monthLabel, monthLegend: monthLegend, rollMarkers: rollMarkers,
  };
})();

if (typeof module !== 'undefined' && module.exports) {
  module.exports = AtmSeries;
}
