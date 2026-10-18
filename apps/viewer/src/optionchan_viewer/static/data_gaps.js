// /atm_iv の過去データの欠け（#123）の純粋関数。DOM・Lightweight Charts に触らない。ブラウザでは global の DataGaps、
// node（tests/js/data_gaps.test.js）では module.exports。時刻は UTC の UNIX 秒。
// - 区間は /data_gaps の CSV（prev_at,next_at,cause,status。表 data_gaps の値で、このファイルは区間の時刻を持たない）。
//   prev_at・next_at は欠けの前後のスナップショットで、欠けているのは分の時刻 c が prev_at < c < next_at のもの
// - 足が覆う時間（その足に入るスナップショットの時刻の範囲）: 分足・1 時間足（期近つなぎも）は [time, time + 足の秒数)。
//   日足（time = 取引日の 00:00 JST）は前の足の time + 16 時間〜この足の time + 16 時間（16:00 JST を日中と夜間の境目にする。
//   週末・休業日は前の足で越える）。先頭の日足は time − 8 時間から
// - 約定（n = 0・1・期近つなぎ）で #114 が作った区間（filled・partial）は帯（当たる足すべて）。それ以外（作れなかった区間、
//   mid・30日固定のすべて）は欠けの印（当たる最初の足。当たる足が無ければ区間の後で最初の足）。1 本の足に 1 つにまとめる
var DataGaps = (function() {
  var JST = 9 * 3600;
  var DAY = 86400;
  var KINDS = { bug_format: '欠け 1', bug_comma: '欠け 3', unknown: '停止・取得の失敗', no_change: '値の変化なし' };
  var FILLED = { filled: true, partial: true };
  var NUM = /^[0-9]+$/;

  function pad(x) {
    return (x < 10 ? '0' : '') + x;
  }

  // UTC 秒 → JST の "YYYY-MM-DD HH:MM"
  function jstText(t) {
    var d = new Date((t + JST) * 1000);
    return d.getUTCFullYear() + '-' + pad(d.getUTCMonth() + 1) + '-' + pad(d.getUTCDate()) + ' ' +
      pad(d.getUTCHours()) + ':' + pad(d.getUTCMinutes());
  }

  // /data_gaps の本文 → 区間（prevAt 昇順）。4 列で、時刻が数字かつ prevAt < nextAt の行だけ
  function parse(text) {
    if (typeof text !== 'string' || !text) {
      return [];
    }
    var out = [];
    text.split('\n').forEach(function(line) {
      var cells = line.split(',');
      if (cells.length !== 4 || !NUM.test(cells[0]) || !NUM.test(cells[1])) {
        return;
      }
      var prevAt = Number(cells[0]);
      var nextAt = Number(cells[1]);
      if (!(prevAt < nextAt)) {
        return;
      }
      out.push({ prevAt: prevAt, nextAt: nextAt, cause: cells[2], status: cells[3],
        period: jstText(prevAt) + '〜' + jstText(nextAt) });
    });
    // 安定な並べ替え（同じ prevAt は入力順）
    return out.map(function(g, i) { return [g, i]; })
      .sort(function(a, b) { return a[0].prevAt - b[0].prevAt || a[1] - b[1]; })
      .map(function(x) { return x[0]; });
  }

  // 欠けの分の時刻の最初と最後（prevAt < c < nextAt の 60 の倍数）。無ければ null
  function minutes(g) {
    var first = Math.floor(g.prevAt / 60) * 60 + 60;
    var last = Math.ceil(g.nextAt / 60) * 60 - 60;
    return first <= last ? { first: first, last: last } : null;
  }

  // 'trade'（約定: 作った区間は帯）/ 'quote'（mid・30日固定: 作った区間も気配が無いので欠け）
  function modeOf(series, iv) {
    return !series.fixedIv && iv !== 'mid' ? 'trade' : 'quote';
  }

  // 足ごとの覆う時間 [from, to)（UTC 秒）。bars は time 昇順
  function spans(bars, seconds) {
    return bars.map(function(b, i) {
      if (seconds !== DAY) {
        return { time: b.time, from: b.time, to: b.time + seconds };
      }
      var from = i === 0 ? b.time - 8 * 3600 : bars[i - 1].time + 16 * 3600;
      return { time: b.time, from: from, to: b.time + 16 * 3600 };
    });
  }

  // to > t の最初の足の位置（無ければ length）
  function firstEndingAfter(sp, t) {
    var lo = 0;
    var hi = sp.length;
    while (lo < hi) {
      var mid = (lo + hi) >> 1;
      if (sp[mid].to > t) {
        hi = mid;
      } else {
        lo = mid + 1;
      }
    }
    return lo;
  }

  function add(map, time, g) {
    if (!map.has(time)) {
      map.set(time, []);
    }
    map.get(time).push(g);
  }

  function listOf(map) {
    return Array.from(map.keys()).sort(function(a, b) { return a - b; })
      .map(function(t) { return { time: t, gaps: map.get(t) }; });
  }

  // 足（描いている足。time 昇順）と区間 → { bands, marks }（それぞれ time 昇順・time は一意・gaps は prevAt 順）
  function assign(bars, seconds, gaps, mode) {
    if (!bars.length || !gaps.length) {
      return { bands: [], marks: [] };
    }
    var sp = spans(bars, seconds);
    var bands = new Map();
    var marks = new Map();
    gaps.forEach(function(g) {
      var m = minutes(g);
      if (!m) {
        return;
      }
      var i = firstEndingAfter(sp, m.first);
      var hit = i < sp.length && sp[i].from <= m.last;
      if (mode === 'trade' && FILLED[g.status]) {
        for (var j = i; j < sp.length && sp[j].from <= m.last; j++) {
          add(bands, sp[j].time, g);
        }
      } else if (hit) {
        add(marks, sp[i].time, g);
      } else if (i > 0 && i < sp.length) {
        // 足ごと詰められた区間: 区間より前に終わる足と後に始まる足の両方があるときだけ、後の最初の足
        add(marks, sp[i].time, g);
      }
    });
    return { bands: listOf(bands), marks: listOf(marks) };
  }

  // 欠けの分の時刻が [start, end) にかかる区間（status を問わない。入力の順）
  function overlapping(gaps, start, end) {
    return gaps.filter(function(g) {
      var m = minutes(g);
      return m !== null && m.first < end && m.last >= start;
    });
  }

  function stateText(g, mode) {
    if (!FILLED[g.status]) {
      return '欠け';
    }
    if (mode === 'quote') {
      return '気配なし（#114 で約定から作成した区間）';
    }
    return '約定から作成（#114。IV 再計算・気配なし）' + (g.status === 'partial' ? '・一部欠け' : '');
  }

  function oneText(g, mode) {
    return (KINDS[g.cause] || '不明') + '（' + stateText(g, mode) + '）' + g.period;
  }

  // 凡例（カーソルの足）の区間の説明。複数なら件数と最初の 1 つ
  function legendText(gaps, mode) {
    if (!gaps.length) {
      return '';
    }
    return (gaps.length > 1 ? '区間 ' + gaps.length + ' 件: ' : '') + oneText(gaps[0], mode);
  }

  // 2 つの印の列（それぞれ time 昇順）を time 昇順の 1 つに。同じ time は a が先
  function mergeMarkers(a, b) {
    var out = [];
    var i = 0;
    var j = 0;
    while (i < a.length || j < b.length) {
      if (j >= b.length || (i < a.length && a[i].time <= b[j].time)) {
        out.push(a[i++]);
      } else {
        out.push(b[j++]);
      }
    }
    return out;
  }

  return {
    parse: parse, modeOf: modeOf, spans: spans, assign: assign, overlapping: overlapping, legendText: legendText,
    mergeMarkers: mergeMarkers,
  };
})();

if (typeof module !== 'undefined' && module.exports) {
  module.exports = DataGaps;
}
