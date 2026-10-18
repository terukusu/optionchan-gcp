// /smile の CSV（/smile_snapshot、#79）の読み取り。純粋関数だけ（DOM・Chart.js に触らない）。ブラウザでは global の SmileCsv、
// node（tests/js/smile_csv.test.js）では module.exports。時刻は UTC の UNIX 秒。
// - 1 行目: スナップショットの UNIX 秒, 1 限月の最終取引日, ATM, 2 限月の最終取引日, ATM, source（#123。#114 で DataCube の
//   歩み値から作ったスナップショットなら fill114、それ以外は空）の 6 列。それ以外の形はエラー（想定の CSV でない応答）
// - 2 行目以降: COLUMNS（限月 × 行使価格 × call / put の縦持ち。列の数が違う行は飛ばす）
var SmileCsv = (function() {
  var COLUMNS = ['month', 'type', 'target_price', 'iv', 'price', 'price_time', 'bid_iv', 'ask_iv', 'bid', 'ask', 'mid_iv',
    'otm'];

  function num(x) {
    return x === '' || x === undefined ? null : Number(x);
  }

  // /smile_snapshot の CSV → { s, source, months: {1: {ltd, atm}, 2: ...}, byMonth: {1: [行], 2: [行]}, hasQuote, lo, hi }。
  // 本文が空（データなし）なら null
  function parseSnapshot(text) {
    var lines = text.split('\n').filter(function(line) { return line !== ''; });
    if (!lines.length) {
      return null;
    }
    var meta = lines[0].split(',');
    // 想定の CSV でない応答（HTML 等）・前の形（5 列）はエラーにする（snapshots に NaN の時刻を入れない）
    if (meta.length !== 6 || !/^[0-9]+$/.test(meta[0])) {
      throw new Error('invalid response');
    }
    var snap = {
      s: Number(meta[0]),
      source: meta[5] || null,
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

  return { COLUMNS: COLUMNS, num: num, parseSnapshot: parseSnapshot };
})();

if (typeof module !== 'undefined' && module.exports) {
  module.exports = SmileCsv;
}
