// /smile の注記の文と横の表示範囲（#127）。純粋関数だけ（DOM・Chart.js に触らない）。ブラウザでは global の SmileView、
// node（tests/js/smile_view.test.js）では module.exports。スナップショットは SmileCsv.parseSnapshot の値（null は無い時点）
// - 注記: #114 で約定から作ったスナップショット（source = fill114。#123）と、気配の無い時点（取引時間外）の文。
//   画面を見る人向けの言葉にし、Issue 番号は出さない
// - 横の範囲: bounds は表示・固定の時点の全行の行使価格 + margin（パン・ピンチ・端の送りの上限）。初めの範囲は、描いている
//   限月・表示の、値のある行の行使価格の最小〜最大 + margin（スマイルの全体。外れ値は除かない）
var SmileView = (function() {
  var SOURCE_NOTE = 'この時点は取得できなかったため、別に購入した約定データから作り直しています。' +
    '売り気配・買い気配は無く、点（約定の IV）だけを表示しています';
  var NO_QUOTE_NOTE = '気配がありません（取引時間外）。約定の IV の点だけを表示しています';
  var VALUES = ['iv', 'mid_iv', 'bid_iv', 'ask_iv'];

  // 時点の注記の文（無ければ null）。fill114 は気配が無い理由が分かっているので取引時間外の文は出さない
  function snapshotNote(snap) {
    if (!snap) {
      return null;
    }
    if (snap.source === 'fill114') {
      return SOURCE_NOTE;
    }
    return snap.hasQuote ? null : NO_QUOTE_NOTE;
  }

  function margin(lo, hi) {
    return Math.max(250, (hi - lo) * 0.02);
  }

  function withMargin(lo, hi) {
    var m = margin(lo, hi);
    return { min: lo - m, max: hi + m };
  }

  // 表示・固定の時点の全行の行使価格の範囲 + margin（無ければ null）
  function bounds(snaps) {
    var lo = null;
    var hi = null;
    snaps.forEach(function(s) {
      if (s && s.lo !== null) {
        lo = lo === null ? s.lo : Math.min(lo, s.lo);
        hi = hi === null ? s.hi : Math.max(hi, s.hi);
      }
    });
    return lo === null ? null : withMargin(lo, hi);
  }

  // 幅を [minWidth, b の幅] に収め、b の中へ寄せる（b が null ならそのまま）
  function clamp(min, max, b, minWidth) {
    if (!b) {
      return { min: min, max: max };
    }
    var width = Math.min(Math.max(max - min, minWidth), b.max - b.min);
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

  // 描いている行（months の限月、otm なら otm=1 の行、iv・mid_iv・bid_iv・ask_iv のどれかがある行）の行使価格の
  // { lo, hi }（無ければ null）
  function drawnRange(snaps, months, otm) {
    var lo = null;
    var hi = null;
    snaps.forEach(function(s) {
      if (!s) {
        return;
      }
      months.forEach(function(m) {
        (s.byMonth[m] || []).forEach(function(r) {
          if (otm && r.otm !== 1) {
            return;
          }
          var drawn = VALUES.some(function(c) { return r[c] !== null; });
          if (!drawn) {
            return;
          }
          lo = lo === null ? r.target_price : Math.min(lo, r.target_price);
          hi = hi === null ? r.target_price : Math.max(hi, r.target_price);
        });
      });
    });
    return lo === null ? null : { lo: lo, hi: hi };
  }

  // 初めの範囲（view が未定のとき・表示のリセット）。描ける行が無ければ bounds、時点が無ければ null
  function initialView(snaps, months, otm, minWidth) {
    var b = bounds(snaps);
    if (!b) {
      return null;
    }
    var r = drawnRange(snaps, months, otm);
    if (!r) {
      return b;
    }
    var w = withMargin(r.lo, r.hi);
    return clamp(w.min, w.max, b, minWidth);
  }

  return {
    SOURCE_NOTE: SOURCE_NOTE, NO_QUOTE_NOTE: NO_QUOTE_NOTE, snapshotNote: snapshotNote, margin: margin, bounds: bounds,
    clamp: clamp, drawnRange: drawnRange, initialView: initialView,
  };
})();

if (typeof module !== 'undefined' && module.exports) {
  module.exports = SmileView;
}
