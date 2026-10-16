// スマイルカーブのタッチ操作の規則（#84・#86）。純粋関数だけ（DOM・Chart.js に触らない）。ブラウザでは global の SmileTouch、
// node（tests/js/smile_touch.test.js）では module.exports。px は canvas の左端からの CSS px、値は行使価格（円）。
// - 1 本指（#86）: SLOP_PX 動かさずに LONG_PRESS_MS 押し続けたら長押し（選択モード）。それより前に SLOP_PX 動いたら、
//   横（|dx| ≥ |dy|）なら表示範囲のパン、縦ならページのスクロール。動かさずに離せばタップ
// - 選択モードのなぞり: 指の位置に一番近い行使価格に吸い付く。描画領域の端から EDGE_PX の帯に入ったら表示範囲を送る
//   （端に近いほど速く、最大 EDGE_RATE × 表示幅 / 秒）。データの範囲の端で止まる
var SmileTouch = (function() {
  var SLOP_PX = 8;  // 長押しの取り消し・パンか縦かの判定の移動量（Android の touch slop 8dp と同じ）
  var LONG_PRESS_MS = 400;  // 長押しの時間（Android の長押しの既定 400ms と同じ）
  var EDGE_PX = 32;
  var EDGE_RATE = 0.6;

  function classify(dx, dy, slop) {
    var ax = Math.abs(dx);
    var ay = Math.abs(dy);
    if (Math.max(ax, ay) < slop) {
      return 'pending';
    }
    return ax >= ay ? 'pan' : 'scroll';
  }

  // 押している指の判定: 押した位置からの移動 (dx, dy)、押してからの時間（ms）。
  // 時間を先に見る（まだ判定していない = それまでの移動は SLOP_PX 未満なので、時間が来ていれば長押し）
  function press(dx, dy, elapsedMs) {
    if (elapsedMs >= LONG_PRESS_MS) {
      return 'select';
    }
    return classify(dx, dy, SLOP_PX);
  }

  // 一番近い値（同じ距離なら先の値）。空なら null
  function nearest(strikes, x) {
    var best = null;
    for (var i = 0; i < strikes.length; i++) {
      if (best === null || Math.abs(strikes[i] - x) < Math.abs(best - x)) {
        best = strikes[i];
      }
    }
    return best;
  }

  function clampPx(px, left, right) {
    return Math.min(Math.max(px, left), right);
  }

  function valueAt(px, left, right, view) {
    return view.min + (px - left) / (right - left) * (view.max - view.min);
  }

  // 端の帯での送りの向きと強さ: 左は負・右は正（-1〜1）。帯の外は 0、端より外は ±1
  function edgeFactor(px, left, right, zone) {
    if (px < left + zone) {
      return -Math.min(1, (left + zone - px) / zone);
    }
    if (px > right - zone) {
      return Math.min(1, (px - (right - zone)) / zone);
    }
    return 0;
  }

  // dt 秒で送る値（円）
  function edgeDelta(factor, width, dt) {
    return factor * EDGE_RATE * width * dt;
  }

  // 幅を保って delta だけ送る。bounds（データの範囲）の端で止める（bounds が null なら送らない）
  function shiftView(view, delta, bounds) {
    if (!bounds) {
      return view;
    }
    var width = view.max - view.min;
    // 表示幅がデータの範囲より広い（clampView がそうはしない）ときは送らない
    if (bounds.max - bounds.min < width) {
      return view;
    }
    var min = Math.max(bounds.min, Math.min(view.min + delta, bounds.max - width));
    return min === view.min ? view : { min: min, max: min + width };
  }

  return { SLOP_PX: SLOP_PX, LONG_PRESS_MS: LONG_PRESS_MS, EDGE_PX: EDGE_PX, EDGE_RATE: EDGE_RATE,
    classify: classify, press: press, nearest: nearest,
    clampPx: clampPx, valueAt: valueAt, edgeFactor: edgeFactor, edgeDelta: edgeDelta, shiftView: shiftView };
})();

if (typeof module !== 'undefined' && module.exports) {
  module.exports = SmileTouch;
}
