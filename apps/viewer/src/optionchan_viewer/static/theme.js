// 画面の配色（ダーク / ライト）。既定はダーク。選んだものはブラウザごとに覚える（localStorage。読めないときは既定）。
// - 各ページの <head> で CSS より前に（defer なしで）読み、<html data-theme> を描く前に決める（ライトの一瞬の表示を出さない）
// - CSS（viewer.css・home.css）は data-theme で色の変数を切り替える。属性が無い（JS が動かない）ときもダーク
// - チャートの canvas の色は各ページの JS が ViewerTheme.get() で選び、ViewerTheme.onChange で塗り直す
// - 切り替えのボタンは #theme-toggle（あるページだけ）。ブラウザでは global の ViewerTheme、node（tests/js/theme.test.js）では
//   module.exports（純粋関数の normalize だけを試す）
var ViewerTheme = (function() {
  var KEY = 'optionchan-theme';
  var DEFAULT = 'dark';
  var listeners = [];

  // 覚えている値 → テーマ（知らない値・null は既定）
  function normalize(v) {
    return v === 'light' || v === 'dark' ? v : DEFAULT;
  }

  function load() {
    try {
      return normalize(window.localStorage.getItem(KEY));
    } catch (e) {
      return DEFAULT;
    }
  }

  function apply(t) {
    document.documentElement.setAttribute('data-theme', t);
  }

  function get() {
    return normalize(document.documentElement.getAttribute('data-theme'));
  }

  function set(t) {
    t = normalize(t);
    apply(t);
    try {
      window.localStorage.setItem(KEY, t);
    } catch (e) {
      // 覚えられなくても、このページの表示は切り替える
    }
    listeners.forEach(function(fn) { fn(t); });
  }

  function onChange(fn) {
    listeners.push(fn);
  }

  function bindToggle() {
    var b = document.getElementById('theme-toggle');
    if (!b) {
      return;
    }
    // ボタンには押すと切り替わる先を出す
    function label() {
      b.textContent = get() === 'dark' ? 'ライト表示' : 'ダーク表示';
    }
    b.addEventListener('click', function() {
      set(get() === 'dark' ? 'light' : 'dark');
    });
    onChange(label);
    label();
  }

  if (typeof document !== 'undefined') {
    apply(load());
    document.addEventListener('DOMContentLoaded', bindToggle);
  }

  return { DEFAULT: DEFAULT, normalize: normalize, get: get, set: set, onChange: onChange };
})();

if (typeof module !== 'undefined' && module.exports) {
  module.exports = ViewerTheme;
}
