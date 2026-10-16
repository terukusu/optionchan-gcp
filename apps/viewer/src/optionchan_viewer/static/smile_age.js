// 約定 IV の鮮度の規則（#82）。純粋関数だけ（DOM・Chart.js に触らない）。ブラウザでは global の SmileAge、
// node（tests/js/smile_age.test.js）では module.exports。時刻はどちらも UTC の UNIX 秒。
// - 経過 = スナップショット時刻 − price_time（秒）。price_time が無い・スナップショットより後（日付ずれ #4 の名残等）は null
// - 段: 経過が区切り以下（「以内」）の最初の段。null は最後の段（それより前・時刻不明）
// - 「何分前」: 分は切り捨て。1 時間以上は「H時間M分前」、24 時間以上は「D日H時間M分前」
var SmileAge = (function() {
  var TIERS = [
    { max: 5 * 60, label: '5分以内' },
    { max: 30 * 60, label: '30分以内' },
    { max: 2 * 3600, label: '2時間以内' },
    { max: Infinity, label: '2時間より前・時刻不明' },
  ];

  function ageSeconds(snapshot, priceTime) {
    if (priceTime === null || priceTime === undefined || snapshot === null || snapshot === undefined) {
      return null;
    }
    var age = snapshot - priceTime;
    return age >= 0 ? age : null;
  }

  function tierOf(age) {
    if (age === null) {
      return TIERS.length - 1;
    }
    for (var i = 0; i < TIERS.length; i++) {
      if (age <= TIERS[i].max) {
        return i;
      }
    }
    return TIERS.length - 1;
  }

  function agoText(age) {
    if (age === null) {
      return null;
    }
    var minutes = Math.floor(age / 60);
    if (minutes < 60) {
      return minutes + '分前';
    }
    var days = Math.floor(minutes / 1440);
    var hours = Math.floor((minutes % 1440) / 60);
    var text = hours + '時間' + (minutes % 60) + '分前';
    return days > 0 ? days + '日' + text : text;
  }

  return { TIERS: TIERS, ageSeconds: ageSeconds, tierOf: tierOf, agoText: agoText };
})();

if (typeof module !== 'undefined' && module.exports) {
  module.exports = SmileAge;
}
