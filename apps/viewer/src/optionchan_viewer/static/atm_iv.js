// ATM IV の 1 時間足（Chart.js 4 + chartjs-chart-financial 0.2 + Luxon アダプタ。構成は appengine/static/atm_iv.js と同じ）
var chart = null;
var NEON = { up: '#01ff01', down: '#fe0000', unchanged: '#999' };
// chartjs-chart-financial 0.1 の枠線の既定色（Border: Yes）
var BORDER = 'rgba(90, 90, 90, 1)';

// 取得失敗: ローディング（くるくる）を隠して、別の要素にステータスを出す（ローディングの中身は書き換えない）
function showError(status) {
  document.getElementById("loading").style.display = "none";
  document.getElementById("message").textContent = "データを取得できませんでした (" + status + ")";
}

function csv2Array(str) {
  var csvData = [];
  var lines = str.split("\n");
  for (var i = 0; i < lines.length; i++) {
    var cells = lines[i].split(",").map(x => x !== '' ? x : null);
    if (cells.length > 1) {
      csvData.push(cells);
    }
  }
  return csvData;
}

function num(x) {
  return x !== null ? Number(x) : null;
}

// 画面の選択（Bar Type・Color Scheme・Border）からデータセットを作る
function dataset(data) {
  var type = document.getElementById('type').value;
  var neon = document.getElementById('color-scheme').value === 'neon';
  var border = document.getElementById('border').value === 'true';
  var defaults = Chart.defaults.elements[type];
  var fill = neon ? NEON : (defaults.backgroundColors || defaults.borderColors);
  var ds = { type: type, label: 'iv', data: data };
  if (type === 'candlestick') {
    ds.backgroundColors = fill;
    // 枠線なし = 塗りと同じ色（0.1 の Border: No と同じ見た目）
    ds.borderColors = border ? BORDER : fill;
  } else {
    ds.borderColors = neon ? NEON : defaults.borderColors;
  }
  return ds;
}

function drawChart(data) {
  document.getElementById("loading").style.display = "none";
  document.getElementById("control").style.display = "block";

  if (chart) {
    chart.destroy();
  }
  var ctx = document.getElementById('chartCanvas').getContext('2d');
  chart = new Chart(ctx, {
    type: document.getElementById('type').value,
    data: {
      datasets: [dataset(data)]
    },
    options: {
      responsive: true,
      plugins: {
        title: {
          display: true,
          text: "ATMオプションIV推移"
        },
      },
      scales: {
        x: {
          time: {
            unit: 'hour',
            displayFormats: {
              hour: 'M/d H:00'
            }
          },
          ticks: {
            autoSkip: true,
          },
          grid: {
            drawOnChartArea: false,
            drawTicks: false
          }
        },
        y: {
          display: true,
          // chartjs-chart-financial 0.2 の既定は 0 から。現行（0.1）と同じく 13 付近から
          beginAtZero: false,
          suggestedMin: 13
        }
      }
    }
  });
}

function getCsvData(days, nmonth, callback) {
  if (Number(days) > 100) {
    // 上限（サーバー側も 1〜100 だけを受け付ける）
    return;
  }
  var req = new XMLHttpRequest();
  req.open("GET", '/atm_iv_data?d=' + encodeURIComponent(days) + '&n=' + encodeURIComponent(nmonth), true);
  req.onload = function() {
    if (req.status !== 200) {
      showError(req.status);
      return;
    }
    // 値の無い足（iv が全部 NULL の 1 時間）は描かない（chartjs-chart-financial 0.2 は null を 0 として軸の範囲に入れるため）
    var rows = csv2Array(req.responseText).filter(function(row) {
      return row[1] !== null && row[2] !== null;
    });
    var ohlcData = rows.map(function(row) {
      return {
        o: num(row[0]),
        h: num(row[1]),
        l: num(row[2]),
        c: num(row[3]),
        // started_at は "YYYY-MM-DD HH:MM:SS+00:00"。ISO 8601 の形にしてから読む
        x: Date.parse(row[4].replace(' ', 'T')),
      };
    });
    callback(ohlcData);
  };
  req.send(null);
}

function update() {
  if (!chart) {
    return;
  }
  document.getElementById("message").textContent = "";
  document.getElementById("loading").style.display = "block";
  var days = document.getElementById('days').value;
  var nmonth = document.getElementById('nmonth').value;
  getCsvData(days, nmonth, drawChart);
}

document.getElementById('update').addEventListener('click', update);

getCsvData('20', '0', drawChart);
