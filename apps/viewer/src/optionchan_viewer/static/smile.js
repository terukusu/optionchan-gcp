// スマイルカーブ（Chart.js 4。系列・色・軸・タイトルは appengine/static/smile.js と同じ）
// CSV から 2 次元配列に変換（空セルは null、セルが 1 個以下の行は捨てる）
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

function series(label, data, color, axis) {
  return { label: label, data: data, borderColor: color, backgroundColor: color, fill: false, tension: 0,
           borderWidth: 1, pointRadius: 0, spanGaps: false, yAxisID: axis };
}

function drawChart(data) {
  var labels = [], cols = [[], [], [], [], [], [], [], [], []];

  var meta = data.shift();
  var updatedAt = new Date(meta[0] * 1000);
  var atm = meta[1];

  for (var row of data) {
    labels.push(Number(row[0])); // target_price
    for (var c = 1; c <= 8; c++) {
      cols[c].push(num(row[c]));
    }
  }
  // 1: o1_call_iv, 2: o1_call_price_time, 3: o1_put_iv, 4: o1_put_price_time,
  // 5: o2_call_iv, 6: o2_call_price_time, 7: o2_put_iv, 8: o2_put_price_time

  // 準備ができたのでローディングくるくるを消す
  document.getElementById("loading").style.display = "none";

  var ctx = document.getElementById("chartCanvas").getContext("2d");
  new Chart(ctx, {
    type: 'line',
    data: {
      labels: labels,
      datasets: [
        series("1限月PUT IV", cols[3], "blue", "y-axis-1"),
        series("1限月CALL IV", cols[1], "red", "y-axis-1"),
        series("2限月PUT IV", cols[7], "#87CEEB", "y-axis-1"),
        series("2限月CALL IV", cols[5], "#FFB6C1", "y-axis-1"),
        series("1限月PUT取引時刻", cols[4], "purple", "y-axis-2"),
        series("1限月CALL取引時刻", cols[2], "green", "y-axis-2"),
        series("2限月PUT取引時刻", cols[8], "#D8BfD8", "y-axis-2"),
        series("2限月CALL取引時刻", cols[6], "#9FCC9F", "y-axis-2"),
      ]
    },
    options: {
      responsive: true,
      plugins: {
        title: {
          display: true,
          text: "スマイルカーブ(" + (updatedAt.getMonth() + 1) + '/' + updatedAt.getDate() + ' '
                + updatedAt.getHours() + ':' + ("0" + updatedAt.getMinutes()).slice(-2) + ") ATM = " + atm,
        },
      },
      scales: {
        x: {
          ticks: {
            callback: function(value) {
              var label = this.getLabelForValue(value);
              return (label % 250 == 0) ? label : '';
            }
          }
        },
        "y-axis-1": {
          type: "linear",
          position: "left",
        },
        "y-axis-2": {
          type: "linear",
          position: "right",
          ticks: {
            callback: function(v) {
              var d = new Date(v * 1000);
              return ("0" + d.getDate()).slice(-2) + '日'
                + ("0" + d.getHours()).slice(-2) + ':' + ("0" + d.getMinutes()).slice(-2);
            },
          },
          grid: {
            drawOnChartArea: false,
          },
        },
      }
    }
  });
}

function main() {
  var req = new XMLHttpRequest();
  req.open("GET", '/smile_data', true);
  req.onload = function() {
    if (req.status !== 200) {
      showError(req.status);
      return;
    }
    drawChart(csv2Array(req.responseText));
  };
  req.send(null);
}

main();
