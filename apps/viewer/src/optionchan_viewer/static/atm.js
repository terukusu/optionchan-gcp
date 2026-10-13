// ATM オプションパラメータ推移（Chart.js 4。系列・色・軸・タイトルは appengine/static/atm.js と同じ）
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

// 1 行目（メタ行）はセルが 1 個なので csv2Array では捨てられる。更新時刻は先頭行から読む
function drawChart(text) {
  var updatedAt = new Date(Number(text.split("\n")[0]) * 1000);
  var data = csv2Array(text);
  var labels = [], targetPrice = [], iv = [], price = [];

  for (var row of data) {
    labels.push(Number(row[0]));
    targetPrice.push(num(row[1]));
    iv.push(num(row[2]));
    price.push(num(row[3]));
  }

  document.getElementById("loading").style.display = "none";
  var ctx = document.getElementById("chartCanvas").getContext("2d");
  new Chart(ctx, {
    type: 'line',
    data: {
      labels: labels,
      datasets: [
        series("ATM権利行使価格", targetPrice, "red", "y-axis-1"),
        series("ATM-PUTのIV", iv, "blue", "y-axis-2"),
        series("ATM-PUTの値", price, "green", "y-axis-3"),
      ]
    },
    options: {
      responsive: true,
      plugins: {
        title: {
          display: true,
          text: "ATMオプションパラメータ推移 (" + (updatedAt.getMonth() + 1) + '/' + updatedAt.getDate() + ' '
                + updatedAt.getHours() + ':' + ("0" + updatedAt.getMinutes()).slice(-2) + " 更新)",
        },
      },
      scales: {
        x: {
          ticks: {
            autoSkip: false,
            callback: function(value) {
              var d = new Date(this.getLabelForValue(value) * 1000);
              var xLabel = ("0" + (d.getMonth() + 1)).slice(-2) + '/' + ("0" + d.getDate()).slice(-2) + ' '
                + ("0" + d.getHours()).slice(-2) + ':' + ("0" + d.getMinutes()).slice(-2);
              var m = xLabel.match(/ ([0-9]+):00/);
              return (m && m[1] % 2 == 0) ? xLabel : "";
            },
          },
          grid: {
            drawOnChartArea: false,
            drawTicks: false,
          },
        },
        "y-axis-1": {
          type: "linear",
          position: "left",
        },
        "y-axis-2": {
          type: "linear",
          position: "right",
          grid: {
            drawOnChartArea: false,
          },
        },
        "y-axis-3": {
          type: "linear",
          position: "right",
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
  req.open("GET", '/atm_data', true);
  req.onload = function() {
    if (req.status !== 200) {
      showError(req.status);
      return;
    }
    drawChart(req.responseText);
  };
  req.send(null);
}

main();
