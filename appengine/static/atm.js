function csv2Array(str) {
  var csvData = [];
  var lines = str.split("\n");
  for (var i = 0; i < lines.length; i++) {
    var cells = lines[i].split(",").map(x => x !== '' ? x : null);
    if(cells.length > 1) {
        csvData.push(cells);
    }
  }
  return csvData;
}

function drawChart(data) {
  var tmpLabels = [], tmpData1 = [], tmpData2 = [], tmpData3 = [], tmpData4 = [];

  var meta = data.shift()
  var updatedAt = new Date(meta[0] * 1000);

  for (var row in data) {
    tmpLabels.push(data[row][0])
    tmpData1.push(data[row][1])
    tmpData2.push(data[row][2])
    tmpData3.push(data[row][3])
    tmpData4.push(data[row][4])
  };

  document.getElementById("loading").style.display ="none";
  var ctx = document.getElementById("chartCanvas").getContext("2d");
  var myChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels: tmpLabels,
      datasets: [
        { label: "ATM権利行使価格", data: tmpData1, borderColor: "red",
          backgroundColor: "red", fill: false, lineTension: 0,
          borderWidth: 1, pointRadius: 0, spanGaps: false, yAxisID: "y-axis-1"},
        { label: "ATM-PUTのIV", data: tmpData2, borderColor: "blue",
          backgroundColor: "blue", fill: false, lineTension: 0,
          borderWidth: 1, pointRadius: 0, spanGaps: false, yAxisID: "y-axis-2"},
        { label: "ATM-PUTの値", data: tmpData3, borderColor: "green",
          backgroundColor: "green", fill: false, lineTension: 0,
          borderWidth: 1, pointRadius: 0, spanGaps: false, yAxisID: "y-axis-3"},
//        { label: "EUR/USD(AveragingMaster)", data: tmpData4, borderColor: "purple",
//          backgroundColor: "purple", fill: false, lineTension: 0,
//          borderWidth: 1, pointRadius: 0},
      ]
    },
    options: {
        responsive: true,
        title:{
            display:true,
            text: "ATMオプションパラメータ推移 (" + (updatedAt.getMonth() + 1) + '/' + updatedAt.getDate()  +' '
                          + updatedAt.getHours() + ':' + ("0"+updatedAt.getMinutes()).slice(-2) + " 更新)",
        },
        scales: {
            xAxes: [{
                distribution: "linear",
                ticks: {
                    autoSkip: false,
                    callback: function(value) {
                      var d=new Date(value * 1000);
                      var xLabel = ("0" + (d.getMonth() + 1)).slice(-2) + '/' + ("0" + d.getDate()).slice(-2)+' '
                          + ("0" + d.getHours()).slice(-2) + ':' + ("0" + d.getMinutes()).slice(-2);
                      var m =  xLabel.match(/ ([0-9]+):00/);
                      return (m && m[1]%2 == 0) ? xLabel : "";
                    },
                },
                gridLines: {
                    drawOnChartArea: false,
                    drawTicks: false,
                },
            }],
            yAxes: [{
                id: "y-axis-1",
                type: "linear",
                position: "left",
//                ticks: {
//                    max: 0.2,
//                    min: 0,
//                    stepSize: 0.1
//                },
            }, {
                id: "y-axis-2",
                type: "linear",
                position: "right",
//                ticks: {
//                    max: 1.5,
//                    min: 0,
//                    stepSize: .5
//                },
                gridLines: {
                    drawOnChartArea: false,
                },
            },{
                id: "y-axis-3",
                type: "linear",
                position: "right",
//                ticks: {
//                    max: 1.5,
//                    min: 0,
//                    stepSize: .5
//                },
               gridLines: {
                   drawOnChartArea: false,
               },
            }],
        }
    }
  });
}

function main() {
  var req = new XMLHttpRequest();
  var filePath = '/atm_data';
  req.open("GET", filePath, true);
  req.onload = function() {
    var data = csv2Array(req.responseText);
    drawChart(data);
  }
  req.send(null);
}

main();
