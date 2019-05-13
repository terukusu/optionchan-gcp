var chart = null;

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
    document.getElementById("loading").style.display = "none";
    document.getElementById("control").style.display = "block";

    var ctx = document.getElementById('chartCanvas').getContext('2d');

    chart = new Chart(ctx, {
        type: 'candlestick',
        data: {
            datasets: [{
                label: 'iv',
                data: data,
                spanGaps: false
            }]
        },
        options: {
            responsive: true,
            title:{
                display:true,
                text: "ATMオプションIV推移"
            },
            scales: {
                xAxes: [{
                    time: {
                        unit: 'hour',
                        displayFormats: {
                                hour: 'M/d H:00'
                        }
                    },
                    ticks: {
                        autoSkip: true,
                    },
                    gridLines: {
                        drawOnChartArea: false,
                        drawTicks: false
                    }
                }],
                yAxes: [{
                    display: true,
                    ticks: {
                        suggestedMin: 13
                    }
                }]
            }
        }
    });
}

var update = function() {
    if (!chart) {
        return;
    }

	var dataset = chart.config.data.datasets[0];

	// candlestick vs ohlc
	var type = document.getElementById('type').value;
	dataset.type = type;

	// color
	var colorScheme = document.getElementById('color-scheme').value;
	if (colorScheme === 'neon') {
		dataset.color = {
			up: '#01ff01',
			down: '#fe0000',
			unchanged: '#999',
		};
	} else {
		delete dataset.color;
	}

	// border
	var border = document.getElementById('border').value;
	var defaultOpts = Chart.defaults.global.elements[type];
	if (border === 'true') {
		dataset.borderColor = defaultOpts.borderColor;
	} else {
		dataset.borderColor = {
			up: defaultOpts.color.up,
			down: defaultOpts.color.down,
			unchanged: defaultOpts.color.up
		};
	}

    document.getElementById("loading").style.display = "block";

	var num_days = document.getElementById('days').value;

    getCsvData(num_days, function(ohlcData) {
    document.getElementById("loading").style.display = "none";
        chart.data.datasets[0].data = ohlcData;
    	chart.update();
    });

};


function getCsvData(days='10', callback) {
    var req = new XMLHttpRequest();
    var filePath = '/atm_iv_data?d=' + days;

    if (Number(days) > 100) {
        // upper limit
        return;
    }

    req.open("GET", filePath, true);
    req.onload = function() {
        var rawData = csv2Array(req.responseText);
        var ohlcData = rawData.map(function(row) {
            return {
                o: row[0] !== null ? Number(row[0]) : null,
                h: row[1] !== null ? Number(row[1]) : null,
                l: row[2] !== null ? Number(row[2]) : null,
                c: row[3] !== null ? Number(row[3]) : null,
                t: Date.parse(row[4]),
            };
        });

        callback(ohlcData);
    };

    req.send(null);
}

function main() {
    getCsvData(20,function(ohlcData) {
        drawChart(ohlcData);
    });
}

document.getElementById('update').addEventListener('click', update);

main();
