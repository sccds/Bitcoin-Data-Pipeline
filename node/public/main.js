$(function() {
    var data_points = [];
    data_points.push({values: [], key: 'BTC-USD'});
    //data_points.push({values: [], key: 'LTC-USD'});
    //data_points.push({values: [], key: 'ETH-USD'});

    $('#chart').height($(window).height() - $('#header').height() * 2);

    /*
    // line chart
    var chart = nv.models.lineChart()
                .interpolate('monotone')
                .margin({botton: 100})
                .useInteractiveGuideline(true)
                .showLegend(true)
                .color(d3.scale.category10().range());
    chart.xAxis
        .axisLabel('Time')
        .tickFormat(formatDateTick);
    chart.yAxis
        .axisLabel('Price');
    */


    // candlestick bar chart
    var chart = nv.models.candlestickBarChart()
                .margin({botton: 100})
                .useInteractiveGuideline(true)
                .showLegend(true)
                .color(d3.scale.category10().range());
    chart.xAxis
        .axisLabel('Time')
        .tickFormat(formatDateTick);
    chart.yAxis
        .axisLabel('Price');


    nv.addGraph(loadGraph);


    function loadGraph() {
        d3.select('#chart svg')
            .datum(data_points)
            .transition()
            .duration(5)
            .call(chart);
        nv.utils.windowResize(chart.update);
        return chart;
    }


    function formatDateTick(time) {
        var date = new Date(time);
        //console.log(time);
        return d3.time.format('%H:%M:%S')(date);
    }


    function newDataCallback(message) {
        var parsed = JSON.parse(message);
        var timestamp = parsed['timestamp'];
        var openP = parsed['open'];
        var closeP = parsed['close'];
        var highP = parsed['high'];
        var lowP = parsed['low'];
        var avgP = parsed['average'];
        var symbol = parsed['symbol'];
        var point = {};
        point.x = timestamp;
        point.y = avgP;
        point.open = openP;
        point.close = closeP;
        point.high = highP;
        point.low = lowP;

        var i = getSymbolIndex(symbol, data_points);
        //console.log("symbol: %s, i: %s, ts: %s, price: %s", symbol, i, point.x, point.y);
        data_points[i].values.push(point);
        if (data_points[i].values.length > 100) {
            data_points[i].values.shift();
        }
        loadGraph();
    }


    function getSymbolIndex(symbol, arr) {
        for (var i = 0; i < arr.length; i++) {
            if (arr[i].key == symbol) {
                return i;
            }
        }
        return -1;
    }

    var socket = io();
    socket.on('data', function(data) {
        newDataCallback(data);
    });
});