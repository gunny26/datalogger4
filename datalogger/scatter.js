$(function () {
    $('#container').highcharts({
        chart: {
            type: 'scatter',
            zoomType: 'xy'
        },
        title: {
            text: 'Height Versus Weight of 507 Individuals by Gender'
        },
        subtitle: {
            text: 'Source: Heinz  2003'
        },
        xAxis: {
            title: {
                enabled: true,
                text: 'Height (cm)'
            },
            startOnTick: true,
            endOnTick: true,
            showLastLabel: true
        },
        yAxis: {
            title: {
                text: 'Weight (kg)'
            }
        },
        plotOptions: {
            scatter: {
                marker: {
                    radius: 5,
                    states: {
                        hover: {
                            enabled: true,
                            lineColor: 'rgb(100,100,100)'
                        }
                    }
                },
                states: {
                    hover: {
                        marker: {
                            enabled: false
                        }
                    }
                },
                tooltip: {
                    headerFormat: '<b>{series.name}</b><br>',
                    pointFormat: '{point.x} cm, {point.y} kg'
                }
            }
        },
        series: [{
            "data": [
                [43726.902787950305, 25608.555536905926]
            ],
            "name": "fca-sr1-19bc"
        }, {
            "data": [
                [51.035499798754849, 92.905291556070253]
            ],
            "name": "fcb-sr1-12bc"
        }, {
            "data": [
                [1551.719907866584, 1739.0908559163411]
            ],
            "name": "fcb-sr2-28bc"
        }, {
            "data": [
                [28971.057895236547, 16331.120363023547]
            ],
            "name": "fcb-sr2-08bc"
        }, {
            "data": [
                [338.60185379121037, 213.50000068214206]
            ],
            "name": "fca-sr1-17bc"
        }, {
            "data": [
                [6681.2378135257295, 4857.2844306098086]
            ],
            "name": "fca-sr1-03bc"
        }, {
            "data": [
                [1688.6045464409722, 1332.209144698249]
            ],
            "name": "fcb-sr2-14bc"
        }, {
            "data": [
                [164359.57564290366, 158397.07630750869]
            ],
            "name": "fcb-sr2-8gb-22"
        }, {
            "data": [
                [1537.0925969017876, 1732.2135439978706]
            ],
            "name": "fca-sr2-27bc"
        }, {
            "data": [
                [2601.1626148223877, 2281.9878474341499]
            ],
            "name": "fcb-sr2-24bc"
        }, {
            "data": [
                [187904.40738932291, 198801.08452690972]
            ],
            "name": "fcb-sr1-8gb-12"
        }, {
            "data": [
                [28974.840277777777, 16333.499994913736]
            ],
            "name": "fca-sr2-07bc"
        }, {
            "data": [
                [186735.49297417534, 196878.8598361545]
            ],
            "name": "fca-sr1-8gb-11"
        }, {
            "data": [
                [2579.0578602684868, 1899.0798563427395]
            ],
            "name": "fcb-sr1-10bc"
        }, {
            "data": [
                [3372.4918963114419, 2842.2997656928169]
            ],
            "name": "fcb-sr1-04bc"
        }, {
            "data": [
                [2562.9493659337363, 2266.122106552124]
            ],
            "name": "fca-sr2-23bc"
        }, {
            "data": [
                [156407.85662163628, 149959.43031141494]
            ],
            "name": "fca-sr2-8gb-21"
        }, {
            "data": [
                [44565.467536078562, 26611.944464789496]
            ],
            "name": "fcb-sr1-20bc"
        }, {
            "data": [
                [2613.2476666768393, 1912.7002245585124]
            ],
            "name": "fca-sr1-09bc"
        }, {
            "data": [
                [327.49074017670421, 205.51851881543794]
            ],
            "name": "fcb-sr1-18bc"
        }, {
            "data": [
                [1695.4259300231934, 1340.5925910737778]
            ],
            "name": "fca-sr2-13bc"
        }, {
            "data": [
                [50.871147966219318, 92.623607754293417]
            ],
            "name": "fca-sr1-11bc"
        }]
    });
});
