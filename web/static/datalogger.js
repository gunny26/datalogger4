/*
 * render tablesorter usable html from json data
 * first row is used as headers
 * add following classes for styling
 *
 * table      : tablesorter
 * even rows  : datalogger-tsastats-table-even-row
 * uneven rows: datalogger-tsastats-table-uneven-row
 * text fields: datalogger-tsastats-table-value-field
 * numeric f. : datalogger-tsastats-table-index-field
 */
function renderTable(data, id) {
    firstRow = true;
    evenRow = false;
    html="<table id=" + id + " class=tablesorter>";
    data.forEach(function(row) {
        /* header fields if this is the first row */
        if (firstRow == true) {
            html += "<thead>";
            html += "<th class=datalogger-tsastats-table-header>" 
            html += row.join("</th><th class=datalogger-tsastats-table-header>") 
            html += "</th>";
            html += "</thead><tbody>";
            firstRow = false;
        } else {
            /* even or uneven row, set according class */
            trClass = "";
            if (evenRow == true) {
                trClass = "datalogger-tsastats-table-even-row";
                evenRow = false;
            } else {
                trClass = "datalogger-tsastats-table-uneven-row";
                evenRow = true;
            }
            html += "<tr class=" + trClass + ">"
            /* join fields according to datatype */
            row.forEach(function(field) {
                if (jQuery.isNumeric(field) == true) {
                    try {
                        html += "<td class=datalogger-tsastats-table-value-field>" + field.toFixed(2) + "</td>";
                    } catch (e) {
                        //seems not to be floating point
                        console.log(e);
                        html += "<td class=datalogger-tsastats-table-value-field>" + field + "</td>";
                    }
                } else {
                    html += "<td class=datalogger-tsastats-table-index-field>" + field + "</td>";
                }
            });
            html += "</tr>"
        }
    });
    html += "</tbody></table>";
    return html;
}
/*
 * render table from TimeseriesStats json data
 * 
 * data comes in object form
 *
 */
function renderTsStatTable(data) {
    html = "<table><tbody>";
    /* identify rownames to put in first columns */
    var rownames = [];
    for ( var rowname in data ) {
        if (data.hasOwnProperty(rowname)) {
            rownames.push(rowname);
        }
    }
    /* build up table row by row */
    var colnames = [];
    rownames.forEach(function(rowname) {
        if (colnames.length == 0) {
            /* only true for first row, build header */
            Object.keys(data[rowname]).forEach(function(colname) {
                colnames.push(colname)
            });
            html += "<tr><th>" + "key</th><th>" + colnames.join("</th><th>") + "</th></tr>";
        }
        /* build value rows */
        var values = [rowname, ];
        colnames.forEach(function(colname) {
            values.push(data[rowname][colname])
        });
        html += "<tr><td>" + values.join("</td><td>") + "</td></tr>";
    });
    return html + "<tbody></table>";
}
/*
 * fill select box with data from backend using JSON AJAX call
 * uiObj - Jquery Select object to fill
 */
function fillIndexKeynames(uiObj) {
    uiObj.empty();
    uiObj.append('<option value=""></option>');
    var url = base_url + "/get_index_keynames/" + $('#project').val() + "/" + $('#tablename').val();
    console.log("getting index_keynames from " + url);
    $.getJSON(url, {}, function(result) {
        result.sort();
        result.forEach(function(item) {
            uiObj.append('<option value=' + item + '>' + item + '</option>');
        });
    });
    /* adding special value __total__ to indicate total grouping of al available values */
    uiObj.append("<option value=__total__>__total__</option>");
}
/*
 * fill select box with data from backend using JSON AJAX call
 * uiObj - Jquery select object to fill
 */
function fillValueKeynames(uiObj) {
    uiObj.empty();
    uiObj.append('<option value=""></option>');
    var url = base_url + "/get_value_keynames/" + $('#project').val() + "/" + $('#tablename').val();
    console.log("getting index_keys from " + url);
    $.getJSON(url, {}, function(result) {
        result.sort();
        result.forEach(function(item) {
            uiObj.append('<option value=' + item + '>' + item + '</option>');
        });
    })
        .fail(function() {
            console.log("fail reached");
            ticker("there was a failure calling backend");
        })
    ;
}   
/*
 * fill autocomplete data for given uiObj
 */
function fillTsAutocomplete(uiObj) {
    var url = base_url + "/get_caches/" +   $('#project').val() + "/" + $('#tablename').val() + "/" + $('#datestring').val()
    console.log("Getting Data from url " + url);
    var index_keys = [];
    $.getJSON(url, {}, function(data) {
        console.log("success reached");
        for (key in data.ts.keys) {
            index_keys.push(key);
        }
        ticker('recreating autocomplete list for ' + $('#project').val() + "/" + $('#tablename').val());
        uiObj.removeData('autocomplete');
        uiObj.autocomplete({source: index_keys});
    })
        .done(function() {
            console.log("done reached");
        })
        .fail(function() {
            console.log("fail reached");
            ticker("there was a failuer calling backend");
        })
        .always(function() {
            console.log("always reached");
        })
}
/*
 * get value keys for this particular project, tablename combination
 */
function fillDatestring(uiObj) {
    var url = base_url + "/get_last_business_day_datestring";
    console.log("getting last businessday datestring from " + url);
    $.getJSON(url, function(result) {
        uiObj.val(result);
    });
}
/*
 * get a list of available projects
 */
function fillProject(uiObj) {
    var url = base_url + '/get_projects/'
    console.log('Getting Data from url ' + url);
    ticker('getting list of projects');
    uiObj.empty();
    uiObj.append('<option value=""></option>')
    $.getJSON(url).then(function(data) {
        $('body').css('cursor','wait');
        data.sort();
        data.forEach(function(rowdata) {
            uiObj.append('<option value=' + rowdata + '>' + rowdata + '</option>');
        });
        $('body').css('cursor','default');
    });
}
/*
 * Generator returns function to output Messages on status line
 * ticker = tickerGenerator($("#something"));
 * ticker("that message went to the status line");
 */
function tickerGenerator(uiObj) {
    return function(message) {
        uiObj.html(message);
    }
}
/*
 * get a list of available tablenames for this project
 * project is required
 */
function fillTablename(uiObj) {
    uiObj.empty();
    uiObj.append('<option value=""></option>');
    var url = base_url + '/get_tablenames/' + $("#project").val()
    ticker('getting list of tablenames for project ' + $("#project").val());
    console.log('Getting Data from url ' + url);
    $.getJSON(url).then(function(data) {
        $('body').css('cursor','wait');
        data.sort();
        data.forEach( function(rowdata) {
            uiObj.append('<option value=' + rowdata + '>' + rowdata + '</option>');
        });
        $('body').css('cursor','default');
    });
}
/*
 * get a list of available statistical function names
 */
function fillStatFuncnames(uiObj) {
    uiObj.empty();
    uiObj.append('<option value=""></option>');
    var url = base_url + '/get_stat_func_names'
    ticker('getting statistical function names');
    console.log('Getting Data from url ' + url);
    $.getJSON(url).then(function(data) {
        $('body').css('cursor','wait');
        data.sort();
        data.forEach( function(rowdata) {
            $('#stat_func_names').append('<option value=' + rowdata + '>' + rowdata + '</option>');
        });
        $('body').css('cursor','default');
    });
}
/*
 * Base64 encoding
 */
var Base64={_keyStr:"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=",encode:function(e){var t="";var n,r,i,s,o,u,a;var f=0;e=Base64._utf8_encode(e);while(f<e.length){n=e.charCodeAt(f++);r=e.charCodeAt(f++);i=e.charCodeAt(f++);s=n>>2;o=(n&3)<<4|r>>4;u=(r&15)<<2|i>>6;a=i&63;if(isNaN(r)){u=a=64}else if(isNaN(i)){a=64}t=t+this._keyStr.charAt(s)+this._keyStr.charAt(o)+this._keyStr.charAt(u)+this._keyStr.charAt(a)}return t},decode:function(e){var t="";var n,r,i;var s,o,u,a;var f=0;e=e.replace(/[^A-Za-z0-9\+\/\=]/g,"");while(f<e.length){s=this._keyStr.indexOf(e.charAt(f++));o=this._keyStr.indexOf(e.charAt(f++));u=this._keyStr.indexOf(e.charAt(f++));a=this._keyStr.indexOf(e.charAt(f++));n=s<<2|o>>4;r=(o&15)<<4|u>>2;i=(u&3)<<6|a;t=t+String.fromCharCode(n);if(u!=64){t=t+String.fromCharCode(r)}if(a!=64){t=t+String.fromCharCode(i)}}t=Base64._utf8_decode(t);return t},_utf8_encode:function(e){e=e.replace(/\r\n/g,"\n");var t="";for(var n=0;n<e.length;n++){var r=e.charCodeAt(n);if(r<128){t+=String.fromCharCode(r)}else if(r>127&&r<2048){t+=String.fromCharCode(r>>6|192);t+=String.fromCharCode(r&63|128)}else{t+=String.fromCharCode(r>>12|224);t+=String.fromCharCode(r>>6&63|128);t+=String.fromCharCode(r&63|128)}}return t},_utf8_decode:function(e){var t="";var n=0;var r=c1=c2=0;while(n<e.length){r=e.charCodeAt(n);if(r<128){t+=String.fromCharCode(r);n++}else if(r>191&&r<224){c2=e.charCodeAt(n+1);t+=String.fromCharCode((r&31)<<6|c2&63);n+=2}else{c2=e.charCodeAt(n+1);c3=e.charCodeAt(n+2);t+=String.fromCharCode((r&15)<<12|(c2&63)<<6|c3&63);n+=3}}return t}}
// use Base64.encode(<str>) or Base64.decode(<str>)

/*
 * Graphics Section
 * special hicharts functions
 */

/*
 *correct timestamps from python
 */
Highcharts.setOptions({
    global: {
        useUTC: true,
        timezoneOffset : -120
    }
});
/*
 * Draw Daily Timeseries Graphics
 */
function drawDailyGraph(canvas, title, data, min, max) {
    //var chart = new Highcharts.Chart({
    $("#" + canvas).highcharts({
        chart: {
            type: 'spline',
            zoomType : 'xy'
        },
        title: {
            text: title 
        },
        xAxis: {
            type: 'datetime',
            dateTimeLabelFormats: { // don't display the dummy year
                month: '%e. %b',
                year: '%b'
            },
            title: {
                text: 'Datetime'
            }
        },
        yAxis: {
            title: {
                text: 'unit according to measured counter'
            },
            min: min,
            max: max
        },
        tooltip: {
            headerFormat: '<b>{series.name}</b><br>',
            pointFormat: '{point.x:%H:%M:%S}: {point.y:.2f} '
        },
        plotOptions: {
            spline: {
                marker: {
                    enabled: false
                }
            }
        },
        series: data
    });
}
/*
 * Daily Scatterplot Graphics 
 */
function drawDailyScatterplot(canvas, title, data, value_key1, value_key2) {
    $("#" + canvas).highcharts({
        chart: {
            type: 'scatter',
            zoomType: 'xy'
        },
        title: {
            text: title
        },
        subtitle: {
            text: 'DataLogger'
        },
        xAxis: {
            title: {
                enabled: true,
                text: value_key1
            },
            startOnTick: true,
            endOnTick: true,
            showLastLabel: true
        },
        yAxis: {
            title: {
                text: value_key2
            }
        },
        legend: {
            enabled : false
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
                    pointFormat: '{point.x}, {point.y}'
                }
            }
        },
        series: data
    });
}
/*
 * Monthly Graphics
 */
function drawMonthlyGraph(canvas, title, data, max) {
    $("#" + canvas).highcharts({
        chart: {
            type: 'spline',
            zoomType : 'xy'
        },
        title: {
            text: title 
        },
        xAxis: {
            type: 'category',
            title: {
                text: 'Date'
            }
        },
        yAxis: {
            title: {
                text: 'unit according to measured counter'
            },
            min: null,
            max: max
        },
        tooltip: {
            headerFormat: '<b>{series.name}</b><br>',
            pointFormat: '{point.x:%H:%M:%S}: {point.y:.2f} '
        },
        plotOptions: {
            spline: {
                marker: {
                    enabled: false
                }
            }
        },
        series: data
    });
}


