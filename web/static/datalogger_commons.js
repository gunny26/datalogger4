// base url for datalogger JSOn calls
var base_url = "http://srvmgdata1.tilak.cc/DataLogger";
// actual container reference to draw to
var actual_container_num = 0;
var actual_container;

/*
get a list of available projects
*/
function get_projects() {
    var url = base_url + "/get_projects/"
    console.log("Getting Data from url " + url);
    preselect = $("#project").val();
    $("#project").empty();
    $.getJSON(url).then(function(data) {
        $('body').css('cursor','wait');
        data.forEach(function(rowdata) {
            console.log("appending "+rowdata+" to project select");
            $("#project").append("<option value=" + rowdata + ">" + rowdata + "</option>");
        });
        console.log("preselecting " + preselect);
        $("#project").val(preselect);
        $('body').css('cursor','default');
    });
    $( "#project").change(function() {
        alert( "Handler for .change() called." );
        get_tablenames($("#project").val());
    });
}

/*
get a list of available tablenames for this project
project is required
*/
function get_tablenames(project) {
    //var project = $("#project").val();
    $("#tablename").empty();
    var url = base_url + "/get_tablenames/" + project
    console.log("Getting Data from url " + url);
    var ret_data = [];
    $.getJSON(url).then(function(data) {
        $('body').css('cursor','wait');
        data.forEach( function(rowdata) {
            $("#tablename").append("<option value=" + rowdata + ">" + rowdata + "</option>");
            ret_data.push(rowdata);
        });
        $('body').css('cursor','default');
    });
    $( "#tablename").change(function() {
        alert("Handler for .change() called.");
        get_index_keynames($("#project").val(), $("#tablename").val());
        get_value_keynames($("#project").val(), $("#tablename").val());
        //recreate autocomplete for new data
        $("#keys").removeData('autocomplete');
        $("#keys").autocomplete({source: get_ts_caches($("#project").val(), $("#tablename").val(), $("#datestring").val()),});
    });
}


/*
Add new drawing container ad end of graphs container
*/
function add_container() {
    actual_container = "container" + actual_container_num;
    $("#graphs").append('<div id='+ actual_container + ' style="min-width: 310px; height: 400px; margin: 0 auto">container1</div>');
    actual_container_num++;
}

/*
get data from json and create a graph on actual container
*/
function getData() {
    var project = $("#project").val();
    var tablename = $("#tablename").val();
    var datestring = $("#datestring").val();
    var keys = $("#keys").val();
    var selected = $("#value_keynames" ).val();
    var datatype = $("#datatype").val();
    var index_keynames = $("#index_keynames").val()
    if (index_keynames == "") {
        index_keynames = null;
    }
    var url = base_url + "/get_chart_data_ungrouped/" + project + "/" + tablename + "/" + datestring + "/" + keys + "/" + JSON.stringify(selected) + "/" + JSON.stringify(datatype) + "/" + JSON.stringify(index_keynames)
    var series;
    console.log("Getting Data from url " + url);
    //document.body.style.cursor = "wait";
    $.getJSON(url).then(function(data) {
        $('body').css('cursor','wait');
        //console.log(data);
        console.log("Drawing to actual_container = #" + actual_container);
        draw_develop("#" + actual_container, keys, JSON.stringify(selected), data);
        $('body').css('cursor','default');
    });
    //document.body.style.cursor = "default";
}

// Base64 Object
var Base64={_keyStr:"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=",encode:function(e){var t="";var n,r,i,s,o,u,a;var f=0;e=Base64._utf8_encode(e);while(f<e.length){n=e.charCodeAt(f++);r=e.charCodeAt(f++);i=e.charCodeAt(f++);s=n>>2;o=(n&3)<<4|r>>4;u=(r&15)<<2|i>>6;a=i&63;if(isNaN(r)){u=a=64}else if(isNaN(i)){a=64}t=t+this._keyStr.charAt(s)+this._keyStr.charAt(o)+this._keyStr.charAt(u)+this._keyStr.charAt(a)}return t},decode:function(e){var t="";var n,r,i;var s,o,u,a;var f=0;e=e.replace(/[^A-Za-z0-9\+\/\=]/g,"");while(f<e.length){s=this._keyStr.indexOf(e.charAt(f++));o=this._keyStr.indexOf(e.charAt(f++));u=this._keyStr.indexOf(e.charAt(f++));a=this._keyStr.indexOf(e.charAt(f++));n=s<<2|o>>4;r=(o&15)<<4|u>>2;i=(u&3)<<6|a;t=t+String.fromCharCode(n);if(u!=64){t=t+String.fromCharCode(r)}if(a!=64){t=t+String.fromCharCode(i)}}t=Base64._utf8_decode(t);return t},_utf8_encode:function(e){e=e.replace(/\r\n/g,"\n");var t="";for(var n=0;n<e.length;n++){var r=e.charCodeAt(n);if(r<128){t+=String.fromCharCode(r)}else if(r>127&&r<2048){t+=String.fromCharCode(r>>6|192);t+=String.fromCharCode(r&63|128)}else{t+=String.fromCharCode(r>>12|224);t+=String.fromCharCode(r>>6&63|128);t+=String.fromCharCode(r&63|128)}}return t},_utf8_decode:function(e){var t="";var n=0;var r=c1=c2=0;while(n<e.length){r=e.charCodeAt(n);if(r<128){t+=String.fromCharCode(r);n++}else if(r>191&&r<224){c2=e.charCodeAt(n+1);t+=String.fromCharCode((r&31)<<6|c2&63);n+=2}else{c2=e.charCodeAt(n+1);c3=e.charCodeAt(n+2);t+=String.fromCharCode((r&15)<<12|(c2&63)<<6|c3&63);n+=3}}return t}}
// use Base63.encode(<str>) or Base64.decode(<str>)


