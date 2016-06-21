// base url for datalogger JSOn calls
var base_url = "/DataLogger";

$(document).ready(function(){
    // set some meaningful default values
    get_last_business_day_datestring();
    get_stat_func_names();
    // datatype select box
    $("#getData").click(function() {
        getData();
    });
});

function ticker(message) {
    $('#status').html(message);
}

function get_url_key(keyname) {
    console.log("searching for " + keyname + " in url search parameters");
    var searchkeys = $(location).attr('search').slice(1).split("&")
    var retval;
    searchkeys.forEach(function (data) {
        console.log(data);
        var key = data.split("=")[0];
        if (keyname  == key) {
            var value = data.split("=")[1];
            console.log("found searched value : " + value);
            retval = value;
        }
    });
    return(retval);
}

/*
// get value keys for this particular project, tablename combination
*/
function get_last_business_day_datestring() {
    //var value_keynames = $("#value_keynames");
    var url = base_url + "/get_last_business_day_datestring";
    console.log("getting last businessday datestring from " + url);
    $.getJSON(url, function(result) {
        console.log("Last Businessday datestring: " + result);
        $("#datestring").val(result);
    });
    get_projects();
}

/*
get a list of available projects
*/
function get_projects() {
    var url = base_url + '/get_projects/'
    console.log('Getting Data from url ' + url);
    ticker('getting list of projects');
    $("#select_project").empty();
    $('#select_project').append('<select id="project"></select>');
    $('#project').append('<option value=""></option>')
    $.getJSON(url).then(function(data) {
        $('body').css('cursor','wait');
        data.sort();
        data.forEach(function(rowdata) {
            console.log('appending '+rowdata+' to project select');
            $('#project').append('<option value=' + rowdata + '>' + rowdata + '</option>');
        });
        $('body').css('cursor','default');
    });
    // defining change for project
    $( '#project').change(function() {
        get_tablenames($('#project').val());
    });
}

/*
get a list of available tablenames for this project
project is required
*/
function get_tablenames(project) {
    $('#select_tablename').empty();
    $('#select_tablename').append('<select id="tablename"></select>');
    $('#tablename').append('<option value=""></option>');
    var url = base_url + '/get_tablenames/' + project
    ticker('getting list of tablenames for project ' + project);
    console.log('Getting Data from url ' + url);
    var ret_data = [];
    $.getJSON(url).then(function(data) {
        $('body').css('cursor','wait');
        data.sort();
        data.forEach( function(rowdata) {
            $('#tablename').append('<option value=' + rowdata + '>' + rowdata + '</option>');
            ret_data.push(rowdata);
        });
        $('body').css('cursor','default');
    });
    // definig change for tablename
    $( '#tablename').change(function() {
        //recreate autocomplete for new data
    });
}

/*
get a list of available tablenames for this project
project is required
*/
function get_stat_func_names() {
    $('#select_stat_funcs').empty();
    $('#select_stat_funcs').append('<select id="stat_func_names"></select>');
    $('#stat_func_names').append('<option value=""></option>');
    var url = base_url + '/get_stat_func_names'
    ticker('getting statistical function names');
    console.log('Getting Data from url ' + url);
    var ret_data = [];
    $.getJSON(url).then(function(data) {
        $('body').css('cursor','wait');
        console.log(data);
        data.sort();
        data.forEach( function(rowdata) {
            $('#stat_func_names').append('<option value=' + rowdata + '>' + rowdata + '</option>');
            ret_data.push(rowdata);
        });
        $('body').css('cursor','default');
    });
}


/*
get data from json and create a graph on actual container
*/
function getData() {
    var project = $("#project").val();
    var tablename = $("#tablename").val();
    var datestring = $("#datestring").val();
    var stat_func_name = $("#stat_func_names" ).val();
    var url = base_url + "/get_tsastats_func/" + project + "/" + tablename + "/" + datestring + "/" + stat_func_name;
    console.log("Getting Data from url " + url);
    ticker("Fetching data, this could last for some time, if this is the first request");
    $.getJSON(url).then(function(data) {
        $('#reports').html(renderTable(data, "reportTable")); 
        $("#reportTable").tablesorter();
        ticker("Got data, finished, maybe you select another one?");
    });
}