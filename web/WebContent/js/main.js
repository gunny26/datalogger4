// base url for JSON data
var baseurl = "https://datalogger-api.tirol-kliniken.cc/DataLogger";

$(document).ready(function() {
	setDate();
	getStatisticalFunctions();
	getProjects();
	// show data after clicking the submit button
	$('.myform').submit(function(event) {
		event.preventDefault();
		getData();
	});
	
});

// fill select fields with projects
function getProjects() {
	url = baseurl + '/get_projects/';
	$.getJSON(url, function(data) {
		data.sort();
		$(data).each(function(index, value) {
			$('#select_project').append($('<option></option>').val(value).html(value));
		});
		// get details depending on selected value
		getDataDetails($('#select_project').val());
		$('#select_project').change(function() {
			getDataDetails($('#select_project').val());
		});

	});
}

// fill select fields with details
function getDataDetails(data) {
	url = baseurl + '/get_tablenames/' + data;
	$('#select_details').empty();
	$.getJSON(url, function(data) {
		data.sort();
		$(data).each(function(index, value) {
			$('#select_details').append($('<option></option>').val(value).html(value));
		});
	});
}

//	fill select fields with available statistical functions 
function getStatisticalFunctions() {
	url = baseurl + '/get_stat_func_names/'
	$('#select_statistics').empty();
	$.getJSON(url, function(data) {
		data.sort();
		$(data).each(function(index, value) {
			$('#select_statistics').append($('<option></option>').val(value).html(value));
		});
	});
}

// date picker
// dates are not accessable for today or future
function setDate() {
	var yesterday = new Date();
	var minDate = new Date();
	
	yesterday.setDate(yesterday.getDate() - 1);
	//console.log("yesterday " + yesterday)
	minDate.setDate(minDate.getDate() - 60);
	//console.log("minDate " + minDate)
	
 $(".datepicker").datepicker({
	 format: 'dd-mm-yyyy', 
	 minDate: minDate,
	 endDate: yesterday,
	 startDate: yesterday
	 });
	//$(".datepicker").datepicker("setDate", yesterday);

}


// collecting data
function getData() {
	var project = $('#select_project').val();
	var details = $('#select_details').val();
	var statistics = $('#select_statistics').val();
	var datestring = $('.datepicker').val();
	var url = baseurl + "/get_tsastats_func/" + project + "/" + details + "/"
			+ datestring + "/" + statistics;
	console.log(url)
	$(document).ajaxSend(function(event, request, settings) {
		$('.progress').show();
	});
	$.getJSON(url).then(function(data) {
		$('.report').html(renderTable(data, "reportTable"));
		$('.report').DataTable();
		$(document).ajaxComplete(function(event, request, settings) {
			$('.progress').hide();
		});
	})
	.done(function() { $('.errormessage').html(""); })
	.fail(function() {
		$('.progress').hide();
		$('.errormessage').html(
				"<div class=\"notify alert alert-danger fade in\"> <a href=\"../pages/dailyreport.html\" class=\"close\" data-dismiss=\"alert\" aria-label=\"close\">&times;</a><h4> <span class=\"glyphicon glyphicon-exclamation-sign\" aria-hidden=\"true\"></span> <strong>Anfrage fehlgeschlagen</strong> </h4> <p>Der gewünschte Report kann nicht angezeigt werden. Bitte die Auswahlparameter prüfen.</p></div>"					
	);
					});
}

function renderTable(data, id) {
	//console.log(data +","+ id);
    firstRow = true;
    evenRow = false;

    html="<table>";
    data.forEach(function(row) {
        // header fields if this is the first row 
        if (firstRow == true) {
            html += "<thead>";
            html += "<th class=\"info\">" 
            html += row.join("</th><th class=\"info\">") 
            html += "</th>";
            html += "</thead><tbody>";
            firstRow = false;
        } else {
            html += "<tr>"
            // join fields according to datatype 
            row.forEach(function(field) {
                if (jQuery.isNumeric(field) == true) {
                    try {
                        html += "<td>" + field.toFixed(2) + "</td>";
                    } catch (e) {
                        //seems not to be floating point
                        console.log(e);
                        html += "<td>" + field + "</td>";
                    }
                } else {
                    html += "<td>" + field + "</td>";
                }
            });
            html += "</tr>"
        }
    });
    html += "</tbody></table>";
    console.log(html)
    return html;
    
    
    /*
    html="<table id=" + id + " class=tablesorter>";
    data.forEach(function(row) {
        // header fields if this is the first row 
        if (firstRow == true) {
            html += "<thead>";
            html += "<th class=datalogger-tsastats-table-header>" 
            html += row.join("</th><th class=datalogger-tsastats-table-header>") 
            html += "</th>";
            html += "</thead><tbody>";
            firstRow = false;
        } else {
            // even or uneven row, set according class 
            trClass = "";
            if (evenRow == true) {
                trClass = "datalogger-tsastats-table-even-row";
                evenRow = false;
            } else {
                trClass = "datalogger-tsastats-table-uneven-row";
                evenRow = true;
            }
            html += "<tr class=" + trClass + ">"
            // join fields according to datatype 
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
    */
}
