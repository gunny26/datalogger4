/*
render tablesorter usable html from json data
first row is used as headers
add following classes for styling

table      : tablesorter
even rows  : datalogger-tsastats-table-even-row
uneven rows: datalogger-tsastats-table-uneven-row
text fields: datalogger-tsastats-table-value-field
numeric f. : datalogger-tsastats-table-index-field
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
