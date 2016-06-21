<cferror type="request"  template="mymedis_anvorb_error2.cfm">
<meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1">


<cfquery name="PERSONAL1" datasource=#session.db_info#>
select distinct
ckis_mgr.ARBPL_TEAM.PERSONAL_ART_NR P_ART,
LISTAGG((substr(person.name1,1,9) || substr(person.name2,1,1) || '(' ||substr(personal.diensttelefon,-5,5) || ')') ,',  ') WITHIN GROUP (ORDER BY ckis_mgr.ARBPL_TEAM.RANG) AS PERSONAL
from ckis_mgr.arbpl_team 
join ckis_mgr.personal on arbpl_team.personal_pid = personal.pid
join ckis_mgr.person on arbpl_team.personal_pid = person.pid
join ckis_mgr.personal_art on arbpl_team.personal_art_nr = personal_art.nr
where arbpl_team.arbpl_inr = 81 and arbpl_team.op_akt_dat = to_date('#session.anfang#','DD.MM.YYYY') + #session.anfang_korr#
GROUP BY ckis_mgr.ARBPL_TEAM.PERSONAL_ART_NR
</cfquery>


<cfquery name="PERSONAL2" datasource=#session.db_info#>
select distinct
ckis_mgr.ARBPL_TEAM.PERSONAL_ART_NR P_ART,
LISTAGG((substr(person.name1,1,9) || substr(person.name2,1,1) || '(' ||substr(personal.diensttelefon,-5,5) || ')') ,',  ') WITHIN GROUP (ORDER BY ckis_mgr.ARBPL_TEAM.RANG) AS PERSONAL
from ckis_mgr.arbpl_team 
join ckis_mgr.personal on arbpl_team.personal_pid = personal.pid
join ckis_mgr.person on arbpl_team.personal_pid = person.pid
join ckis_mgr.personal_art on arbpl_team.personal_art_nr = personal_art.nr
where arbpl_team.arbpl_inr = 88 and arbpl_team.op_akt_dat = to_date('#session.anfang#','DD.MM.YYYY') + #session.anfang_korr#
GROUP BY ckis_mgr.ARBPL_TEAM.PERSONAL_ART_NR
</cfquery>


<cfquery name="Daten" datasource=#session.db_info#>
select op_plan.inr Inr, substr(arbpl.bez,1,30) Arbpl, arbpl.inr Arbpl_Inr, arbpl.sort Arbpl_Sort, fachabt.inr Fachabt_inr, 
   concat(concat(lpad(round(op_plan.op_akt_zeit/3600,0),2,'0'),':'),(lpad(round(mod(op_plan.op_akt_zeit,3600)/60,0),2,'0'))) op_akt_zeit,
   op_plan.status status,
   (select a.skey from ckis_mgr.fachabt a where a.inr = op_plan.lst_fachabt) FA, 
   decode(decode(op_plan.patient_pid, Null, '-', (select b.name1 from ckis_mgr.person b where b.pid = op_plan.patient_pid and b.modified_at = to_date('01.01.37','DD.MM.YY'))),NULL, '-',
   (decode(op_plan.patient_pid, Null, (select vm.Nachname ||' '|| vm.vorname from ckis_mgr.vormerk_patient vm where vm.inr = op_plan.vormerk_patient_inr), (select concat(concat(b.name1,' '), initcap(b.name2)) from ckis_mgr.person b where b.pid = op_plan.patient_pid and b.modified_at = to_date('01.01.37','DD.MM.YY'))))) NAME1,
   decode(decode(op_plan.patient_pid, Null, '-', (select to_char(b.geburtsdatum,'DD.MM.YYYY') from ckis_mgr.person b where b.pid = op_plan.patient_pid and b.modified_at = to_date('01.01.37','DD.MM.YY'))),NULL, '-',
   (decode(op_plan.patient_pid, Null, (select to_char(vm.geburtsdatum,'DD.MM.YY') from ckis_mgr.vormerk_patient vm where vm.inr = op_plan.vormerk_patient_inr), (select to_char(b.geburtsdatum, 'DD.MM.YY') from ckis_mgr.person b where b.pid = op_plan.patient_pid and b.modified_at = to_date('01.01.37','DD.MM.YY'))))) GEB_DATUM,
   decode(decode(op_plan.patient_pid, Null, '-', (select to_char(b.geburtsdatum,'DD.MM.YYYY') from ckis_mgr.person b where b.pid = op_plan.patient_pid and b.modified_at = to_date('01.01.37','DD.MM.YY'))),NULL, '-',
   (decode(op_plan.patient_pid, Null, (select to_char(vm.geburtsdatum,'DD.MM.YYYY') from ckis_mgr.vormerk_patient vm where vm.inr = op_plan.vormerk_patient_inr), (select to_char(b.geburtsdatum, 'DD.MM.YYYY') from ckis_mgr.person b where b.pid = op_plan.patient_pid and b.modified_at = to_date('01.01.37','DD.MM.YY'))))) GEB_DATUM1,
   decode(station.skey, Null, '-', station.skey) A_Station,
   to_date(op_plan.op_akt_dat,'DD.MM.YY') op_akt_dat,
   decode(op_plan.beschr, Null, '-', op_plan.beschr) beschr,
   decode(op_plan.bem, Null, '', op_plan.bem) bem,
   decode(OP_PLAN.OP_ABHOL_STAT_INR, Null, '-', OP_PLAN.OP_ABHOL_STAT_INR) OP_Abhol_Stat_Inr, 
   op_plan.Aufenthalt_inr  Aufenthal_inr , Fachabt."BEZ" BEZ,
   decode(op_plan.op_bestell_bem, null, '-', op_plan.op_bestell_bem) bbem,   
   decode((select PERSON."NAME1" from CKIS_MGR."OP_TEAM" OP_TEAM, CKIS_MGR."PERSON" PERSON where OP_TEAM."PERSONAL_PID" = PERSON."PID" and PERSON."MODIFIED_AT" = to_date('01.01.2037','DD.MM.YYYY') and OP_TEAM."NARKOSE_INR" is null and OP_TEAM."PERSONAL_ART_NR" = 2 and  OP_TEAM."RANG" = 1 and OP_TEAM."OP_PLAN_INR" = OP_PLAN."INR"), Null, '-',
   (select concat(initcap(PERSON."NAME1"), concat(' ',concat(substr(PERSON."NAME2",1,1),'.'))) from CKIS_MGR."OP_TEAM" OP_TEAM, CKIS_MGR."PERSON" PERSON where OP_TEAM."PERSONAL_PID" = PERSON."PID" and PERSON."MODIFIED_AT" = to_date('01.01.2037','DD.MM.YYYY') and OP_TEAM."NARKOSE_INR" is null and OP_TEAM."PERSONAL_ART_NR" = 2 and  OP_TEAM."RANG" = 1 and OP_TEAM."OP_PLAN_INR" = OP_PLAN."INR")) OPER,
   decode(op_plan.status,
            1, 'nicht geplant', 2, 'geplant', 3, 'bestaetigt', 4, 'freigegeben', 5, 'durchgefuehrt', 6, 'abgebrochen',
            10, 'storniert', 41, 'bestellt', 42, 'abzuholen', 43, 'geholt', 44, 'geschleust', 49, 'Durchfuehrung', '-') PLAN_STATUS,
   decode(op_plan.POSTOP_VERS, 1, 'Stationsbett', 2, 'Intermediate Care', 3, 'Intensivbett', 20000, 'Observation', '') AN_BETT1,
   decode(op_plan.POSTOP_VERS, 1, 'Station', 2, 'IMC', 3, 'ICU', 20000, 'OBS', '') AN_BETT2,
   decode(op_plan.vormerk_patient_inr, null, 0, 1) Vormerk_pat 
from ckis_mgr.op_plan op_plan, ckis_mgr.arbpl arbpl, ckis_mgr.fachabt fachabt, ckis_mgr.station station   
where op_plan.ausf_arbpl = arbpl.inr(+) and op_plan.lst_fachabt = fachabt.inr(+) and op_plan.op_abhol_stat_inr = station.inr(+) and op_plan.op_akt_dat >= to_date('#session.anfang#','DD.MM.YYYY') + #session.anfang_korr# and 
   op_plan.op_akt_dat <= to_date('#session.ende#','DD.MM.YYYY') + #session.ende_korr# and op_plan.status <> 10 and op_plan.status <> 100 and 
   op_plan.lst_fachabt in (1, 2, 3, 4, 5, 6, 8, /*11,*/ 12, 14, 17, 19, 20, 22)  
order by op_plan.op_akt_dat, arbpl.sort, op_plan.op_akt_zeit
</cfquery>


<cfquery name="op_tag" datasource=#session.db_info#>
select to_char(to_date('#session.anfang#','DD.MM.YY') + #session.anfang_korr#,'DD.MM.YYYY') OP_TAG from dual 
</cfquery>


<cfquery name="op_tag_int" datasource=#session.db_info#>
select decode(to_number(to_char(to_date('#session.anfang#','DD.MM.YYYY') + #session.anfang_korr# - 1,'D')),
              1, 'Montag',
			  2, 'Dienstag',
			  3, 'Mittwoch',
			  4, 'Donnerstag',
			  5, 'Freitag',
			  6, 'Samstag',
			  'Sonntag') 
			  OP_TAG_int from dual
</cfquery>


<cfquery name="change_ok" datasource=#session.db_info#>
select 
decode(substr(trunc(a.Datum) - trunc(sysdate),1,1), '-', 1, 0)
from  ckis_mgr.lki_datum a 
where trunc(a.datum) = trunc(sysdate)
</cfquery>


<cfquery name="change_ok1" datasource=#session.db_info#>
select 
decode(substr(trunc(to_date('#session.anfang#','DD.MM.YY') + #session.anfang_korr#) - trunc(sysdate),1,1), '-', 1, 0) change
from dual
</cfquery>

<cfset session.change_ok = #change_ok1.change#>

<cfset QueryString_Recordset1=Iif(CGI.QUERY_STRING NEQ "",DE("&"&CGI.QUERY_STRING),DE(""))>

<html>
<head>
<title> </title> 
<meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1">
<link href="../allgemein/system.css" rel="stylesheet" type="text/css">


<script language="JavaScript" type="text/JavaScript">
<!--
function MM_reloadPage(init) {  //reloads the window if Nav4 resized
  if (init==true) with (navigator) {if ((appName=="Netscape")&&(parseInt(appVersion)==4)) {
    document.MM_pgW=innerWidth; document.MM_pgH=innerHeight; onresize=MM_reloadPage; }}
  else if (innerWidth!=document.MM_pgW || innerHeight!=document.MM_pgH) location.reload();
}
MM_reloadPage(true);
//-->
</script>


<style type="text/css">
<!--
.Kopf      {position: relative; left: 0px; top: 0px; visibility: visible; z-index: 1; height: 5%;}
.Daten     {height: 60%; width: 100%; overflow: auto;}
.Farbe1    {background-color:#CCCCCC}
.Farbe2    {color:#FF0000;}
.Farbe3    {color:#FFFFFF;}
.Farbe4    {color:#FFFFFF;}
.Farbe5    {color:#0000FF;}
.textmitte {font:Verdana, Arial, Helvetica, sans-serif; background-color:#FFFFFF; border-color:#000000 size: -1;}
-->
</style>

</head>


<body bgcolor="#B6D6BD" onselectstart="return false" ondragstart="return false" oncontextmenu="return false" oncontext="return false">>


<table  width="98%" border="0" bordercolor="#B6D6BD">
<tr>
  <td width="6%"></td>
  <td width="30%"><strong><font size="+1" face="Verdana, Arial, Helvetica, sans-serif"><em>Zuteilung Aufwach- / Intensivstation </em></font></strong></td>
  <td width="62%"><div align="left"><span class="Farbe2"><strong><samp><font size="-1" face="Verdana, Arial, Helvetica, sans-serif">Dienst1:</font><font size="-1" face="Verdana, Arial, Helvetica, sans-serif"></font></samp></strong></span><strong><samp><font size="-1" face="Verdana, Arial, Helvetica, sans-serif">&nbsp;&nbsp;&nbsp;<cfoutput>#Personal1.personal#</cfoutput><br>
    <span class="Farbe2">Dienst2:</span>&nbsp;&nbsp;&nbsp;<cfoutput>#Personal2.personal#</cfoutput></font></samp></strong></div></td>
</tr>
</table>


<div class="Kopf" id="Layer2" style="position:absolute; left:8px; top:16px; width:85px; height:58px; z-index:2"><span class="Kopf"><img src="../allgemein/mymedis_logo.gif" width="83" height="56"></span></span></div>
<p>&nbsp;</p>


<div class="Kopf">
  
  <table  width="98%" border="0" bordercolor="#B6D6BD">
  <tr> 
  <td width="3%" align="center"><form name="form" method="post" action="../anvorb/mymedis_anvorb_report1_set.cfm"><input type="submit" name="Submit22" value=" < 7 "></form></td>	
  <td width="3%"><form name="form" method="post" action="../anvorb/mymedis_anvorb_report2_set.cfm"><input type="submit" name="Submit26" value=" < 1 "></form></td>	
  <td width="3%"><form name="form" method="post"><input type="text" name="Submit23" size="32" value="   <cfoutput>#op_tag_int.op_tag_int#</cfoutput>   /   <cfoutput>#op_tag.op_tag#</cfoutput>"></form></td>	
  <td width="3%"><form name="form" method="post" action="../anvorb/mymedis_anvorb_report3_set.cfm"><input type="submit" name="Submit24" value=" 1 > "></form></td>	
  <td width="3%"><form name="form" method="post" action="../anvorb/mymedis_anvorb_report4_set.cfm"><input type="submit" name="Submit2" value=" 7 > "></form></td>	
  <td width="10%"></td>	
<cfif #session.anfang_korr# is not 0>
    <td width="3%"><form name="form" method="post" action="../anvorb/mymedis_anvorb_report5_set.cfm"><input type="submit" name="Submit27" value=" folgender Plantag "></form></td>	
<cfelse>
    <td width="3%"><form name="form" method="post" action=""><input disabled name="Submit27" type="button" class="Farbe3" value=" folgender Plantag ">
    </form></td>	
</cfif>
  <td width="3%">&nbsp;</td>
  <td width="3%"><form name="form" method="post" action="../anvorb/mymedis_anvorb_print2.cfm"><input type="submit" name="Submit272" value=" Druckansicht 1">  </form></td>	
  
  <!--<td width="3%"><form name="form" method="post" action="opdis_anvorb_print.cfm"><input type="submit" name="Submit275" value="T"> </form></td>-->
  <td width="3%"><form name="form" method="post" action="../anvorb/mymedis_anvorb_print3.cfm"><input type="submit" name="Submit272" value=" Druckansicht 2">  </form></td>
  <td width="62%">&nbsp;</td>
  <td width="6%"></td>
  <td width="6%"></form></td>
  </tr>
</table>


<cfif #Daten.RecordCount# is not 0>
    <table width="98%" class="Kopf" border="1"  cellpadding="2" cellspacing="2" bordercolor="#B6D6BD">
      <tr>
        <th width="2%" nowrap bordercolor="#000000" bgcolor="#FFFFFF"><em><font size="-2" face="Verdana, Arial, Helvetica, sans-serif">STM</font></em></th>
        <th width="2%" nowrap bordercolor="#000000" bgcolor="#FFFFFF"><em><font size="-2" face="Verdana, Arial, Helvetica, sans-serif">IMC</font></em></th>
        <th width="2%"  nowrap bordercolor="#000000" bgcolor="#FFFFFF"><em><font size="-2" face="Verdana, Arial, Helvetica, sans-serif">ICU</font></em></th>
        <th width="2%"  nowrap bordercolor="#000000" bgcolor="#FFFFFF"><em><font size="-2" face="Verdana, Arial, Helvetica, sans-serif">OBS</font></em></th>
        <th width="2%"  nowrap bordercolor="#000000" bgcolor="#FFFFFF"><em><font size="-2" face="Verdana, Arial, Helvetica, sans-serif">X</font></em></th>
        <th width="9%" nowrap bordercolor="#000000" bgcolor="#FFFFFF"><em><font size="-2" face="Verdana, Arial, Helvetica, sans-serif">postop Versorgung</font></em></th>
        <th width="11%" nowrap bordercolor="#000000" bgcolor="#FFFFFF"><em><font size="-2" face="Verdana, Arial, Helvetica, sans-serif">Arbeitsplatz</font></em></th>
        <th width="3%"  nowrap bordercolor="#000000" bgcolor="#FFFFFF"><em><font size="-2" face="Verdana, Arial, Helvetica, sans-serif">lfd.</font></em></th>
        <th width="12%" nowrap bordercolor="#000000" bgcolor="#FFFFFF"><em><font size="-2" face="Verdana, Arial, Helvetica, sans-serif">Patient</font></em></th>
        <th width="7%"  nowrap bordercolor="#000000" bgcolor="#FFFFFF"><div align="center"><em><font size="-2" face="Verdana, Arial, Helvetica, sans-serif">geb.</font></em></div></th>
        <th width="3%"  nowrap bordercolor="#000000" bgcolor="#FFFFFF"><div align="center"><em><font size="-2" face="Verdana, Arial, Helvetica, sans-serif">Sta.</font></em></div></th>
        <th width="28%" nowrap bordercolor="#000000" bgcolor="#FFFFFF"><em><font size="-2" face="Verdana, Arial, Helvetica, sans-serif">OP-Beschreibung</font></em></th>
        <th width="9%" nowrap bordercolor="#000000" bgcolor="#FFFFFF"><em><font size="-2" face="Verdana, Arial, Helvetica, sans-serif">Operateur</font></em></th>
        <th width="6%" nowrap bordercolor="#000000" bgcolor="#FFFFFF"><div align="center"><em><font size="-2" face="Verdana, Arial, Helvetica, sans-serif">Plan-Status</font></em></div></th>
      </tr>
    </table>
</cfif>
</div>


<div class="Farbe2">
<cfif #Daten.RecordCount# is 0>
<table width="100%">
  <tr> 
    <td width="17%" height="24" class="Farbe2"><font size="+1" face="Verdana, Arial, Helvetica, sans-serif">&nbsp;</font></td>
	<td width="83%" class="Farbe2"><strong><font size="+1" face="Verdana, Arial, Helvetica, sans-serif">Keine Daten in der OP-Planung !!!</font></strong></td>
  </tr>
</table>
</cfif>


<table width="98%" border="1" height="5"  cellpadding="2" cellspacing="2">
  <cfoutput query="Daten">
  <form name="FORM" action="../anvorb/mymedis_anvorb_report_set_A.cfm" method="post">
   <tr bgcolor="##B6D6BD">
      <input name="OP_PLAN_INR" type="hidden" value="#DATEN.INR#">
      <th width="2%" align="center" valign="bottom"  bgcolor="##B6D6BD" ><input name="AN_BETT" style="float:center" type="submit" value="1"></th>
      <th width="2%" align="center" valign="bottom"  bgcolor="##B6D6BD" ><input name="AN_BETT" style="float:center" type="submit" value="2"></th>
      <th width="2%" align="center" valign="bottom"  bgcolor="##B6D6BD" ><input name="AN_BETT" style="float:center" type="submit" value="3"></th>
      <th width="2%" align="center" valign="bottom"  bgcolor="##B6D6BD" ><input name="AN_BETT" style="float:center" type="submit" value="4"></th>
      <th width="2%" align="center" valign="bottom"  bgcolor="##B6D6BD" ><input name="AN_BETT" style="float:center" type="submit" value="-"></th>
      <td width="9%" bgcolor="##FFFFFF" class="Farbe2"><div align="left"><strong><samp><font size="-1" face="Verdana, Arial, Helvetica, sans-serif"> #Daten.AN_BETT1#</font></samp></strong></div></td>
      <td width="11%" bgcolor="##FFFFFF" class="Farbe5" ><div align="left"><strong><samp><font size="-1" face="Verdana, Arial, Helvetica, sans-serif"> #Daten.ARBPL#</font></samp></strong></div></td>
      <td width="3%" bgcolor="##FFFFFF" ><div align="center"><samp><font size="-1" face="Verdana, Arial, Helvetica, sans-serif">#Daten.OP_AKT_ZEIT#	</font></samp></div></td>

      <cfif #Daten.VORMERK_PAT# is '1'>
        <td width="12%" bgcolor="##FFFFFF" class="Farbe2"><div align="left"><samp><font size="-1" face="Verdana, Arial, Helvetica, sans-serif">#Daten.NAME1#</font></samp></div></td>
        <cfelse>
        <td width="12%" bgcolor="##FFFFFF"><div align="left"><samp><font size="-1" face="Verdana, Arial, Helvetica, sans-serif">#Daten.NAME1#</font></samp></div></td>
      </cfif>

      <cfif #Daten.VORMERK_PAT# is '1'>
        <td width="7%" bgcolor="##FFFFFF" class="Farbe2"><div align="center"><samp><font size="-1" face="Verdana, Arial, Helvetica, sans-serif">#Daten.GEB_DATUM#</font></samp></div></td>
        <cfelse>
        <td width="7%" bgcolor="##FFFFFF"><div align="center"><samp><font size="-1" face="Verdana, Arial, Helvetica, sans-serif">#Daten.GEB_DATUM#</font></samp></div></td>
      </cfif>

      <cfif #Daten.A_STATION# is '-'>
        <td width="3%" bgcolor="##FFFFFF" class="Farbe3"><div align="center"><samp><font size="-1" face="Verdana, Arial, Helvetica, sans-serif">#Daten.A_STATION#</font></samp></div></td>
        <cfelse>
        <td width="3%"  bgcolor="##FFFFFF"><div align="center"><samp><font size="-1" face="Verdana, Arial, Helvetica, sans-serif">#Daten.A_STATION#</font></samp></div></td>
      </cfif>

      <cfif #Daten.beschr# is '-'>
        <td width="28%" bgcolor="##FFFFFF" class="Farbe3"><div align="left"><samp><font size="-1" face="Verdana, Arial, Helvetica, sans-serif">#Daten.beschr# <br> 
          <span class="Farbe2">#Daten.bem#</span></font></samp></div></td>
        <cfelse>
        <td width="28%"  bgcolor="##FFFFFF"><div align="left"><samp><font size="-1" face="Verdana, Arial, Helvetica, sans-serif">#Daten.beschr# <br> 
          <span class="Farbe2">#Daten.bem#</span></font></samp></div></td>
      </cfif>

      <cfif #Daten.OPER# is '-'>
        <td width="9%" bgcolor="##FFFFFF" class="Farbe3"><samp><font size="-1" face="Verdana, Arial, Helvetica, sans-serif">#Daten.OPER#</font></samp></td>
        <cfelse>
        <td width="9%" bgcolor="##FFFFFF"><samp><font size="-1" face="Verdana, Arial, Helvetica, sans-serif">#Daten.OPER#</font></samp></td>
      </cfif>

      <cfif #Daten.PLAN_STATUS# is '-'>
        <td width="6%" bgcolor="##FFFFFF" class="Farbe3"><samp><font size="-1" face="Verdana, Arial, Helvetica, sans-serif">#Daten.PLAN_STATUS#</font></samp></td>
        <cfelse>
        <td width="6%" bgcolor="##FFFFFF"><samp><font size="-1" face="Verdana, Arial, Helvetica, sans-serif">#Daten.PLAN_STATUS#</font></samp></td>
      </cfif>

    </tr>
</form>      
</cfoutput>
</table>
</div> 


<p>&nbsp;</p>
</body>
</html>

