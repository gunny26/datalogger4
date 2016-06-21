<CFLOOP COLLECTION="#Session#" ITEM="sessionVar">
    <CFSET StructDelete(Session, sessionVar)>
</CFLOOP>

<html>
<head>
<title>OpDIS   ---   Report - Error</title>
<meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1">

</head>

<body  onload = "if (self != top) top.location = self.location"  bgcolor="#B6D6BD">

<div id="Layer1" style="position:absolute; left:8px; top:16px; width:108px; height:90px; z-index:2"><img src="../allgemein/mymedis_logo.gif" width="130" height="70"></div>
<div id="Layer2" style="position:absolute; left:160px; top:50px; width:500px; height:33px; z-index:2"><font size="+2" face="Verdana, Arial, Helvetica, sans-serif"><strong><em>Timeout  in Anwendung  !!! </em></strong></font></div>
<p>&nbsp;</p>
<p>&nbsp;</p>
<p>&nbsp;</p>
<p>&nbsp;</p>


</body>
</html>




