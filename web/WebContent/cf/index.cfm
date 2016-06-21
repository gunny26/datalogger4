<cfscript>
        param name = "url.p" default = "second" type="any";
        listtest = "first,second,third";
        <!---fromUrl = ListGetAt(listtest, url.p);--->
	fromUrl = ListContains(listtest, url.p);
	i=0;
	while(i <= 10){
		WriteOutput(i & ",");
		++i;
	}
	WriteOutput("<br>");
	for(i = 0; i <= 10; ++i){
		WriteOutput(i & ",");
	}
</cfscript>
<cfoutput>
	Position entry: <strong>#ListGetAt(listtest,fromUrl)#</strong><br>
</cfoutput>
<cfloop	list="#listtest#" index="iPosition">
	<cfoutput>
		<a href="?p=#iPosition#">#iPosition#</a><br>
	</cfoutput>

</cfloop>
<!---
<cfoutput>

        <p>Output: </p></br>
        <ul>
                <li><a href="?p=1">First entry</a></li>
                <li><a href="?p=2">Second entry</a></li>
                <li><a href="?p=3">Third entry</a></li>
        </ul>

        Result: #fromUrl#
</cfoutput>
--->
