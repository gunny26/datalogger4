<cfscript>
	param name = "url.faq" default = 1 type = "any";
	faq = ArrayNew(1);
	faq[1] = StructNew(); 
	faq[1].question = "is this entry 1?";
	faq[1].answer = "yes, it's entry 1 ";
	faq[2] = StructNew();
	faq[2].question = "Is this entry 2?";
	faq[2].answer = "yes, it's entry 2";
	faq[3] = StructNew();
	faq[3].question = "is this enty 3?";
	faq[3].answer = "yes, it's entry 3";
	
	if(!isNumeric(url.faq)){
		url.faq = 1;
	} else {
		if(url.faq < 1){
			url.faq = 1;
		} else if (url.faq > ArrayLen(faq)){
			url.faq = 1;
		}
	}
	
	WriteOutput(faq[url.faq].question & "<br>");
	WriteOutput(faq[url.faq].answer & "<br>");
</cfscript>
<cfloop from="1" to="#ArrayLen(faq)#" index="ifaq">
	<cfoutput>
		<a href="?faq=#ifaq#">#faq[ifaq].question#</a><br>
	</cfoutput>
</cfloop>
<cfdump var="#faq#">
