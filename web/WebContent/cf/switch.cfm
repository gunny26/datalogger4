<cfscript>
	param name = "url.faq" default = 1 type = "any";
	faq = ArrayNew(1);
	faq[1] = StructNew();
	faq[1].id = "a"; 
	faq[1].question = "is this entry 1?";
	faq[1].answer = "yes, it's entry 1 ";
	faq[2] = StructNew();
	faq[2].id = "b";
	faq[2].question = "Is this entry 2?";
	faq[2].answer = "yes, it's entry 2";
	faq[3] = StructNew();
	faq[3].id = "c";
	faq[3].question = "is this enty 3?";
	faq[3].answer = "yes, it's entry 3";
	
</cfscript>
<cfswitch expression="#url.faq#">
	<cfcase value="a">
		<cfset question = faq[1].question>
		<cfset answer = faq[1].answer>	
	</cfcase>        
	<cfcase value="c">
                <cfset question = faq[3].question>
                <cfset answer = faq[3].answer>
        </cfcase>
	<cfdefaultcase>
	 	<cfset question = faq[2].question>
                <cfset answer = faq[2].answer>
	</cfdefaultcase>
</cfswitch>
<cfoutput>
	<strong>Questions</strong>
	#faq[url.faq].question#</br>
	<strong>Answer</string>
	#faq[url.faq].answer#</br>
</cfoutput>
<cfloop from="1" to="#ArrayLen(faq)#" index="ifaq">
	<cfoutput>
		<strong>Questions</strong>
		<a href="?faq=#ifaq#">#faq[ifaq].question#</a><br>
	</cfoutput>
</cfloop>
<cfdump var="#faq#">
