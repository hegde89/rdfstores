package edu.unika.aifb.atwrank;

import java.util.ArrayList;

import org.json.simple.JSONValue;

import edu.unika.aifb.atwrank.reader.WikiReader;

public class WikiReadTest {

		public static void main(String[] args){
			
			//WikiReader wikireader = new WikiReader("http://semanticweb.org");
			WikiReader wikireader = new WikiReader("http://localhost/wiki");
			//wikireader.readTextForPage("Tools");
			ArrayList titles = wikireader.getAllPageTitles();
			
			wikireader.getStats();
			Object[] titlearray = titles.toArray();
			System.out.println("number of titles in array:"+titlearray.length);
			
			for(int i=0;i<titlearray.length;i++){
				System.out.println(JSONValue.toJSONString(titlearray[i]));
				
			}
		}
	
	
}
