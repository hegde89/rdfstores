package edu.unika.aifb.atwrank;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.json.simple.JSONValue;

import edu.unika.aifb.atwrank.reader.WikiReader;

public class WikiReadTest {

		private static String getTokenString(String text) throws IOException {
			StandardAnalyzer al = new StandardAnalyzer();
			TokenStream ts = al.tokenStream(null, new StringReader(text));
			Token t = new Token();
			StringBuilder sb = new StringBuilder();
			while ((t = ts.next(t)) != null) {
				sb.append(t.term()).append(" ");
			}
			return sb.toString();
		}
	
		public static void main(String[] args) throws IOException {
			
			WikiReader wikireader = new WikiReader("http://semanticweb.org");
//			WikiReader wikireader = new WikiReader("http://localhost/wiki");
			//wikireader.readTextForPage("Tools");
			List<String> titles = wikireader.getAllPageTitles();
			
			System.out.println(wikireader.getStats());
			System.out.println("number of titles in array:" + titles.size());

//			System.out.println(titles);
			PrintWriter out = new PrintWriter(new FileWriter("/data/test.nt"));
			int i = 0;
			for (String title : titles) {
				try {
					Integer.parseInt(title);
				}
				catch (NumberFormatException e) {
					System.out.println(title);
					String text = wikireader.getRenderedText(title);
					text = text.replaceAll("\n", " ");
					out.println("<http://semanticweb.org/id/" + URLEncoder.encode(title, "UTF-8") + "> <http://example.org/page_text> \"" + getTokenString(text) + "\" .");
					out.flush();
					i++;
					if (i == 50)
						break;
				}
			}
			out.close();
		}
	
	
}
