package edu.unika.aifb.atwrank.reader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.htmlparser.beans.StringBean;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class WikiReader {

	public String baseurl;

	public WikiReader(String baseurl) {
		super();
		this.baseurl = baseurl;
	}

	
	public String getRenderedText(String title) {
		StringBean sb = new StringBean();
		sb.setURL(baseurl + "/index.php?title=" + title + "&action=render");
		return sb.getStrings();
	}

	public String getStats() {

		URL pageurl;
		String numpages = null;
		try {

			String inputcompl = getStringFromWiki(baseurl
					+ "/api.php?action=query&meta=siteinfo&siprop=statistics&format=json");

			JSONParser parser = new JSONParser();

			Map json = (Map) parser.parse(inputcompl);

			Object jsonobj = ((Map) ((Map) json.get("query")).get("statistics"))
					.get("pages");
			numpages = JSONValue.toJSONString(jsonobj);

			// System.out.println(numpages);
		} catch (ParseException pe) {
			System.out.println(pe);

		}

		return numpages;
	}

	@SuppressWarnings("unchecked")
	public List<String> getAllPageTitles() {

		// get number of pages
		// http://semanticweb.org/api.php?action=query&meta=siteinfo&siprop=statistics

		// get 500 pages
		// http://semanticweb.org/api.php?action=query&list=allpages&aplimit=500

		// get all page titles
		List<String> titlelist = new ArrayList<String>();

		// http://semanticweb.org/api.php?action=query&list=allpages&aplimit=500&format=json

		try {

			String inputcompl = getStringFromWiki(baseurl
					+ "/api.php?action=query&list=allpages&aplimit=500&format=json");

			JSONParser parser = new JSONParser();

			Map json = (Map) parser.parse(inputcompl);
			Object nextjsonobj = ((Map) ((Map) json.get("query-continue")).get("allpages")).get("apfrom");
			String apfrom = JSONValue.toJSONString(nextjsonobj);

			do {
				Object pages = ((Map) json.get("query")).get("allpages");
				// System.out.println("pages" + pages);
				JSONArray array = (JSONArray) pages;
				for (Object page : array) {
					titlelist.add((String)((JSONObject)page).get("title"));
				}

				inputcompl = getStringFromWiki(baseurl
						+ "/api.php?action=query&list=allpages&aplimit=500&format=json&apfrom="
						+ (apfrom.replace('"', ' ')).trim());

				json = (Map) parser.parse(inputcompl);

				if (json.containsKey("query-continue")) {
					nextjsonobj = ((Map) ((Map) json.get("query-continue"))
							.get("allpages")).get("apfrom");
				} else {
					System.out.println(json);
					break;
				}

				apfrom = JSONValue.toJSONString(nextjsonobj);
				// System.out.println(apfrom);
				System.out.println(apfrom);
			} 
			while ((apfrom != null));

		} catch (ParseException pe) {
			System.out.println(pe);
		}

		return titlelist;

	}

	

	protected String getStringFromWiki(String urlstr) {

		String inputcompl = "";

		try {

			URL url = new URL(urlstr);

			BufferedReader in;

			in = new BufferedReader(new InputStreamReader(url.openStream()));

			String inputLine;

			while ((inputLine = in.readLine()) != null) {
				inputcompl += inputLine;
			}
			in.close();

		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return inputcompl;
	}
}

// get number of pages
// http://semanticweb.org/api.php?action=query&meta=siteinfo&siprop=statistics

// get 500 pages
// http://semanticweb.org/api.php?action=query&list=allpages&aplimit=500

// get rendertext of a page
// http://semanticweb.org/index.php?title=Tools&action=render

// SMW
// http://semanticweb.org/index.php?title=Special:ExportRDF&page=ESWC2006&backlinks=1&recursive=1

/*
 * <swivt:Subject rdf:about="http://semanticweb.org/id/ESWC2006">
 * <property:pagetext rdf:datatype="http://www.w3.org/2001/XMLSchema#String">
 * text text text</property:pagetext> </swivt:Subject>
 * 
 * 
 * <owl:DatatypeProperty
 * rdf:about="http://semanticweb.org/id/Property-3Apagetext">
 * <rdfs:label>Pagetext</rdfs:label> <swivt:page
 * rdf:resource="http://semanticweb.org/wiki/Property:Pagetext"/>
 * <rdfs:isDefinedBy
 * rdf:resource="http://semanticweb.org/wiki/Special:ExportRDF/Property:Pagetext"
 * /> </owl:DatatypeProperty>
 */