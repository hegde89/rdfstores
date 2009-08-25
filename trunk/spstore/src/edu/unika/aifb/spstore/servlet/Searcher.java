package edu.unika.aifb.spstore.servlet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.spstore.servlet.request.HybridSearchRequest;
import edu.unika.aifb.spstore.servlet.request.KeywordSearchRequest;
import edu.unika.aifb.spstore.servlet.request.SearchRequest;
import edu.unika.aifb.spstore.servlet.request.StructuredSearchRequest;

/**
 * Servlet implementation class Searcher
 */
public class Searcher extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private JSONParser m_parser;
	private IndexReader m_reader;

    public Searcher() throws IOException {
    	m_parser = new JSONParser();
    	m_reader = new IndexReader(new IndexDirectory("/data/sp/indexes/sp/v2"));
    }
    
    private SearchRequest createSearchRequest(JSONObject object, HttpServletResponse response) throws IOException, StorageException {
    	if (object.containsKey(JSONFormat.OPT_KEYWORD_QUERY) && !object.containsKey(JSONFormat.OPT_TRANSLATION)) {
    		response.sendError(HttpServletResponse.SC_BAD_REQUEST, "translation has to be specified for keyword queries");
    		return null;
    	}
    	
    	System.out.println(object.keySet());
    	
    	if (object.containsKey(JSONFormat.OPT_STRUCTURED_QUERY) && object.containsKey(JSONFormat.OPT_KEYWORD_QUERY)) {
    		return new HybridSearchRequest(m_reader, object);
    	}
    	else if (object.containsKey(JSONFormat.OPT_STRUCTURED_QUERY)) {
    		return new StructuredSearchRequest(m_reader, object);
    	}
    	else if (object.containsKey(JSONFormat.OPT_KEYWORD_QUERY)) {
    		return new KeywordSearchRequest(m_reader, object);
    	}
    	else {
    		response.sendError(HttpServletResponse.SC_BAD_REQUEST, "missing options");
    	}
    	
    	return null;
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		response.getOutputStream().println("no get");
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		Object obj = null;
		try {
			obj = m_parser.parse(new InputStreamReader(request.getInputStream()));
		} catch (ParseException e) {
			e.printStackTrace();
			response.sendError(HttpServletResponse.SC_BAD_REQUEST, "malformed json");
			return;
		}
		
		if (!(obj instanceof JSONObject)) {
			response.sendError(HttpServletResponse.SC_BAD_REQUEST, "malformed json");
			return;
		}
		
		try {
			SearchRequest sr = createSearchRequest((JSONObject)obj, response);
			
			if (sr == null)
				return; // error set by createSearchRequest
			
			response.getOutputStream().println(sr.getResult());
		} catch (StorageException e) {
			throw new ServletException(e);
		}
		
	}
	
	public void destroy() {
	}
}
