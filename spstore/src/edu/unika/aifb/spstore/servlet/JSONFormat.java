package edu.unika.aifb.spstore.servlet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.query.KeywordQuery;
import edu.unika.aifb.graphindex.query.QueryEdge;
import edu.unika.aifb.graphindex.query.QueryGraph;
import edu.unika.aifb.graphindex.query.StructuredQuery;
import edu.unika.aifb.graphindex.searcher.hybrid.exploration.TranslatedQuery;

public class JSONFormat {
	public static final String OPT_KEYWORD_QUERY = "keyword-query";
	public static final String OPT_STRUCTURED_QUERY = "structured-query";
	public static final String OPT_TRANSLATION = "translation";
	public static final String OPT_SELECT = "select-variables";
	public static final String OPT_INTERPRETATIONS = "interpretations";
	public static final String OPT_WITHRESULTS = "with-results";
	public static final String OPT_TRIPLE_PATTERNS = "triple-patterns";
	public static final String OPT_COLUMNS = "columns";
	public static final String OPT_ROWS = "rows";
	public static final String OPT_RESULT = "result";
	
	public static class QueryTranslationInformation {
		private int m_numberOfInterpretations;

		private int m_withResults;
		
		public QueryTranslationInformation(int numberOfInterpretations, int withResults) {
			m_numberOfInterpretations = numberOfInterpretations;
			m_withResults = withResults;
		}
		
		public int getNumberOfInterpretations() {
			return m_numberOfInterpretations;
		}

		public int getWithResults() {
			return m_withResults;
		}
	}
	
	public static StructuredQuery structuredQuery(JSONObject object) {
		if (!object.containsKey(OPT_TRIPLE_PATTERNS) || !(object.get(OPT_TRIPLE_PATTERNS) instanceof JSONArray))
			throw new IllegalArgumentException("triple patterns missing or not array");
		if (!object.containsKey(OPT_SELECT) || !(object.get(OPT_SELECT) instanceof JSONArray))
			throw new IllegalArgumentException("select variables missing or not an array");
		
		StructuredQuery q = new StructuredQuery("from-json");
		
		// triple patterns
		JSONArray patterns = (JSONArray)object.get(OPT_TRIPLE_PATTERNS);
		System.out.println(patterns);
		for (Object patternObject : patterns) {
			JSONArray pattern = (JSONArray)patternObject;
			q.addEdge((String)pattern.get(0), (String)pattern.get(1), (String)pattern.get(2));
		}
		
		// select variables
		for (Object var : (JSONArray)object.get(OPT_SELECT))
			q.setAsSelect((String)var);
		
		return q;
	}
	
	public static QueryTranslationInformation translation(JSONObject object) {
		int interpretations = 0, withResults = 0;
		
		if (!object.containsKey(JSONFormat.OPT_INTERPRETATIONS))
			throw new IllegalArgumentException(OPT_INTERPRETATIONS + " missing in translation");
		if (!object.containsKey(JSONFormat.OPT_WITHRESULTS))
			throw new IllegalArgumentException(OPT_WITHRESULTS + " missing in translation");
		
		if (object.get(JSONFormat.OPT_INTERPRETATIONS) instanceof Number)
			interpretations = ((Number)object.get(JSONFormat.OPT_INTERPRETATIONS)).intValue();
		else if (object.get(JSONFormat.OPT_INTERPRETATIONS) instanceof String) {
			if (((String)object.get(JSONFormat.OPT_INTERPRETATIONS)).equals("all"))
				interpretations = -1;
			else {
				try {
					interpretations = Integer.parseInt((String)object.get(JSONFormat.OPT_INTERPRETATIONS));
				}
				catch (NumberFormatException e) {
					throw new IllegalArgumentException(JSONFormat.OPT_INTERPRETATIONS + " has to be either a number or 'all'");
				}
			}
		}
			
		if (object.get(JSONFormat.OPT_WITHRESULTS) instanceof Number)
			withResults = ((Number)object.get(JSONFormat.OPT_WITHRESULTS)).intValue();
		else if (object.get(JSONFormat.OPT_WITHRESULTS) instanceof String) {
			if (((String)object.get(JSONFormat.OPT_WITHRESULTS)).equals("all"))
				withResults = -1;
			else {
				try {
					interpretations = Integer.parseInt((String)object.get(JSONFormat.OPT_WITHRESULTS));
				}
				catch (NumberFormatException e) {
					throw new IllegalArgumentException(JSONFormat.OPT_WITHRESULTS + " has to be either a number or 'all'");
				}
			}
		}
		
		return new QueryTranslationInformation(interpretations, withResults);
	}
	
	@SuppressWarnings("unchecked")
	public static JSONObject fromTable(Table<String> table) {
		JSONObject obj = new JSONObject();

		obj.put(OPT_COLUMNS, Arrays.asList(table.getColumnNames()));
		
		List<List<String>> rows = new ArrayList<List<String>>(table.rowCount());
		for (String[] row : table) 
			rows.add(Arrays.asList(row));
		obj.put(OPT_ROWS, rows);
		
		return obj;
	}
	
	@SuppressWarnings("unchecked")
	public static JSONObject fromQuery(StructuredQuery query) {
		JSONObject obj = new JSONObject();
		
		obj.put(OPT_SELECT, query.getSelectVariableLabels());
		
		List<List<String>> patterns = new ArrayList<List<String>>();
		for (QueryEdge edge : query.getQueryGraph().edgeSet()) 
			patterns.add(Arrays.asList(edge.getSource().getLabel(), edge.getLabel(), edge.getTarget().getLabel()));
		obj.put(OPT_TRIPLE_PATTERNS, patterns);
		
		return obj;
	}
	
	@SuppressWarnings("unchecked")
	public static JSONObject fromTranslatedQuery(TranslatedQuery query) {
		JSONObject obj = new JSONObject();

		obj.put(OPT_STRUCTURED_QUERY, fromQuery(query));
		
		if (query.getResult() != null)
			obj.put(OPT_RESULT, fromTable(query.getResult()));
		else
			obj.put(OPT_RESULT, new JSONObject());
		
		return obj;
	}
}
