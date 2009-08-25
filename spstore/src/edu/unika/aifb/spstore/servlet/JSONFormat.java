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
