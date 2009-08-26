package edu.unika.aifb.spstore.servlet.request;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONObject;

import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.query.HybridQuery;
import edu.unika.aifb.graphindex.query.KeywordQuery;
import edu.unika.aifb.graphindex.searcher.hybrid.exploration.ExploringHybridQueryEvaluator;
import edu.unika.aifb.graphindex.searcher.hybrid.exploration.TranslatedQuery;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.spstore.servlet.JSONFormat;

public class KeywordSearchRequest extends SearchRequest {

	private KeywordQuery m_query;
	private int m_numberOfInterpretations = 1;
	private int m_interpretationResults = 0;
	private ExploringHybridQueryEvaluator m_evaluator;

	public KeywordSearchRequest(IndexReader reader, JSONObject obj) throws IOException, StorageException, IllegalArgumentException {
		super(reader, obj);
		
		m_evaluator = new ExploringHybridQueryEvaluator(reader);

		m_query = new KeywordQuery("from-json", (String)obj.get(JSONFormat.OPT_KEYWORD_QUERY));
		
		JSONObject translation = (JSONObject)obj.get(JSONFormat.OPT_TRANSLATION);
		if (translation.get(JSONFormat.OPT_INTERPRETATIONS) instanceof Number)
			m_numberOfInterpretations = ((Number)translation.get(JSONFormat.OPT_INTERPRETATIONS)).intValue();
		else if (translation.get(JSONFormat.OPT_INTERPRETATIONS) instanceof String) {
			if (((String)translation.get(JSONFormat.OPT_INTERPRETATIONS)).equals("all"))
				m_numberOfInterpretations = -1;
			else
				throw new IllegalArgumentException(JSONFormat.OPT_INTERPRETATIONS + " has to be either a number or 'all'");
		}
			
		if (translation.get(JSONFormat.OPT_WITHRESULTS) instanceof Number)
			m_interpretationResults = ((Number)translation.get(JSONFormat.OPT_WITHRESULTS)).intValue();
		else if (translation.get(JSONFormat.OPT_WITHRESULTS) instanceof String) {
			if (((String)translation.get(JSONFormat.OPT_WITHRESULTS)).equals("all"))
				m_interpretationResults = -1;
			else
				throw new IllegalArgumentException(JSONFormat.OPT_WITHRESULTS + " has to be either a number or 'all'");
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public String getResult() throws StorageException, IOException {
		List<TranslatedQuery> queries = m_evaluator.evaluate(new HybridQuery("hybrid", null, m_query), m_numberOfInterpretations, m_interpretationResults);
		
		List interpretations = new ArrayList();
		for (TranslatedQuery q : queries)
			interpretations.add(JSONFormat.fromTranslatedQuery(q));
		
		JSONObject obj = new JSONObject();
		obj.put(JSONFormat.OPT_INTERPRETATIONS, interpretations);
		
		return obj.toJSONString();
	}

}
