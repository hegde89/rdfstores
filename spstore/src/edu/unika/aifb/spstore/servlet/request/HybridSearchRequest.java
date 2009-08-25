package edu.unika.aifb.spstore.servlet.request;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONObject;

import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.query.HybridQuery;
import edu.unika.aifb.graphindex.query.KeywordQuery;
import edu.unika.aifb.graphindex.query.StructuredQuery;
import edu.unika.aifb.graphindex.searcher.hybrid.exploration.ExploringHybridQueryEvaluator;
import edu.unika.aifb.graphindex.searcher.hybrid.exploration.TranslatedQuery;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.spstore.servlet.JSONFormat;


public class HybridSearchRequest extends SearchRequest {

	private HybridQuery m_query;
	private int m_numberOfInterpretations = 1;
	private int m_interpretationResults = 0;
	private ExploringHybridQueryEvaluator m_evaluator;

	public HybridSearchRequest(IndexReader reader, JSONObject obj) throws IOException, StorageException {
		super(reader, obj);
		
		m_evaluator = new ExploringHybridQueryEvaluator(reader);

		KeywordQuery kq = new KeywordQuery("from-json", (String)obj.get(JSONFormat.OPT_KEYWORD_QUERY));
		StructuredQuery sq = JSONFormat.structuredQuery((JSONObject)obj.get(JSONFormat.OPT_STRUCTURED_QUERY));
		
		m_query = new HybridQuery("json-hybrid", sq, kq);
		
		JSONObject translation = (JSONObject)obj.get(JSONFormat.OPT_TRANSLATION);
		m_numberOfInterpretations = ((Number)translation.get(JSONFormat.OPT_INTERPRETATIONS)).intValue();
		m_interpretationResults = ((Number)translation.get(JSONFormat.OPT_WITHRESULTS)).intValue();
	}

	@SuppressWarnings("unchecked")
	@Override
	public String getResult() throws StorageException, IOException {
		List<TranslatedQuery> queries = m_evaluator.evaluate(m_query, m_numberOfInterpretations, m_interpretationResults);
		
		List interpretations = new ArrayList();
		for (TranslatedQuery q : queries)
			interpretations.add(JSONFormat.fromTranslatedQuery(q));
		
		JSONObject obj = new JSONObject();
		obj.put(JSONFormat.OPT_INTERPRETATIONS, interpretations);
		
		return obj.toJSONString();
	}
}
