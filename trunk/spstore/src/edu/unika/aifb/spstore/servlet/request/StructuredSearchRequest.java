package edu.unika.aifb.spstore.servlet.request;

import java.io.IOException;

import org.json.simple.JSONObject;

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.query.StructuredQuery;
import edu.unika.aifb.graphindex.searcher.structured.StructuredQueryEvaluator;
import edu.unika.aifb.graphindex.searcher.structured.VPEvaluator;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.spstore.servlet.JSONFormat;

public class StructuredSearchRequest extends SearchRequest {

	private StructuredQueryEvaluator m_evaluator;
	private StructuredQuery m_query;
	
	public StructuredSearchRequest(IndexReader reader, JSONObject obj) throws IOException {
		super(reader, obj);
		m_query = JSONFormat.structuredQuery((JSONObject)obj.get(JSONFormat.OPT_STRUCTURED_QUERY));
		System.out.println(m_query);
		m_evaluator = new VPEvaluator(m_reader);
	}

	@Override
	public String getResult() throws StorageException, IOException {
		Table<String> table = m_evaluator.evaluate(m_query);
		return JSONFormat.fromTable(table).toJSONString();
	}
}
