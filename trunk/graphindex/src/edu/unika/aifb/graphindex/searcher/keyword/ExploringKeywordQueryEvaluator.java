package edu.unika.aifb.graphindex.searcher.keyword;

import java.io.IOException;

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.query.HybridQuery;
import edu.unika.aifb.graphindex.query.KeywordQuery;
import edu.unika.aifb.graphindex.searcher.hybrid.HybridQueryEvaluator;
import edu.unika.aifb.graphindex.searcher.hybrid.exploration.ExploringHybridQueryEvaluator;
import edu.unika.aifb.graphindex.storage.StorageException;

public class ExploringKeywordQueryEvaluator extends KeywordQueryEvaluator {

	private HybridQueryEvaluator m_eval;
	
	public ExploringKeywordQueryEvaluator(IndexReader idxReader) throws IOException, StorageException {
		super(idxReader);
		m_eval = new ExploringHybridQueryEvaluator(idxReader);
	}

	@Override
	public Table<String> evaluate(KeywordQuery query) throws StorageException, IOException {
		return m_eval.evaluate(new HybridQuery(query.getName(), null, query));
	}

}
