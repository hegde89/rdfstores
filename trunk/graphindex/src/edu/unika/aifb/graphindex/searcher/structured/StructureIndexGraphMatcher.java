package edu.unika.aifb.graphindex.searcher.structured;

import java.io.IOException;

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.query.StructuredQuery;
import edu.unika.aifb.graphindex.searcher.structured.sig.SmallIndexGraphMatcher;
import edu.unika.aifb.graphindex.storage.StorageException;

public class StructureIndexGraphMatcher extends StructuredQueryEvaluator {

	private SmallIndexGraphMatcher m_matcher;
	
	public StructureIndexGraphMatcher(IndexReader idxReader) throws IOException, StorageException {
		super(idxReader);
		m_matcher = new SmallIndexGraphMatcher(idxReader);
		m_matcher.initialize();
	}

	@Override
	public Table<String> evaluate(StructuredQuery q) throws StorageException, IOException {
		QueryExecution qe = new QueryExecution(q, m_idxReader);
		m_matcher.setQueryExecution(qe);
		m_matcher.match();
		
		return qe.getIndexMatches();
	}

}
