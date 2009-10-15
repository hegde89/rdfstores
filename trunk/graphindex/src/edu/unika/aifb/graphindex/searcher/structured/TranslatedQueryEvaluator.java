package edu.unika.aifb.graphindex.searcher.structured;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.HitCollector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.FSDirectory;

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.query.QueryEdge;
import edu.unika.aifb.graphindex.query.StructuredQuery;
import edu.unika.aifb.graphindex.searcher.hybrid.exploration.TranslatedQuery;
import edu.unika.aifb.graphindex.searcher.keyword.model.Constant;
import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordQNode;
import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordSegment;
import edu.unika.aifb.graphindex.storage.StorageException;

public class TranslatedQueryEvaluator extends StructuredQueryEvaluator {

	private org.apache.lucene.index.IndexReader m_valueReader;
	private QueryParser m_parser;
	private IndexSearcher m_searcher;
	private VPEvaluator m_vpEvaluator;
	
	private static final Logger log = Logger.getLogger(TranslatedQueryEvaluator.class);
	
	public TranslatedQueryEvaluator(IndexReader idxReader) throws StorageException {
		super(idxReader);
		
		try {
			m_valueReader = org.apache.lucene.index.IndexReader.open(FSDirectory.getDirectory(idxReader.getIndexDirectory().getDirectory(IndexDirectory.VALUE_DIR)), true);
			m_searcher = new IndexSearcher(m_valueReader);
			m_parser = new QueryParser(null, new StandardAnalyzer());
			m_parser.setDefaultOperator(QueryParser.AND_OPERATOR);
			m_vpEvaluator = new VPEvaluator(m_idxReader);
		} catch (CorruptIndexException e) {
			throw new StorageException(e);
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}
	
	private Table<String> getAttributeTable(QueryEdge edge) throws StorageException {
		final Table<String> table = new Table<String>(edge.getSource().getLabel(), edge.getTarget().getLabel());
		
		
		BooleanQuery query = new BooleanQuery();
		
		for (String keyword: ((KeywordQNode)edge.getTarget()).getKeywords()) {
			BooleanClause clause = new BooleanClause(new TermQuery(new Term(Constant.CONTENT_FIELD, keyword)), Occur.MUST);
			query.add(clause);
		}
		query.add(new BooleanClause(new TermQuery(new Term(Constant.ATTRIBUTE_FIELD, edge.getLabel())), Occur.MUST));
		
		try {

			final List<Integer> docIds = new ArrayList<Integer>();
			m_searcher.search(query, new HitCollector() {
				@Override
				public void collect(int docId, float score) {
					docIds.add(docId);
				}
			});
			
			Collections.sort(docIds);
			
			for (int docId : docIds) {
				Document doc = m_valueReader.document(docId);
				String[] row = new String[] { doc.getFieldable(Constant.URI_FIELD).stringValue(), edge.getTarget().getLabel() };
				table.addRow(row);
			}
		} catch (IOException e) {
			throw new StorageException(e);
		}
		
		return table;
	}

	@Override
	public Table<String> evaluate(StructuredQuery q) throws StorageException, IOException {
		if (!(q instanceof TranslatedQuery))
			throw new IllegalArgumentException("query has to be a translated query");
		
		TranslatedQuery tq = (TranslatedQuery)q;

		log.debug("attribute edges: " + tq.getAttributeEdges().size());
		
		QueryExecution qe = new QueryExecution(tq, m_idxReader);
		
		List<Table<String>> resultTables = new ArrayList<Table<String>>();
		for (QueryEdge edge : tq.getAttributeEdges()) {
			resultTables.add(getAttributeTable(edge));
			qe.visited(edge);
		}
		
		log.debug(resultTables);
		qe.setResultTables(resultTables);
		
		m_vpEvaluator.setQueryExecution(qe);
		m_vpEvaluator.evaluate(tq);

		if (qe.getResult() != null)
			return qe.getResult();
		else
			return new Table<String>();
	}

}
