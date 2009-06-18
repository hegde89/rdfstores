package edu.unika.aifb.graphindex.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SetBasedFieldSelector;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocCollector;

import edu.unika.aifb.graphindex.StructureIndex;
import edu.unika.aifb.graphindex.StructureIndexReader;
import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.data.Tables;
import edu.unika.aifb.graphindex.graph.GraphEdge;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.query.exploring.EdgeElement;
import edu.unika.aifb.graphindex.query.exploring.ExploringIndexMatcher;
import edu.unika.aifb.graphindex.query.exploring.GraphElement;
import edu.unika.aifb.graphindex.query.exploring.NodeElement;
import edu.unika.aifb.graphindex.query.matcher_v2.SmallIndexGraphMatcher;
import edu.unika.aifb.graphindex.query.matcher_v2.SmallIndexMatchesValidator;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Counters;
import edu.unika.aifb.graphindex.util.Timings;
import edu.unika.aifb.graphindex.util.TypeUtil;
import edu.unika.aifb.keywordsearch.Constant;
import edu.unika.aifb.keywordsearch.KeywordElement;
import edu.unika.aifb.keywordsearch.KeywordSegement;
import edu.unika.aifb.keywordsearch.api.IEntity;
import edu.unika.aifb.keywordsearch.impl.Entity;
import edu.unika.aifb.keywordsearch.search.KeywordQueryParser;
import edu.unika.aifb.keywordsearch.search.KeywordSearcher;

public class IndirectExploringQueryEvaluator extends ExploringQueryEvaluator {
	private StructureIndex m_schemaIndex;
	private ExploringIndexMatcher m_schemaMatcher;
	private KeywordSearcher m_schemaKS;

	private StructureIndex m_queryIndex;
	private IndexReader m_keywordReader;
	private IndexSearcher m_keywordSearcher;
	private HashSet<String> allAttributes;
	private KeywordSearcher m_queryKS;
	private VPEvaluator m_queryEvaluator;

	private static final Logger log = Logger.getLogger(IndirectExploringQueryEvaluator.class);

	public IndirectExploringQueryEvaluator(StructureIndexReader schemaReader, KeywordSearcher schemaKS,
			StructureIndexReader queryReader, KeywordSearcher queryKS) throws StorageException {
		m_schemaIndex = schemaReader.getIndex();
		m_schemaKS = schemaKS;
		for (String ig : schemaReader.getGraphNames()) {
			m_schemaMatcher = new ExploringIndexMatcher(m_schemaIndex, ig);
			m_schemaMatcher.initialize();
			break;
		}
		
		m_queryIndex = queryReader.getIndex();
		m_queryKS = queryKS;
		m_queryEvaluator = new VPEvaluator();
	}
	
	public void evaluate(String query) throws StorageException {
		Timings timings = new Timings();
		Counters counters = new Counters();
		
//		m_queryMatcher.setTimings(timings);
//		m_queryMatcher.setCounters(counters);
//		m_queryValidator.setTimings(timings);
//		m_queryValidator.setCounters(counters);
		m_schemaMatcher.setTimings(timings);
		m_schemaMatcher.setCounters(counters);
		m_queryIndex.getCollector().addTimings(timings);
		m_queryIndex.getCollector().addCounters(counters);
		GTable.timings = timings;
		Tables.timings = timings;

		log.info("evaluating...");
		timings.start(Timings.TOTAL_QUERY_EVAL);

		timings.start(Timings.STEP_KWSEARCH);
		Map<KeywordSegement,Collection<KeywordElement>> decomposition = search(query, m_schemaKS, timings);
		timings.end(Timings.STEP_KWSEARCH);
		
		List<GTable<String>> indexMatches = new ArrayList<GTable<String>>();
		List<Query> queries = new ArrayList<Query>();
		List<Map<String,Set<KeywordSegement>>> selectMappings = new ArrayList<Map<String,Set<KeywordSegement>>>();
		Map<KeywordSegement,List<GraphElement>> segment2elements = new HashMap<KeywordSegement,List<GraphElement>>();
		Map<String,Set<String>> ext2entities = new HashMap<String,Set<String>>();

		timings.start(Timings.STEP_EXPLORE);
		explore(decomposition, m_schemaMatcher, indexMatches, queries, selectMappings, segment2elements, ext2entities, 
			timings, counters);
		timings.end(Timings.STEP_EXPLORE);

		counters.set(Counters.QT_QUERIES, queries.size());

		int numberOfQueries = m_allQueries ? indexMatches.size() : Math.min(1, indexMatches.size());
		
		timings.start(Timings.STEP_QA);
		// execute queries
		for (int i = 0; i < numberOfQueries; i++) {
			Query q = queries.get(i);
			counters.set(Counters.QT_QUERY_EDGES, q.getLiterals().size());
			q.createQueryGraph(m_queryIndex);
			QueryExecution qe = new QueryExecution(q, m_queryIndex);
			log.debug(q);
			
			Map<String,Set<KeywordSegement>> select2ks = selectMappings.get(i);
			
			Map<KeywordSegement,Collection<KeywordElement>> keywordEntities = search(query, m_queryKS, timings);
			List<GTable<String>> resultTables = new ArrayList<GTable<String>>();
			for (String selectNode : select2ks.keySet()) {
				Set<String> keywords = new HashSet<String>();
				for (KeywordSegement ks : select2ks.get(selectNode))
					keywords.addAll(ks.getKeywords());

				List<String> columns = new ArrayList<String>();
				columns.add(keywords.toString());
				columns.add(selectNode);
				GTable<String> table = new GTable<String>(columns);
				
				Set<String> entities = new HashSet<String>(100);
				for (KeywordSegement ksOld : select2ks.get(selectNode)) {
					KeywordSegement ks = null;
					for (KeywordSegement k : keywordEntities.keySet())
						if (k.getKeywords().equals(ksOld.getKeywords()))
							ks = k;
					
					for (KeywordElement ele : keywordEntities.get(ks))
						if (ele.getType() == KeywordElement.ENTITY || ele.getType() == KeywordElement.CONCEPT) 
							entities.add(ele.getUri());
				}
					
				for (String entity : entities) {
					String[] row = new String [table.columnCount()];
					row[table.getColumn(selectNode)] = entity;
//					for (String keyword : keywords) 
//						row[table.getColumn(keyword)] = keyword;
					row[table.getColumn(keywords.toString())] = keywords.toString();
					table.addRow(row);
				}
				resultTables.add(table);
			}
			
			qe.setResultTables(resultTables);
			
			for (GraphEdge<QueryNode> edge : q.getGraph().edges()) {
				if (edge.getLabel().startsWith("???"))
					qe.visited(edge);
			}
			
			m_queryEvaluator.setQueryExecution(qe);
			
			m_queryEvaluator.evaluate();
			
			if (qe.getResult() != null)
				counters.inc(Counters.RESULTS, qe.getResult().rowCount());
		}
		
		timings.end(Timings.STEP_QA);
		timings.end(Timings.TOTAL_QUERY_EVAL);
	}

}
