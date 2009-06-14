package edu.unika.aifb.graphindex.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.StructureIndex;
import edu.unika.aifb.graphindex.StructureIndexReader;
import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.data.Tables;
import edu.unika.aifb.graphindex.query.exploring.EdgeElement;
import edu.unika.aifb.graphindex.query.exploring.ExploringIndexMatcher;
import edu.unika.aifb.graphindex.query.exploring.GraphElement;
import edu.unika.aifb.graphindex.query.exploring.NodeElement;
import edu.unika.aifb.graphindex.query.matcher_v2.SmallIndexMatchesValidator;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Counters;
import edu.unika.aifb.graphindex.util.Timings;
import edu.unika.aifb.keywordsearch.KeywordElement;
import edu.unika.aifb.keywordsearch.KeywordSegement;
import edu.unika.aifb.keywordsearch.search.KeywordSearcher;

public class IndirectExploringQueryEvaluator extends ExploringQueryEvaluator {
	private StructureIndex m_schemaIndex;
	private ExploringIndexMatcher m_schemaMatcher;
	private KeywordSearcher m_schemaKS;

	private StructureIndex m_queryIndex;
	private IndexGraphMatcher m_queryMatcher;
	private IndexMatchesValidator m_queryValidator;
	private KeywordSearcher m_queryKS;

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
		for (String ig : queryReader.getGraphNames()) {
			m_queryMatcher = new ExploringIndexMatcher(m_queryIndex, ig);
			m_queryMatcher.initialize();
			break;
		}
		m_queryValidator = new SmallIndexMatchesValidator(m_queryIndex, m_queryIndex.getCollector());
	}
	public void evaluate(String query) throws StorageException {
		Timings timings = new Timings();
		Counters counters = new Counters();
		
		m_queryMatcher.setTimings(timings);
		m_queryMatcher.setCounters(counters);
		m_queryValidator.setTimings(timings);
		m_queryValidator.setCounters(counters);
		m_schemaIndex.getCollector().addTimings(timings);
		m_schemaIndex.getCollector().addCounters(counters);
		GTable.timings = timings;
		Tables.timings = timings;

		log.info("evaluating...");
		timings.start(Timings.TOTAL_QUERY_EVAL);

		List<GTable<String>> indexMatches = new ArrayList<GTable<String>>();
		List<Query> queries = new ArrayList<Query>();
		List<Map<String,KeywordSegement>> selectMappings = new ArrayList<Map<String,KeywordSegement>>();
		Map<KeywordSegement,List<GraphElement>> segment2elements = new HashMap<KeywordSegement,List<GraphElement>>();
		Map<String,Set<String>> ext2entities = new HashMap<String,Set<String>>();

		searchAndExplore(query, m_schemaMatcher, m_schemaKS, indexMatches, queries, selectMappings, segment2elements, ext2entities, 
			timings, counters);

		timings.start(Timings.STEP_INDIRECT_QUERY);
		// execute queries
		for (int i = 0; i < indexMatches.size(); i++) {
			Query q = queries.get(i);
			log.debug(q);
			
			QueryExecution qe = new QueryExecution(q, m_queryIndex);
			
			m_queryMatcher.setQueryExecution(qe);
		}
		timings.end(Timings.STEP_INDIRECT_QUERY);

		timings.end(Timings.TOTAL_QUERY_EVAL);
	}

}
