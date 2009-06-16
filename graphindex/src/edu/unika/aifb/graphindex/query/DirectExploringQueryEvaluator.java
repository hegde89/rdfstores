package edu.unika.aifb.graphindex.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.StructureIndex;
import edu.unika.aifb.graphindex.StructureIndexReader;
import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.data.Tables;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphEdge;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.query.exploring.ExploringIndexMatcher;
import edu.unika.aifb.graphindex.query.exploring.GraphElement;
import edu.unika.aifb.graphindex.query.matcher_v2.SmallIndexMatchesValidator;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Counters;
import edu.unika.aifb.graphindex.util.Timings;
import edu.unika.aifb.keywordsearch.KeywordElement;
import edu.unika.aifb.keywordsearch.KeywordSegement;
import edu.unika.aifb.keywordsearch.search.KeywordSearcher;

public class DirectExploringQueryEvaluator extends ExploringQueryEvaluator {

	private StructureIndex m_index;
	private ExploringIndexMatcher m_matcher;
	private IndexMatchesValidator m_validator;
	private KeywordSearcher m_searcher;
	
	private static final Logger log = Logger.getLogger(DirectExploringQueryEvaluator.class);

	public DirectExploringQueryEvaluator(StructureIndexReader reader, KeywordSearcher searcher) throws StorageException {
		m_index = reader.getIndex();
		m_searcher = searcher;
		
		for (String ig : reader.getGraphNames()) {
			m_matcher = new ExploringIndexMatcher(m_index, ig);
			m_matcher.initialize();
			break;
		}
		m_validator = new SmallIndexMatchesValidator(m_index, m_index.getCollector());
	}

	public void evaluate(String query) throws StorageException {
		Timings timings = new Timings();
		Counters counters = new Counters();
		
		m_matcher.setTimings(timings);
		m_matcher.setCounters(counters);
		m_validator.setTimings(timings);
		m_validator.setCounters(counters);
		m_index.getCollector().addTimings(timings);
		m_index.getCollector().addCounters(counters);
		GTable.timings = timings;
		Tables.timings = timings;

		log.info("evaluating...");
		timings.start(Timings.TOTAL_QUERY_EVAL);

		timings.start(Timings.STEP_KWSEARCH);
		Map<KeywordSegement,Collection<KeywordElement>> decomposition = search(query, m_searcher, timings);
		timings.end(Timings.STEP_KWSEARCH);

		List<GTable<String>> indexMatches = new ArrayList<GTable<String>>();
		List<Query> queries = new ArrayList<Query>();
		List<Map<String,Set<KeywordSegement>>> selectMappings = new ArrayList<Map<String,Set<KeywordSegement>>>();
		Map<KeywordSegement,List<GraphElement>> segment2elements = new HashMap<KeywordSegement,List<GraphElement>>();
		Map<String,Set<String>> ext2entities = new HashMap<String,Set<String>>();

		timings.start(Timings.STEP_EXPLORE);
		explore(decomposition, m_matcher, indexMatches, queries, selectMappings, segment2elements, ext2entities, timings, counters);
		timings.end(Timings.STEP_EXPLORE);
		
		timings.start(Timings.STEP_IQA);
		
		counters.set(Counters.QT_QUERIES, queries.size());
		
		
		for (int i = 0; i < Math.min(1, indexMatches.size()); i++) {
			Query q = queries.get(i);
			counters.set(Counters.QT_QUERY_SIZE, q.getLiterals().size());
			log.debug(q);
			q.createQueryGraph(m_index);
			QueryExecution qe = new QueryExecution(q, m_index);
			Graph<QueryNode> queryGraph = qe.getQueryGraph();
			
			GTable<String> indexMatch = indexMatches.get(i);
			qe.setIndexMatches(indexMatch);
			log.debug(indexMatch);
			
			Map<String,Set<KeywordSegement>> select2ks = selectMappings.get(i);
			
			List<EvaluationClass> classes = new ArrayList<EvaluationClass>();
			classes.add(new EvaluationClass(indexMatch));
			
			for (GraphEdge<QueryNode> edge : queryGraph.edges()) {
				String src = queryGraph.getSourceNode(edge).getName();
				String trg = queryGraph.getTargetNode(edge).getName();
				
				if (q.getSelectVariables().contains(src)) {
					List<EvaluationClass> newClasses = new ArrayList<EvaluationClass>();
					for (EvaluationClass ec : classes) {
						newClasses.addAll(ec.addMatch(src, false, null, null));
					}
					classes.addAll(newClasses);					
				}
				
				if (q.getSelectVariables().contains(trg)) {
					List<EvaluationClass> newClasses = new ArrayList<EvaluationClass>();
					for (EvaluationClass ec : classes) {
						newClasses.addAll(ec.addMatch(trg, false, null, null));
					}
					classes.addAll(newClasses);					
				}
				
				if (edge.getLabel().startsWith("???")) {
					qe.visited(edge);
				}
			}
//			log.debug("ext2entities: " + ext2entities.keySet());
			for (Iterator<EvaluationClass> j = classes.iterator(); j.hasNext(); ) {
				EvaluationClass ec = j.next();
				log.debug(ec);
				for (String selectNode : q.getSelectVariables()) {
					if (!ec.getMappings().hasColumn(selectNode))
						continue;
					if (q.getRemovedNodes().contains(selectNode))
						continue;

					boolean stop = false;
					String ksCol = "";
					for (KeywordSegement ks : select2ks.get(selectNode)) {
						ksCol += ks.toString().replaceAll(" ", "_");
//						log.debug("ks: " + ks + ", " + getKSId(ks) + ": " + ec.getMatch(selectNode) + getKSId(ks));
						if (ext2entities.get(ec.getMatch(selectNode) + getKSId(ks)) == null) {
							j.remove();
							stop = true;
//							log.debug("stop");
							break;
						}
					}
					
					if (stop)
						break;

					List<String> columns = new ArrayList<String>();
					columns.add(ksCol);
					columns.add(selectNode);
					GTable<String> table = new GTable<String>(columns);
					int col = table.getColumn(selectNode);
	
					for (KeywordSegement ks : select2ks.get(selectNode)) {
						for (String entity : ext2entities.get(ec.getMatch(selectNode) + getKSId(ks))) {
							String[] row = new String [2];
							row[col] = entity;
							row[0] = ksCol;
							table.addRow(row);
						}
					}
					ec.getResults().add(table);
//					log.debug(table.toDataString());
				}
//					log.debug(ec);
			}
			
			log.debug(classes);
			qe.setEvaluationClasses(classes);
			m_validator.setQueryExecution(qe);
			
			if (classes.size() > 0)
				m_validator.validateIndexMatches();
			
			log.debug("result: " + qe.getResult());
			
			if (qe.getResult() != null)
				counters.inc(Counters.RESULTS, qe.getResult().rowCount());
		}

		timings.end(Timings.STEP_IQA);
		
		timings.end(Timings.TOTAL_QUERY_EVAL);
	}
	
	private String getKSId(KeywordSegement ks) {
		List<String> keywords = new ArrayList<String>(ks.getKeywords());
		Collections.sort(keywords);
		return keywords.toString();
	}
}
