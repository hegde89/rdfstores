package edu.unika.aifb.graphindex.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.jgrapht.DirectedGraph;
import org.jgrapht.experimental.isomorphism.IsomorphismRelation;
import org.semanticweb.kaon2.saturation.indexes.UnaryTermIndex.Results;

import edu.unika.aifb.graphindex.Result;
import edu.unika.aifb.graphindex.ResultSet;
import edu.unika.aifb.graphindex.StructureIndex;
import edu.unika.aifb.graphindex.StructureIndexReader;
import edu.unika.aifb.graphindex.algorithm.rcp.generic.GraphRCP;
import edu.unika.aifb.graphindex.algorithm.rcp.generic.LabelProvider;
import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.data.Tables;
import edu.unika.aifb.graphindex.data.Triple;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.IndexEdge;
import edu.unika.aifb.graphindex.graph.IndexGraph;
import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.NamedGraph;
import edu.unika.aifb.graphindex.graph.QueryGraph;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.graph.isomorphism.SubgraphMatcher;
import edu.unika.aifb.graphindex.graph.isomorphism.FeasibilityChecker;
import edu.unika.aifb.graphindex.graph.isomorphism.MappingListener;
import edu.unika.aifb.graphindex.graph.isomorphism.VertexMapping;
import edu.unika.aifb.graphindex.query.model.Constant;
import edu.unika.aifb.graphindex.query.model.Individual;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.query.model.Term;
import edu.unika.aifb.graphindex.query.model.Variable;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.ExtensionStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.StorageManager;
import edu.unika.aifb.graphindex.storage.lucene.LuceneExtensionStorage;
import edu.unika.aifb.graphindex.util.Timings;
import edu.unika.aifb.graphindex.util.Util;

public class QueryEvaluator implements IQueryEvaluator {
	private StructureIndexReader m_indexReader;
	private StructureIndex m_index;
	private ExtensionManager m_em;
	private ExtensionStorage m_es;
	private QueryMappingListener m_listener;
	private Timings m_timings;
	private Map<Graph<String>,JoinMatcher> m_matchers;
	private MappingListValidator m_mlv;
	static final Logger log = Logger.getLogger(QueryEvaluator.class);
	
	public static List<String> removeNodes = new ArrayList<String>();
	
	public QueryEvaluator(StructureIndexReader indexReader) {
		m_indexReader = indexReader;
		m_index = indexReader.getIndex();
		m_em = m_index.getExtensionManager();
		m_em.setMode(ExtensionManager.MODE_READONLY);
		m_es = m_em.getExtensionStorage();
		m_matchers = new HashMap<Graph<String>,JoinMatcher>();
		m_listener = new QueryMappingListener(m_indexReader);
		m_mlv = new MappingListValidator(m_indexReader, m_index.getCollector());
		for (Graph<String> ig : m_indexReader.getIndexGraphs()) {
			m_matchers.put(ig, new JoinMatcher(ig, m_listener, m_index));
		}
		
	}
	
	public int evaluate(Query query) throws StorageException, InterruptedException, ExecutionException {
		log.info("evaluating...");
		removeNodes = new ArrayList<String>(query.getRemoveNodes());
		log.debug("remove nodes: " + removeNodes);

		m_index.getCollector().reset();
		m_timings = new Timings();
		
		long start = System.currentTimeMillis();
		
		Graph<QueryNode> origGraph = query.toGraph();
		
//		final ExecutorService executor = Executors.newFixedThreadPool(1);
//		final ExecutorCompletionService<List<String[]>> completionService = new ExecutorCompletionService<List<String[]>>(executor);
		List<String[]> result = new ArrayList<String[]>();
		for (Graph<String> indexGraph : m_indexReader.getIndexGraphs()) {
			m_timings.start(Timings.SETUP);
			
//			Util.printDOT(query.getName() + ".dot", origGraph);
//			Util.printDOT(query.getName() + "_m.dot", qg);

			JoinMatcher matcher = m_matchers.get(indexGraph);
			
			matcher.setTimings(m_timings);
			matcher.setQueryGraph(origGraph);
			m_listener.setQueryGraph(origGraph);

			GTable.timings = null;
			Tables.timings = null;
			
			m_timings.end(Timings.SETUP);
			
			m_timings.start(Timings.MATCH);
			matcher.match();
			m_timings.end(Timings.MATCH);

			List<Map<String,String>> mappings = m_listener.generateMappings();
			
			if (mappings.size() == 0)
				continue;
			
			GTable.timings = m_timings;
			Tables.timings = m_timings;
			
			long vt = System.currentTimeMillis();
			List<String[]> res = m_mlv.validateMappings(origGraph, mappings, query.getEvalOrder());
			log.debug(System.currentTimeMillis() - vt);
			if (res.size() > 0)
				result.addAll(res);
		}
//		executor.shutdown();
//		log.debug("result maps: " + results.size());
		
//		if (result.size() < 50) {
//			for (String[] res : result) {
//				String s = "";
//				for (String r : res)
//					s += ">" + r + "< ";
//				log.info("\t" + s);
//			}
//		}
		log.debug("size: " + result.size());
		
		long end = System.currentTimeMillis();
		log.info("duration: " + (end - start) / 1000.0);
		
		m_index.getCollector().addTimings(m_timings);

//		if (result.size() > 0) {
//			Set<Map<String,String>> maps = new HashSet<Map<String,String>>();
//			for (String[] row : result) {
//				Map<String,String> map = new HashMap<String,String>();
//				for (int i = 0; i < row.length; i++)
//					map.put("" + i, row[i]);
//				maps.add(map);
//			}
//			for (Map<String,String> map : maps)
//				log.debug(map);
//			log.debug(maps.size());
//		}
		
//		if (result.size() > 0) {
//			for (int i = 0; i < result.get(0).length; i++) {
//				Set<String> vals = new HashSet<String>();
//				for (String[] row : result)
//					vals.add(row[i]);
//				log.debug(vals.size());
//			}
//		}
		
		((LuceneExtensionStorage)m_es).logStats(log);
		
		return result.size();
	}
	
	public void clearCaches() throws StorageException {
		m_mlv.clearCaches();
		m_es.clearCaches();
		((LuceneExtensionStorage)m_es).reopenAndWarmUp();
	}

	public long[] getTimings() {
		m_index.getCollector().logStats();
		return m_index.getCollector().getConsolidated();
	}
}
