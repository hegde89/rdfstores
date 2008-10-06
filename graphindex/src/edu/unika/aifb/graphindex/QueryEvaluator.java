package edu.unika.aifb.graphindex;

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

import edu.unika.aifb.graphindex.data.Triple;
import edu.unika.aifb.graphindex.graph.IndexEdge;
import edu.unika.aifb.graphindex.graph.IndexGraph;
import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.NamedGraph;
import edu.unika.aifb.graphindex.graph.isomorphism.SubgraphMatcher;
import edu.unika.aifb.graphindex.graph.isomorphism.FeasibilityChecker;
import edu.unika.aifb.graphindex.graph.isomorphism.MappingListener;
import edu.unika.aifb.graphindex.graph.isomorphism.VertexMapping;
import edu.unika.aifb.graphindex.query.LabeledQueryEdge;
import edu.unika.aifb.graphindex.query.NamedQueryGraph;
import edu.unika.aifb.graphindex.query.model.Constant;
import edu.unika.aifb.graphindex.query.model.Individual;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.query.model.Term;
import edu.unika.aifb.graphindex.query.model.Variable;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.StorageManager;
import edu.unika.aifb.graphindex.storage.lucene.LuceneExtensionStorage;

public class QueryEvaluator {
	private ExtensionManager m_em = StorageManager.getInstance().getExtensionManager();
	private LuceneExtensionStorage m_les = (LuceneExtensionStorage)m_em.getExtensionStorage();
	private final VCompatibilityCache m_vcc;
	private final Set<String> m_invalidVertices;
	private StructureIndex m_index;
	private final StatisticsCollector m_collector = new StatisticsCollector();
	private Timings m_timings;
	
	static final Logger log = Logger.getLogger(QueryEvaluator.class);
	
	
	public QueryEvaluator(StructureIndex index) {
		m_vcc = new VCompatibilityCache();
		m_invalidVertices = new HashSet<String>();
		m_index = index;
	}
	
	private ResultSet toResultSet(Set<Map<String,String>> mappings, Set<String> vars) {
		m_timings.start(Timings.RS);
		ResultSet rs = new ResultSet(vars.toArray(new String[]{}));
		Set<String> toRemove = null;
		
		for (Map<String,String> map : mappings) {
			if (toRemove == null) {
				toRemove = new HashSet<String>();
				for (String key : map.keySet())
					if (!vars.contains(key))
						toRemove.add(key);
			}
			for (String key : toRemove)
				map.remove(key);
			rs.addResult(new Result(map));
		}
		m_timings.end(Timings.RS);
		return rs;
	}
	
	public ResultSet evaluate(Query query) throws StorageException, InterruptedException, ExecutionException {
		log.info("evaluating...");
		long start = System.currentTimeMillis();
		m_timings = new Timings();
		
		final NamedQueryGraph<String,LabeledQueryEdge<String>> queryGraph = query.toQueryGraph();
		queryGraph.calc();
		IndexGraph qg = new IndexGraph(queryGraph, 1);

		log.debug("query graph: " + queryGraph);
		Util.printDOT("query.dot", queryGraph);
		
		m_em.setMode(ExtensionManager.MODE_READONLY);
		
		final ExecutorService executor = Executors.newFixedThreadPool(1);
		final ExecutorCompletionService<Set<Map<String,String>>> completionService = new ExecutorCompletionService<Set<Map<String,String>>>(executor);
		
		Set<Map<String,String>> results = new HashSet<Map<String,String>>();
		for (IndexGraph indexGraph : m_index.getIndexGraphs()) {
			
			SubgraphMatcher matcher = new SubgraphMatcher(qg, indexGraph, true, new QueryFeasibilityChecker(qg, indexGraph, m_vcc, m_timings, m_les), 
					new MappingListener() {
						public void mapping(VertexMapping mapping) {
							completionService.submit(new MappingValidator(queryGraph, mapping, null, m_invalidVertices, m_collector));
						}
					});
			
			log.debug("matcher created");
			
			m_timings.start(Timings.MATCH);
			boolean found = matcher.isSubgraphIsomorphic();
			m_timings.end(Timings.MATCH);
			
			log.info("matches: " + matcher.numberOfMappings());
			log.debug("pairs generated: " + matcher.pairs);
			
			if (!found)
				continue;

			for (int i = 0; i < matcher.numberOfMappings(); i++) {
				Future<Set<Map<String,String>>> f = completionService.take();
				Set<Map<String,String>> r = f.get();
				if (r != null)
					results.addAll(r);
			}
		}
		executor.shutdown();
		log.debug("result maps: " + results.size());
		
		ResultSet rs = toResultSet(results, queryGraph.getVariables());
		if (rs.size() < 1000)
			log.debug(rs);
		log.debug("size: " + rs.size());
		
		long end = System.currentTimeMillis();
		log.info("duration: " + (end - start) / 1000.0);

		m_collector.addTimings(m_timings);
		m_collector.logStats();
		
		log.debug("vcc size: " + m_vcc.size());
		m_vcc.clear();

		return null;
	}
}
