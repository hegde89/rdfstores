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

import edu.unika.aifb.graphindex.Result;
import edu.unika.aifb.graphindex.ResultSet;
import edu.unika.aifb.graphindex.StructureIndex;
import edu.unika.aifb.graphindex.StructureIndexReader;
import edu.unika.aifb.graphindex.algorithm.rcp.generic.GraphRCP;
import edu.unika.aifb.graphindex.algorithm.rcp.generic.LabelProvider;
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
import edu.unika.aifb.graphindex.util.Timings;
import edu.unika.aifb.graphindex.util.Util;

public class QueryEvaluator {
	private StructureIndexReader m_indexReader;
	private StructureIndex m_index;
	private ExtensionManager m_em;
	private ExtensionStorage m_es;
	private Timings m_timings;
	
	static final Logger log = Logger.getLogger(QueryEvaluator.class);
	
	
	public QueryEvaluator(StructureIndexReader indexReader) {
		m_indexReader = indexReader;
		m_index = indexReader.getIndex();
		m_em = m_index.getExtensionManager();
		m_es = m_em.getExtensionStorage();
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
		System.gc();
		log.info("evaluating...");
		
		m_index.getCollector().reset();
		m_timings = new Timings();
		long start = System.currentTimeMillis();
		
		m_timings.start(Timings.RCP);
		GraphRCP<String,String> rcp = new GraphRCP<String,String>(new LabelProvider<String,String>() {
			public String getEdgeLabel(String edge) {
				return edge;
			}

			public String getVertexLabel(String vertex) {
				return vertex;
			}
		});
		NamedQueryGraph<String,LabeledQueryEdge<String>> queryGraph = query.toQueryGraph();
		
		Graph<QueryNode> origGraph = query.toGraph();
		Graph<QueryNode> qg = rcp.createQueryGraph(queryGraph);
		m_timings.end(Timings.RCP);

		log.debug("orig query graph: " + origGraph);
		log.debug("query graph: " + qg);
//		Util.printDOT("query_orig.dot", origGraph);
//		Util.printDOT("query.dot", qg);
		
		VCompatibilityCache vcc = new VCompatibilityCache(origGraph, m_index);
		
		m_em.setMode(ExtensionManager.MODE_READONLY);
		
		final ExecutorService executor = Executors.newFixedThreadPool(1);
		final ExecutorCompletionService<Set<Map<String,String>>> completionService = new ExecutorCompletionService<Set<Map<String,String>>>(executor);
		
		Set<Map<String,String>> results = new HashSet<Map<String,String>>();
		for (Graph<String> indexGraph : m_indexReader.getIndexGraphs()) {
			if (indexGraph.nodeCount() < 10)
				continue;
			vcc.setCurrentIndexGraph(indexGraph);
			
			Util.printDOT(query.getName() + ".dot", origGraph);
			Util.printDOT(query.getName() + "_m.dot", qg);

			QueryMappingListener listener = new QueryMappingListener(origGraph, qg, indexGraph, m_indexReader, vcc, completionService);
			JoinMatcher jm = new JoinMatcher(qg, indexGraph, listener, vcc, m_timings);

			m_timings.start(Timings.MATCH);
			jm.match2();
			m_timings.end(Timings.MATCH);

			m_timings.start(Timings.MAPGEN);
			int mappingsCount = listener.generateMappings();
			m_timings.end(Timings.MAPGEN);
			
			if (mappingsCount == 0)
				continue;
			
			listener = null;

			for (int i = 0; i < mappingsCount; i++) {
				Future<Set<Map<String,String>>> f = completionService.take();
				Set<Map<String,String>> r = f.get();
				if (r != null)
					results.addAll(r);
			}
		}
		executor.shutdown();
//		log.debug("result maps: " + results.size());
		
		ResultSet rs = toResultSet(results, query.getVariables());
		if (rs.size() < 50)
			log.debug(rs);
		log.debug("size: " + rs.size());
		
		long end = System.currentTimeMillis();
		log.info("duration: " + (end - start) / 1000.0);

		m_index.getCollector().addTimings(m_timings);
		m_index.getCollector().logStats();
		
		log.debug("vcc size: " + vcc.size());
		vcc.clear();

		return null;
	}
}
