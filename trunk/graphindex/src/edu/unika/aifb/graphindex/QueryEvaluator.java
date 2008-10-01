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
import edu.unika.aifb.graphindex.graph.Edge;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.IndexEdge;
import edu.unika.aifb.graphindex.graph.IndexGraph;
import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.NamedGraph;
import edu.unika.aifb.graphindex.graph.isomorphism.DiGraphMatcher;
import edu.unika.aifb.graphindex.graph.isomorphism.DiGraphMatcher2;
import edu.unika.aifb.graphindex.graph.isomorphism.DiGraphMatcher3;
import edu.unika.aifb.graphindex.graph.isomorphism.EdgeLabelFeasibilityChecker;
import edu.unika.aifb.graphindex.graph.isomorphism.FeasibilityChecker;
import edu.unika.aifb.graphindex.graph.isomorphism.FeasibilityChecker3;
import edu.unika.aifb.graphindex.graph.isomorphism.MappingListener;
import edu.unika.aifb.graphindex.query.Constant;
import edu.unika.aifb.graphindex.query.Individual;
import edu.unika.aifb.graphindex.query.LabeledQueryEdge;
import edu.unika.aifb.graphindex.query.NamedQueryGraph;
import edu.unika.aifb.graphindex.query.Query;
import edu.unika.aifb.graphindex.query.QueryGraph;
import edu.unika.aifb.graphindex.query.QueryVertex;
import edu.unika.aifb.graphindex.query.Term;
import edu.unika.aifb.graphindex.query.Variable;
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
		
		initialize();
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
	
	private void initialize() {
		for (NamedGraph<String,LabeledEdge<String>> indexGraph : m_index.getIndexGraphs()) {
			log.debug("index graph: " + indexGraph);
//			indexGraph.calc();
		}
		log.info("evaluator initialized, " + Util.memory());
	}
	
	public ResultSet evaluate(Query query) throws StorageException, InterruptedException, ExecutionException {
		log.info("evaluating...");
		
		long start = System.currentTimeMillis();
		m_timings = new Timings();
		
		final NamedQueryGraph<String,LabeledQueryEdge<String>> queryGraph = query.toQueryGraph();
		queryGraph.calc();
		log.debug("query graph: " + queryGraph);
		Util.printDOT("query.dot", queryGraph);
		
		m_em.setMode(ExtensionManager.MODE_READONLY);
		
		final ExecutorService executor = Executors.newFixedThreadPool(1);
		final ExecutorCompletionService<Set<Map<String,String>>> completionService = new ExecutorCompletionService<Set<Map<String,String>>>(executor);
		
		Set<Map<String,String>> results = new HashSet<Map<String,String>>();
		for (NamedGraph<String,LabeledEdge<String>> indexGraph : m_index.getIndexGraphs()) {
			
//			DiGraphMatcher3 matcher = new DiGraphMatcher3(queryGraph, indexGraph, true, 
//					new FeasibilityChecker<String,LabeledEdge<String>,DirectedGraph<String,LabeledEdge<String>>>() {
//						public boolean isEdgeCompatible(LabeledEdge<String> e1, LabeledEdge<String> e2) {
//							return e1.getLabel().equals(e2.getLabel());
//						}
//		
//						public boolean isVertexCompatible(String n1, String n2) {
////							return checkVertexCompatible(n1, n2);
//							if (!n1.startsWith("?")) {
//								Boolean value = m_gtc.get(n1, n2);
//								if (value != null)
//									return value.booleanValue();
//							}
//							return true;
//						}
//						
//						public boolean checkVertexCompatible(String n1, String n2) {
//							if (!n1.startsWith("?")) { // not variable, ie. ground term
//								m_timings.start(Timings.GT);
//								Boolean value = m_gtc.get(n1, n2);
//								if (value == null) {
//									for (String label : queryGraph.inEdgeLabels(n1)) {
//										try {
//											if (m_les.hasDocs(n2, label, n1)) {
//												value = true;
//												break;
//											}
//											else {
//												value = false;
//												break;
//											}
//										} catch (StorageException e) {
//											e.printStackTrace();
//										}
//									}
//									m_gtc.put(n1, n2, value);
//								}
//								m_timings.end(Timings.GT);
//								return value.booleanValue();
//							}
//							return true;
//						}
//					},
//					new MappingListener<String,LabeledEdge<String>>() {
//						public void mapping(IsomorphismRelation<String,LabeledEdge<String>> iso) {
//							completionService.submit(new MappingValidator(queryGraph, iso, m_gtc, m_invalidVertices, m_collector));
////							log.debug("mapping " + iso);
//						}
//			});
			
			final IndexGraph qg = new IndexGraph(queryGraph);
			final IndexGraph ig = new IndexGraph(indexGraph);
			DiGraphMatcher3 matcher = new DiGraphMatcher3(qg, ig, true, 
					new FeasibilityChecker3() {
						public boolean isEdgeCompatible(IndexEdge e1, IndexEdge e2) {
							return e1.getLabel().equals(e2.getLabel());
						}
		
						public boolean isVertexCompatible(int n1, int n2) {
//							return checkVertexCompatible(n1, n2);
							Boolean value = m_vcc.get(n1, n2);
							if (value != null)
								return value;
							
							return true;
						}
						
						public boolean checkVertexCompatible(int n1, int n2) {
							Boolean value = m_vcc.get(n1, n2);
							if (value != null)
								return value;
							
							String l1 = qg.getNodeLabel(n1);
							if (!l1.startsWith("?")) { // not variable, ie. ground term
								String l2 = ig.getNodeLabel(n2);
								m_timings.start(Timings.GT);
								for (String label : qg.inEdgeLabels(n1)) {
									try {
										if (m_les.hasDocs(l2, label, l1)) {
											value = true;
											break;
										}
										else {
											value = false;
											break;
										}
									} catch (StorageException e) {
										e.printStackTrace();
									}
								}
								m_vcc.put(n1, n2, value);
								m_timings.end(Timings.GT);
								return value.booleanValue();
							}
							return true;
						}
					},
					new MappingListener<String,LabeledEdge<String>>() {
						public void mapping(IsomorphismRelation<String,LabeledEdge<String>> iso) {
							completionService.submit(new MappingValidator(queryGraph, iso, null, m_invalidVertices, m_collector));
//							log.debug("mapping " + iso);
						}
			});
			
			m_timings.start(Timings.MATCH);
			if (!matcher.isSubgraphIsomorphic()) {
				log.debug("matches: 0");
				m_timings.end(Timings.MATCH);
				continue;
			}
			m_timings.end(Timings.MATCH);
			
			log.info("matches: " + matcher.numberOfMappings());
			log.debug("pairs generated: " + matcher.pairs);

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
		log.debug(rs);
		log.debug("size: " + rs.size());
		
		long end = System.currentTimeMillis();
		log.info("duration: " + (end - start) / 1000.0);

		m_collector.addTimings(m_timings);
		m_collector.logStats();
		
		log.debug("cache size: " + m_vcc.size());
		m_vcc.clear();

		return null;
	}
}
