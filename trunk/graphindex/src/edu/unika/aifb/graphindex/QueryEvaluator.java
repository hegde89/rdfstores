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

import edu.unika.aifb.graphindex.graph.Edge;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.NamedGraph;
import edu.unika.aifb.graphindex.graph.isomorphism.DiGraphMatcher;
import edu.unika.aifb.graphindex.graph.isomorphism.DiGraphMatcher2;
import edu.unika.aifb.graphindex.graph.isomorphism.EdgeLabelFeasibilityChecker;
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
import edu.unika.aifb.graphindex.storage.Triple;

public class QueryEvaluator {
	private ExtensionManager m_em = StorageManager.getInstance().getExtensionManager();
	private final Map<String,Boolean> m_groundTermCache;
	static final Logger log = Logger.getLogger(QueryEvaluator.class);
	private long[] starts = new long[10];
	private long[] timings = new long[10];
	private final  int DATA = 0, JOIN = 1, MAPPING = 2, RS = 3, MATCH = 4;
	
	public QueryEvaluator() {
		m_groundTermCache = new HashMap<String,Boolean>();
	}
	
	private void start(int timer) {
		starts[timer] = System.currentTimeMillis();
	}
	
	private void end(int timer) {
		timings[timer] += System.currentTimeMillis() - starts[timer];
	}
	
	private ResultSet toResultSet(Set<Map<String,String>> mappings, Set<String> vars) {
		start(RS);
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
		end(RS);
		return rs;
	}
	
//	private Set<Map<String,String>> createMappings(NamedQueryGraph<String,LabeledQueryEdge<String>> queryGraph, Map<String,TripleSet> tripleMapping) {
//		start(MAPPING);
//		
//		Set<Map<String,String>> mappings = new HashSet<Map<String,String>>();
//		
//		Set<String> notVisited = new HashSet<String>(queryGraph.vertexSet());
//		Set<String> visited = new HashSet<String>();
//		
//		List<String> startCandidates = new ArrayList<String>();
//		for (String qv : queryGraph.vertexSet()) {
//			if (queryGraph.inDegreeOf(qv) == 0) 
//				startCandidates.add(qv);
//		}
//		
//		// TODO handle queries with no vertices with no incoming edges
//		while (notVisited.size() > 0) {
//			String startVertex = startCandidates.remove(0);
//			Stack<String> toVisit = new Stack<String>();
//			toVisit.add(startVertex);
//			
//			while (toVisit.size() != 0) {
//				String currentVertex = toVisit.pop();
//				if (visited.contains(currentVertex))
//					continue;
//				
//				notVisited.remove(currentVertex);
////				log.debug("visiting: " + currentVertex);
//				
//				List<String> sources = new ArrayList<String>();
//				for (LabeledEdge<String> inEdge : queryGraph.incomingEdgesOf(currentVertex)) {
//					sources.add(inEdge.getSrc());
//					if (!visited.contains(inEdge.getSrc()))
//						toVisit.add(inEdge.getSrc());
//				}
//				
//				List<String> targets = new ArrayList<String>();
//				for (LabeledEdge<String> outEdge : queryGraph.outgoingEdgesOf(currentVertex)) {
//					targets.add(outEdge.getDst());
//					if (!visited.contains(outEdge.getDst()))
//						toVisit.add(outEdge.getDst());
//				}
//				
//				tripleMapping.get(currentVertex).addToMapping(visited, mappings, currentVertex, sources, targets, tripleMapping);
//				
//				visited.add(currentVertex);
//			}
//		}
////		log.debug("mappings: " + mappings);
//		end(MAPPING);
//		return mappings;
//	}
//	
//	private void validateMapping(NamedQueryGraph<String,LabeledQueryEdge<String>> queryGraph, IsomorphismRelation<String,LabeledEdge<String>> iso) throws StorageException {
//		Set<String> notVisited = new HashSet<String>(queryGraph.vertexSet());
//		
//		List<String> startCandidates = new ArrayList<String>();
//		for (String qv : queryGraph.vertexSet()) {
//			if (queryGraph.inDegreeOf(qv) == 0) 
//				startCandidates.add(qv);
//		}
//		
//		// TODO handle queries with no vertices with no incoming edges
//		
//		String startVertex = startCandidates.remove(0);
//		Stack<String> toVisit = new Stack<String>();
//		toVisit.add(startVertex);
//		
//		Map<String,Set<Triple>> tripleMapping = new HashMap<String,Set<Triple>>();
//		
//		while (toVisit.size() != 0) {
//			String sourceVertex = toVisit.pop();
//			notVisited.remove(sourceVertex);
//			
//			Term sourceTerm = queryGraph.getTerm(sourceVertex);
//			String sourceExt = iso.getVertexCorrespondence(sourceVertex, true);
//			
//			for (LabeledEdge<String> edge : queryGraph.outgoingEdgesOf(sourceVertex)) {
//				String propertyUri = edge.getLabel();
//				String targetVertex = edge.getDst();
//				Term targetTerm = queryGraph.getTerm(targetVertex);
//				String targetExt = iso.getVertexCorrespondence(targetVertex, true);
//				
//				log.debug(sourceVertex + "(" + sourceExt + ")" + " " + propertyUri + " " + targetVertex + "(" + targetExt + ")");
//				
//				Set<Triple> sourceTriples = tripleMapping.get(sourceVertex);
//				Set<Triple> targetTriples = tripleMapping.get(targetVertex);
//				
//				if (targetTriples == null) {
//					if (targetTerm instanceof Variable) {
//						targetTriples = m_em.extension(targetExt).getTriples(propertyUri);
//					}
//					else if (targetTerm instanceof Individual) {
//						targetTriples = m_em.extension(targetExt).getTriples(propertyUri, targetTerm.toString());
//					}
//					else if (targetTerm instanceof Constant) {
//						targetTriples = m_em.extension(targetExt).getTriples(propertyUri, targetTerm.toString());
//					}
//				}
//				
//				if (sourceTriples == null) {
//					if (queryGraph.inDegreeOf(sourceVertex) > 0) {
//						if (sourceTerm instanceof Variable) {
//							sourceTriples = m_em.extension(sourceExt).getTriples();
//						}
//						else if (sourceTerm instanceof Individual) {
//							sourceTriples = m_em.extension(sourceExt).getTriples();
//							for (Iterator<Triple> i = sourceTriples.iterator(); i.hasNext(); )
//								if (!i.next().getObject().equals(sourceTerm.toString()))
//									i.remove();
//						}
//						else if (sourceTerm instanceof Constant) {
//							throw new IllegalArgumentException("source term cannot be a constant");
//						}
//					}
//					else {
//						if (!(sourceTerm instanceof Variable))
//							throw new IllegalArgumentException("at this point the source term has to be a variable, if it was an individual it already should have been mapped");
//						
//						sourceTriples = new HashSet<Triple>();
//						for (Triple t : targetTriples) {
//							sourceTriples.add(new Triple("", "", t.getSubject()));
//						}
//						// TODO already finished, join is unnecessary
//					}
//				}
//				
////				log.debug("source triples: " + sourceTriples);
////				log.debug("target triples: " + targetTriples);
//			}
//		}
//	}
	
	public ResultSet evaluate(Query query, StructureIndex index) throws StorageException, InterruptedException, ExecutionException {
		long start = System.currentTimeMillis();
		
		final NamedQueryGraph<String,LabeledQueryEdge<String>> queryGraph = query.toQueryGraph();
		log.debug(queryGraph);
		Util.printDOT("query.dot", queryGraph);
		
		m_em.setMode(ExtensionManager.MODE_READONLY);
		
		final ExecutorService executor = Executors.newFixedThreadPool(1);
//		final ExecutorCompletionService<Set<Map<String,String>>> completionService = new ExecutorCompletionService<Set<Map<String,String>>>(executor);
		
		int i = 0;
		Set<Map<String,String>> results = new HashSet<Map<String,String>>();
		for (NamedGraph<String,LabeledEdge<String>> indexGraph : index.getIndexGraphs()) {
			log.debug(indexGraph);
			Util.printDOT(indexGraph);
			
			final List<Future<Set<Map<String,String>>>> futures = new ArrayList<Future<Set<Map<String,String>>>>();
			
//			DiGraphMatcher<String,LabeledEdge<String>> matcher = new DiGraphMatcher<String,LabeledEdge<String>>(queryGraph, indexGraph, true, new EdgeLabelFeasibilityChecker(),
//					new MappingListener<String,LabeledEdge<String>>() {
//						public void mapping(IsomorphismRelation<String,LabeledEdge<String>> iso) {
//							futures.add(executor.submit(new MappingValidator(queryGraph, iso, m_groundTermCache)));
//						}
//			});
			
			DiGraphMatcher2 matcher = new DiGraphMatcher2(queryGraph, indexGraph, true, new EdgeLabelFeasibilityChecker(),
					new MappingListener<String,LabeledEdge<String>>() {
						public void mapping(IsomorphismRelation<String,LabeledEdge<String>> iso) {
							futures.add(executor.submit(new MappingValidator(queryGraph, iso, m_groundTermCache)));
						}
			});
			
			start(MATCH);
			if (!matcher.isSubgraphIsomorphic()) {
				end(MATCH);
				continue;
			}
			end(MATCH);
			
			log.info("matches: " + matcher.numberOfMappings());

			for (Future<Set<Map<String,String>>> f : futures) {
				Set<Map<String,String>> result = f.get();
				if (result != null)
					results.addAll(result);
			}
		}
		executor.shutdown();
		log.debug("result maps: " + results.size());
		
		long end = System.currentTimeMillis();
		log.info((end - start) / 1000.0);
		m_groundTermCache.clear();
		
		ResultSet rs = toResultSet(results, queryGraph.getVariables());
		log.debug(rs);
		log.debug("size: " + rs.size());
		
		
		log.debug("time spent");
		log.debug(" subgraph matching: " + (timings[MATCH] / 1000.0));
		log.debug(" retrieving data: " + (timings[DATA] / 1000.0));
		log.debug(" joining: " + (timings[JOIN] / 1000.0));
		log.debug(" computing mappings: " + (timings[MAPPING] / 1000.0));
		log.debug(" building result set: " + (timings[RS] / 1000.0));
		return null;
	}
}
