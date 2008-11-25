package edu.unika.aifb.graphindex.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.Stack;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.swing.event.ListSelectionEvent;

import org.apache.log4j.Logger;
import org.jgrapht.experimental.isomorphism.IsomorphismRelation;
import org.semanticweb.kaon2.api.owl.elements.ObjectSome;

import edu.unika.aifb.graphindex.StructureIndex;
import edu.unika.aifb.graphindex.StructureIndexReader;
import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.data.Tables;
import edu.unika.aifb.graphindex.data.Triple;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphEdge;
import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.QueryGraph;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.graph.isomorphism.VertexMapping;
import edu.unika.aifb.graphindex.query.model.Constant;
import edu.unika.aifb.graphindex.query.model.Individual;
import edu.unika.aifb.graphindex.query.model.Term;
import edu.unika.aifb.graphindex.query.model.Variable;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.ExtensionStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.StorageManager;
import edu.unika.aifb.graphindex.storage.ExtensionStorage.Index;
import edu.unika.aifb.graphindex.util.StatisticsCollector;
import edu.unika.aifb.graphindex.util.Timings;

public class MappingListValidator implements Callable<List<String[]>> {
	
	private class ClassEvaluator implements Callable<EvaluationClass> {
		private EvaluationClass m_ec;
		private GraphEdge<QueryNode> currentEdge;
		private String dstLabel;
		private String srcLabel;
		private GTable<String> dstTable;

		public ClassEvaluator(EvaluationClass ec, String srcLabel, String dstLabel, GraphEdge<QueryNode> currentEdge, GTable<String> dstTable) {
			m_ec = ec;
			this.srcLabel = srcLabel;
			this.dstLabel = dstLabel;
			this.currentEdge = currentEdge;
			this.dstTable = dstTable;
		}
		
		private GTable<String> getTable(String src, String dst, String ext, String edge, String object) throws StorageException {
			if (dst.startsWith("?"))
				return m_es.getTable(ext, edge, null, src.startsWith("?") ? null : src);
			else
				return m_es.getTable(ext, edge, object, src.startsWith("?") ? null : src);
		}
		
		private EvaluationClass evaluate(EvaluationClass ec) throws StorageException {
			String srcExt = ec.getMatch(srcLabel);
			String dstExt = ec.getMatch(dstLabel);

			GTable<String> left = null, right = null;
			for (GTable<String> table : ec.getResults()) {
				if (table.hasColumn(srcLabel))
					left = table;
				if (table.hasColumn(dstLabel))
					right = table;
			}
			
			GTable<String> result;
			
			if (left == null && right == null) {
				// very first edge
//				result = getTable(srcLabel, dstLabel, dstExt, currentEdge.getLabel(), dstLabel);
				result = dstTable;
				result.setColumnName(0, srcLabel);
				result.setColumnName(1, dstLabel);
			}
			else if (left == null) {
				// current edge points into a result area
				// load triples from dst ext with label of current edge and map subjects to src node
//				left = getTable(srcLabel, dstLabel, dstExt, currentEdge.getLabel(), dstLabel);
				left = dstTable;
				left.setColumnName(0, srcLabel);
				left.setColumnName(1, dstLabel);
				
				result = join(left, right, Arrays.asList(dstLabel));
			}
			else if (right == null) {
				// current edge points out of a result table
//				right = getTable(srcLabel, dstLabel, dstExt, currentEdge.getLabel(), dstLabel);
				right = dstTable;
				right.setColumnName(0, srcLabel);
				right.setColumnName(1, dstLabel);
				
				result = join(left, right, Arrays.asList(srcLabel));
			}
			else {
				// edge is between two intermediary results
				// we need to load triples from the dst ext with the label of the current edge
				// probably use the objects already mapped there

				Set<String> objects = new HashSet<String>();
				int col = right.getColumn(dstLabel);
				for (String[] t : right) {
					objects.add(t[col]);
				}
				
				GTable<String> middle = new GTable<String>(Arrays.asList(srcLabel, dstLabel));
//				for (String object : objects) {
//					GTable<String> table = m_es.getTable(dstExt, currentEdge.getLabel(), object, null);
//					for (String[] t : table)
//						middle.addRow(t);
//				}
				
				for (String[] row : dstTable) {
					if (objects.contains(row[1]))
						middle.addRow(row);
				}
				
				
				if (left.rowCount() < right.rowCount()) {
					result = join(left, middle, Arrays.asList(srcLabel));
					result = join(result, right, Arrays.asList(dstLabel));
				}
				else {
					result = join(middle, right, Arrays.asList(dstLabel));
					result = join(left, result, Arrays.asList(srcLabel));
				}
			}

			ec.getResults().remove(left);
			ec.getResults().remove(right);
			
			if (result.rowCount() > 0)
				ec.getResults().add(result);
			else
				ec.setEmpty(true);
			
//			log.debug("a: " + ec.getResults().size() + " " + ec.isEmpty());
//			ec.setResult(result);
			
			return ec;
		}
		
		public EvaluationClass call() throws Exception {
			return evaluate(m_ec);
		}
		
	}
	
	private class ClassGroupEvaluator implements Callable<List<EvaluationClass>> {

		private List<EvaluationClass> classes;
		private GraphEdge<QueryNode> currentEdge;
		private String dstLabel;
		private String srcLabel;
		private ExecutorCompletionService<EvaluationClass> completionService;
		
		public ClassGroupEvaluator(List<EvaluationClass> classes, String srcLabel, String dstLabel, GraphEdge<QueryNode> currentEdge, ExecutorCompletionService<EvaluationClass> completionService) {
			this.classes = classes;
			this.srcLabel = srcLabel;
			this.dstLabel = dstLabel;
			this.currentEdge = currentEdge;
			this.completionService = completionService;
		}
		
		private GTable<String> getTable(String src, String dst, String ext, String edge, String object) throws StorageException {
			if (dst.startsWith("?"))
				return m_es.getTable(ext, edge, null, src.startsWith("?") ? null : src);
			else
				return m_es.getTable(ext, edge, object, src.startsWith("?") ? null : src);
		}

		private int evaluate() throws StorageException, InterruptedException, ExecutionException {
			if (classes.size() == 0)
				return 0;
			
			String dstExt = classes.get(0).getMatch(dstLabel);
			
			GTable<String> table = getTable(srcLabel, dstLabel, dstExt, currentEdge.getLabel(), dstLabel);
			log.debug(" cge.evaluate: " + classes.size() + " " + dstExt + " " + table.rowCount());
			
			if (table.rowCount() == 0) {
				log.debug(" " + dstExt + " table empty");
				return 0;
			}
			
			
			for (EvaluationClass ec : classes) {
				completionService.submit(new ClassEvaluator(ec, srcLabel, dstLabel, currentEdge, table));
			}
			
			return classes.size();
		}
		
		public List<EvaluationClass> call() throws Exception {
//			return evaluate();
			return null;
		}
		
	}

	private StructureIndexReader m_indexReader;
	private Graph<QueryNode> m_queryGraph;
	private List<Map<String,String>> m_mappings;
	private ExtensionManager m_em;
	private StatisticsCollector m_collector;
	private Timings t;
	private ExtensionStorage m_es;
	
	private static final int IS_NONE = 0, IS_SRC = 1, IS_DST = 2, IS_SRCDST = 3, IS_DSTSRC = 4;
	
	private static final Logger log = Logger.getLogger(MappingListValidator.class);
	
	public MappingListValidator(StructureIndexReader indexReader, Graph<QueryNode> queryGraph, List<Map<String,String>> mappings, StatisticsCollector collector) {
		m_indexReader = indexReader;

		m_queryGraph = queryGraph;
		m_em = m_indexReader.getIndex().getExtensionManager();
		m_es = m_em.getExtensionStorage();
		m_collector = collector;
		t = new Timings();
		m_mappings = mappings;
	}
	
	private void updateClasses(List<EvaluationClass> classes, String key) {
		int x = classes.size();
		List<EvaluationClass> newClasses = new ArrayList<EvaluationClass>();
		for (EvaluationClass ec : classes)
			newClasses.addAll(ec.addMatch(key));
		classes.addAll(newClasses);
		log.debug("update classes: " + x + " -> " + classes.size());
	}
	
	private boolean isGT(String node) {
		return !node.startsWith("?");
	}
	
	private String src(GraphEdge<QueryNode> e) {
		return m_queryGraph.getNode(e.getSrc()).getSingleMember();
	}
	
	private String dst(GraphEdge<QueryNode> e) {
		return m_queryGraph.getNode(e.getDst()).getSingleMember();
	}
	
	private int intersect(GraphEdge<QueryNode> e1, GraphEdge<QueryNode> e2) {
		if (e1.getSrc() == e2.getSrc())
			return IS_SRC;
		if (e1.getDst() == e2.getDst())
			return IS_DST;
		if (e1.getSrc() == e2.getDst())
			return IS_SRCDST;
		if (e1.getDst() == e2.getSrc())
			return IS_DSTSRC;
		return IS_NONE;
	}
	
	private Map<String,List<EvaluationClass>> getValueMap(List<EvaluationClass> classes, String node) {
		Map<String,List<EvaluationClass>> val2ec = new HashMap<String,List<EvaluationClass>>();
		for (EvaluationClass ec : classes) {
			String val = ec.getMatch(node);
			if (!val2ec.containsKey(val))
				val2ec.put(val, new ArrayList<EvaluationClass>());
			val2ec.get(val).add(ec);
		}
		log.debug("distinct exts for " + node + ": " + val2ec.keySet().size());
		return val2ec;
	}

	private GTable<String> getTable(Index index, String ext, String p, String so, String col1, String col2) throws StorageException  {
		GTable<String> table;
		if (!isGT(so)) {
			List<GTable<String>> tables = m_es.getIndexTables(index, ext, p);
			table = Tables.mergeTables(tables, index == Index.EPO ? 0 : 1);
			table.setSortedColumn(index == Index.EPO ? 0 : 1);
		} else {
			table = m_es.getIndexTable(index, ext, p, so);
		}

		table.setColumnName(0, col1);
		table.setColumnName(1, col2);

		return table;
	}
	
	private Set<String> getDistinctValues(GTable<String> table, String colName) {
		Set<String> vals = new HashSet<String>();
		int col = table.getColumn(colName);
		for (String[] row : table)
			vals.add(row[col]);
		return vals;
	}

	public Map<String,Integer> getScores(Graph<QueryNode> queryGraph) {
		Set<Integer> visited = new HashSet<Integer>();
		int startNode = -1;
		final Map<String,Integer> scores = new HashMap<String,Integer>();
		for (int i = 0; i < queryGraph.nodeCount(); i++) {
			String node = queryGraph.getNode(i).getSingleMember();
			if (isGT(node)) {
				scores.put(node, 0);
				startNode = i;
			}
		}
		
		Stack<Integer> tov = new Stack<Integer>();
		
		tov.push(startNode);
		
		while (tov.size() > 0) {
			int node = tov.pop();
			
			if (visited.contains(node))
				continue;
			visited.add(node);
			
			String curNode = queryGraph.getNode(node).getSingleMember();
			
			int min = Integer.MAX_VALUE;
			for (int i : queryGraph.predecessors(node)) {
				if (!scores.containsKey(curNode)) {
					String v = queryGraph.getNode(i).getSingleMember();
					if (scores.containsKey(v) && scores.get(v) < min)
						min = scores.get(v);
				}
				if (!visited.contains(i))
					tov.push(i);
			}
			
			for (int i : queryGraph.successors(node)) {
				if (!scores.containsKey(curNode)) {
					String v = queryGraph.getNode(i).getSingleMember();
					if (scores.containsKey(v) && scores.get(v) < min)
						min = scores.get(v);
				}
				if (!visited.contains(i))
					tov.push(i);
			}
			
			if (!scores.containsKey(curNode))
				scores.put(curNode, min + 1);
		}
		
		return scores;
	}
	
	public List<String[]> validateMappings(final Graph<QueryNode> queryGraph, List<Map<String,String>> mappings, final Map<String,Integer> e2s) throws StorageException, InterruptedException, ExecutionException {
		List<EvaluationClass> classes = new ArrayList<EvaluationClass>();
		EvaluationClass evc = new EvaluationClass(mappings);
		classes.add(evc);

		// nodes that were added to the evaluated area of the query graph (the result sets)
		final Set<String> matchedNodes = new HashSet<String>();
		
		final Map<String,Integer> cardinality = evc.getCardinalityMap();
		
		for (String node : cardinality.keySet())
			if (isGT(node))
				cardinality.put(node, 1);
		log.debug("cardinalityMap: " + cardinality);
		
		
		final Map<String,Integer> scores = getScores(queryGraph);
		
		// toVisit contains the nodes to be visited by the evaluation algorithm
		// the queue is sorted using a custom comparator so that the 'best' node 
		// is used next
		PriorityQueue<GraphEdge<QueryNode>> toVisit = new PriorityQueue<GraphEdge<QueryNode>>(queryGraph.edgeCount(), new Comparator<GraphEdge<QueryNode>>() {
			public int compare(GraphEdge<QueryNode> e1, GraphEdge<QueryNode> e2) {
//				log.debug("comparing " + e1 + " " + e2);
				if (e1.equals(e2))
					return 0;
				
				// in order for compareTo to be consistent with equals we have to return -1 or 1 if
				// the two edges are not the same, even if the cardinality of both edges is the same
				// sorted sets won't behave as expected otherwise
				
				String s1 = queryGraph.getNode(e1.getSrc()).getSingleMember();
				String s2 = queryGraph.getNode(e2.getSrc()).getSingleMember();
				String d1 = queryGraph.getNode(e1.getDst()).getSingleMember();
				String d2 = queryGraph.getNode(e2.getDst()).getSingleMember();
				
				String es1 = s1 + " " + e1.getLabel() + " " + d1;
				String es2 = s2 + " " + e2.getLabel() + " " + d2;

				if (e2s.get(es1) != null && e2s.get(es2) != null) {
					if (e2s.get(es1) < e2s.get(es2))
						return -1;
					else
						return 1;
				}
				
				int e1score = scores.get(s1) * scores.get(d1);
				int e2score = scores.get(s2) * scores.get(d2);

				if (e1score < e2score)
					return -1;
				else
					return 1;

//				int c1 = cardinality.get(s1) * cardinality.get(d1);
//				int c2 = cardinality.get(s2) * cardinality.get(d2);
//				
//				if (c1 == c2) {
//					if (cardinality.get(d1) < cardinality.get(d2))
//						return 1;
//					else
//						return -1;
//				}
//				
//				return c1 < c2 ? -1 : 1;

			}
		});
		
		Set<GraphEdge<QueryNode>> visited = new HashSet<GraphEdge<QueryNode>>();
		
		toVisit.addAll(queryGraph.edges());
		
		// TODO for start edge prefer edge where the src node has no incoming edges
		
//		log.debug(toVisit);
		
		ExecutorService executor = Executors.newFixedThreadPool(m_indexReader.getNumEvalThreads());
//		ExecutorCompletionService<EvaluationClass> completionService = new ExecutorCompletionService<EvaluationClass>(executor);
		ExecutorCompletionService<EvaluationClass> completionService = new ExecutorCompletionService<EvaluationClass>(executor);
		
		while (toVisit.size() > 0) {
			GraphEdge<QueryNode> currentEdge = toVisit.poll();
			
			if (visited.contains(currentEdge))
				continue;
			
			visited.add(currentEdge);
			
			String srcNode = queryGraph.getNode(currentEdge.getSrc()).getSingleMember();
			String dstNode = queryGraph.getNode(currentEdge.getDst()).getSingleMember();
			String property = currentEdge.getLabel();
			log.debug("");
			log.debug(srcNode + " -> " + dstNode);
			
			if (!matchedNodes.contains(srcNode) && !matchedNodes.contains(dstNode)) {
				// no adjacent edges loaded yet, peek at next edge to decide which index to use
				// unless one of the nodes is a ground term, then load the edge using the index for
				// the ground term
				log.debug("both unmapped");
				
				Index index = null;
				if (!isGT(srcNode) && !isGT(dstNode)) {
					GraphEdge<QueryNode> next = null;
					int is = 0;
					for (GraphEdge<QueryNode> e : toVisit) {
						is = intersect(currentEdge, e);
						if (is != IS_NONE) {
							next = e;
							break;
						}
					}
					
					if (next == null) {
						// no intersecting edge found, curious...
						log.error("no intersecting edge");
						index = Index.EPS;
					}
					else {
						if (is == IS_SRC || is == IS_SRCDST)
							index = Index.EPO;
						else
							index = Index.EPS;
					}
				}
				else if (isGT(srcNode))
					index = Index.EPS;
				else
					index = Index.EPO;
				
				log.debug("index: " + index);
				
				updateClasses(classes, srcNode);
				updateClasses(classes, dstNode);
				
				Map<String,List<EvaluationClass>> val2ec = getValueMap(classes, dstNode);
				if (index == Index.EPS) {
					for (String dstExt : val2ec.keySet()) {
						List<EvaluationClass> ecs = val2ec.get(dstExt);
						GTable<String> table = getTable(index, dstExt, property, srcNode, srcNode, dstNode);
						for (EvaluationClass ec : ecs) {
							ec.getResults().add(table);
							if (table.rowCount() == 0)
								ec.setEmpty(true);
						}
					}
					
				}
				else {
					for (String dstExt : val2ec.keySet()) {
						List<EvaluationClass> ecs = val2ec.get(dstExt);
						GTable<String> table = getTable(index, dstExt, property, dstNode, srcNode, dstNode);
						for (EvaluationClass ec : ecs) {
							ec.getResults().add(table);
							if (table.rowCount() == 0)
								ec.setEmpty(true);
						}
					}
				}
				
				matchedNodes.add(srcNode);
				matchedNodes.add(dstNode);
				
				// for each evaluation class, load data
			}
			else if (matchedNodes.contains(srcNode) && !matchedNodes.contains(dstNode)) {
				// dst node is new node, edge points out of a intermediary result
				// if dst is not a ground term, use the subjects already mapped to src
				// to load data, otherwise use the ground term
				
				log.debug("dst unmapped");
				
				updateClasses(classes, dstNode);
				matchedNodes.add(dstNode);
				Map<String,List<EvaluationClass>> val2ec = getValueMap(classes, dstNode);

				if (isGT(dstNode)) {//  && !isGT(dstNode)) {
					// index is PO
					// load ext/property/dstNode
					log.debug("dst is gt, loading by object");
					
					for (String dstExt : val2ec.keySet()) {
						GTable<String> right = getTable(Index.EPO, dstExt, property, dstNode, srcNode, dstNode);
						for (EvaluationClass ec : val2ec.get(dstExt)) {
							GTable<String> left = ec.findResult(srcNode);
							ec.getResults().remove(left);
							
							if (!left.isSortedBy(srcNode))
								left.sort(srcNode);
							
							left = Tables.mergeJoin(left, right, srcNode);
							
							if (left.rowCount() == 0)
								ec.setEmpty(true);
							else
								ec.getResults().add(left);
						}
					}
//					for (String dstExt : val2ec.keySet()) {
//						for (EvaluationClass ec : val2ec.get(dstExt)) {
//							GTable<String> left = ec.findResult(srcNode);
//							ec.getResults().remove(left);
//							
//							if (!left.isSortedBy(srcNode))
//								left.sort(srcNode);
//							
//							GTable<String> right = new GTable<String>(srcNode, dstNode);
//							int col = left.getColumn(srcNode);
//							for (String [] row : left) {
//								if (m_es.hasTriples(Index.EPS, dstExt, property, row[col]))
//									right.addRow(new String[] { row[col], dstNode } );
//							}
//							
//							right.setSortedColumn(0);
//							
//							if (right.rowCount() == 0) {
//								ec.setEmpty(true);
//								continue;
//							}
//							
//							left = Tables.mergeJoin(left, right, srcNode);
//							
//							if (left.rowCount() == 0)
//								ec.setEmpty(true);
//							else
//								ec.getResults().add(left);
//						}
//					}
				}
				else {
					// index is PS
					// for each value in table column srcNode
					//  load ext/property/value
					log.debug("dst is not gt, loading by subject");
					for (String dstExt : val2ec.keySet()) {
//						Set<String> subjects = new HashSet<String>();
//						for (EvaluationClass ec : val2ec.get(dstExt)) {
//							GTable<String> result = ec.findResult(srcNode);
//							subjects.addAll(getDistinctValues(result, srcNode));
//						}

						GTable<String> table = new GTable<String>(srcNode, dstNode);
//						for (String s : subjects)
//							table.addRows(getTable(Index.EPS, dstExt, property, s, srcNode, dstNode).getRows());
//						if (!dstExt.equals("b44939"))
//							continue;
						table = getTable(Index.EPO, dstExt, property, srcNode, srcNode, dstNode);

						if (table.rowCount() == 0) {
							for (EvaluationClass ec : val2ec.get(dstExt))
								ec.setEmpty(true);
							continue;
						}

						if (!table.isSortedBy(srcNode))
							table.sort(0);

//						log.debug("distinct values at src node: " + subjects.size());

						for (EvaluationClass ec : val2ec.get(dstExt)) {
							GTable<String> result = ec.findResult(srcNode);
							ec.getResults().remove(result);
							
							if (!result.isSortedBy(srcNode))
								result.sort(srcNode);
							result = Tables.mergeJoin(result, table, srcNode);

							if (result.rowCount() == 0)
								ec.setEmpty(true);
							else
								ec.getResults().add(result);
//							log.debug(ec);
						}
					}
				}
				
			}
			else if (!matchedNodes.contains(srcNode) && matchedNodes.contains(dstNode)) {
				// src node is new node, ie. the edge points into a intermediary result
				// if src is not a ground term, use the objects already mapped to dst
				// to load data, otherwise use the ground term
				
				log.debug("src unmapped");
				
				updateClasses(classes, srcNode);
				matchedNodes.add(srcNode);
				Map<String,List<EvaluationClass>> val2ec = getValueMap(classes, dstNode);
				
				if (isGT(srcNode)) {// && !isGT(srcNode)) {
					// index is PS
					// load ext/property/srcNode
					for (String dstExt : val2ec.keySet()) {
						GTable<String> left = getTable(Index.EPS, dstExt, property, srcNode, srcNode, dstNode);
						for (EvaluationClass ec : val2ec.get(dstExt)) {
							GTable<String> right = ec.findResult(dstNode);
							ec.getResults().remove(right);
							
							if (!right.isSortedBy(dstNode))
								right.sort(dstNode);
							
							right = Tables.mergeJoin(right, left, dstNode);
							
							if (right.rowCount() == 0)
								ec.setEmpty(true);
							else
								ec.getResults().add(right);
						}
					}
				}
				else {
					// index is PO
					// for each value in table column dstNode
					//  load ext/property/value
					for (String dstExt : val2ec.keySet()) {
						Set<String> objects = new HashSet<String>();
						for (EvaluationClass ec : val2ec.get(dstExt)) {
							GTable<String> result = ec.findResult(dstNode);
							objects.addAll(getDistinctValues(result, dstNode));
						}
						
						GTable<String> table = new GTable<String>(srcNode, dstNode);
						for (String o : objects) {
							table.addRows(getTable(Index.EPO, dstExt, property, o, srcNode, dstNode).getRows());
						}
						
						if (table.rowCount() == 0) {
							for (EvaluationClass ec : val2ec.get(dstExt))
								ec.setEmpty(true);
							continue;
						}
						
						table.sort(1);

						log.debug("distinct values at dst node: " + objects.size());

						for (EvaluationClass ec : val2ec.get(dstExt)) {
							GTable<String> result = ec.findResult(dstNode);
							ec.getResults().remove(result);
							
							if (!result.isSortedBy(dstNode))
								result.sort(dstNode);
							result = Tables.mergeJoin(result, table, dstNode);

							if (result.rowCount() == 0)
								ec.setEmpty(true);
							else
								ec.getResults().add(result);
//							log.debug(ec);
						}
					}
				}
			}
			else {
				// both nodes were already mapped, ie. the edge connects two intermediary
				// results
				
				log.debug("both mapped");
				log.debug("classes: " + classes.size());
				Map<String,List<EvaluationClass>> val2ec = getValueMap(classes, dstNode);
				
				// TODO retrieve data for each distinct value in *all* evaluation class result 
				// for the specific node and then create table
				
				for (String dstExt : val2ec.keySet()) {
					GTable<String> middle = new GTable<String>(srcNode, dstNode);
					Set<String> objects = new HashSet<String>();
					for (EvaluationClass ec : val2ec.get(dstExt)) {
						GTable<String> right = ec.findResult(dstNode);
						objects.addAll(getDistinctValues(right, dstNode));
					}
					
					log.debug("distinct values for dst: " + objects.size());
					for (String o : objects) {
						middle.addRows(getTable(Index.EPO, dstExt, property, o, srcNode, dstNode).getRows());
					}
					if (middle.rowCount() == 0) {
						for (EvaluationClass ec : val2ec.get(dstExt))
							ec.setEmpty(true);
						continue;
					}

					middle.sort(1);
					
					for (EvaluationClass ec : val2ec.get(dstExt)) {
						GTable<String> right = ec.findResult(dstNode);
						GTable<String> left = ec.findResult(srcNode);
						
//						if (middle.rowCount() == 0) {
//							ec.setEmpty(true);
//							continue;
//						}
//						
						if (!right.isSortedBy(dstNode));
							right.sort(dstNode);
						
						GTable<String> temp = Tables.mergeJoin(middle, right, dstNode);
						
						if (!temp.isSortedBy(srcNode))
							temp.sort(srcNode);
						if (!left.isSortedBy(srcNode))
							left.sort(srcNode);
						
						temp = Tables.mergeJoin(left, temp, srcNode);
						
						ec.getResults().remove(left);
						ec.getResults().remove(right);
						if (temp.rowCount() == 0)
							ec.setEmpty(true);
						else
							ec.getResults().add(temp);
					}
				}
			}
			
			for (Iterator<EvaluationClass> iter = classes.iterator(); iter.hasNext(); )
				if (iter.next().isEmpty())
					iter.remove();
			log.debug("classes after removal of empty results: " + classes.size());
			
//			int tasks = 0;
//			for (String val : val2ec.keySet()) {
//				ClassGroupEvaluator cge = new ClassGroupEvaluator(val2ec.get(val), srcNode, dstNode, currentEdge, completionService);
//				tasks += cge.evaluate();
//			}
//			log.debug(" tasks: " + tasks);
//			
//			List<EvaluationClass> nonEmptyClasses = new ArrayList<EvaluationClass>();
//			for (int i = 0; i < tasks; i++) {
//				Future<EvaluationClass> f = completionService.take();
//				EvaluationClass ec = f.get();
//				
//				if (!ec.isEmpty())
//					nonEmptyClasses.add(ec);
//			}
//			
//			classes = nonEmptyClasses;
//			
//			log.debug(" classes after eval: " + classes.size());
			
			Map<String,Integer> resCardinality = new HashMap<String,Integer>();
			int x = 0;
			for (EvaluationClass ec : classes) {
				for (GTable<String> table : ec.getResults()) {
					x += table.rowCount();
					for (String col : table.getColumnNames()) {
						if (resCardinality.containsKey(col))
							resCardinality.put(col, resCardinality.get(col) + table.rowCount());
						else
							resCardinality.put(col, table.rowCount());
					}
				}
			}
			log.debug(resCardinality);

//			if (classes.size() > 0 && classes.get(0).getResults().size() > 0)
//				log.debug(" x: " + x + " " + classes.get(0).getResults().size() + " " + classes.get(0).getResults());
		}
		
		executor.shutdown();

		log.debug("classes: " + classes.size());
		List<String[]> result = new ArrayList<String[]>();
		for (EvaluationClass ec : classes) {
			if (ec.getResults().size() == 0)
				continue;
			result.addAll(ec.getResults().get(0).getTable());
		}
		
		return result;
	}
	
	public List<String[]> call() throws Exception {
		List<String[]> set = validateMappings(m_queryGraph, m_mappings);
		m_collector.addTimings(t);
		return set;
	}

}
