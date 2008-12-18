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

import sun.tools.tree.RemainderExpression;

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
import edu.unika.aifb.graphindex.storage.lucene.LRUCache;
import edu.unika.aifb.graphindex.storage.lucene.LuceneExtensionStorage;
import edu.unika.aifb.graphindex.util.StatisticsCollector;
import edu.unika.aifb.graphindex.util.Timings;

public class MappingListValidator implements Callable<List<String[]>> {
	
	private StructureIndexReader m_indexReader;
	private ExtensionManager m_em;
	private StatisticsCollector m_collector;
	private Timings t;
	private ExtensionStorage m_es;
	
	private static final int IS_NONE = 0, IS_SRC = 1, IS_DST = 2, IS_SRCDST = 3, IS_DSTSRC = 4;
	
	private static final Logger log = Logger.getLogger(MappingListValidator.class);
	
	public MappingListValidator(StructureIndexReader indexReader, StatisticsCollector collector) {
		m_indexReader = indexReader;

		m_em = m_indexReader.getIndex().getExtensionManager();
		m_es = m_em.getExtensionStorage();
		m_collector = collector;
		t = new Timings();
	}
	
	private void updateClasses(List<EvaluationClass> classes, String key) {
		t.start(Timings.UC);
		long start = System.currentTimeMillis();
		int x = classes.size();
		List<EvaluationClass> newClasses = new ArrayList<EvaluationClass>();
		for (EvaluationClass ec : classes)
			newClasses.addAll(ec.addMatch(key));
		classes.addAll(newClasses);
		log.debug("update classes: " + x + " -> " + classes.size() + " in " + (System.currentTimeMillis() - start));
		t.end(Timings.UC);
	}
	
	private boolean isGT(String node) {
		return !node.startsWith("?");
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
	
	private LRUCache<String,Set<String>> m_subjectExtCache = new LRUCache<String,Set<String>>(400000);
	private LRUCache<String,Set<String>> m_objectExtCache = new LRUCache<String,Set<String>>(400000);

	int extLoaded = 0;
	private Set<String> getExtensions(Index index, String so) throws StorageException {
		Set<String> exts;
		
		if (index == Index.OE)
			throw new UnsupportedOperationException("oe");
		
		if (index == Index.SE)
			exts = m_subjectExtCache.get(so);
		else
			exts = m_objectExtCache.get(so);
		
		if (exts != null)
			return exts;

		exts = m_es.getExtensions(index, so);
		
		if (index == Index.SE)
			m_subjectExtCache.put(so, exts);
		else
			m_objectExtCache.put(so, exts);
		
		extLoaded ++;

		return exts;
	}

	int loaded = 0;
	private GTable<String> getTable(Index index, String ext, String p, String so, String col1, String col2) throws StorageException  {
		t.start(Timings.DATA);
		GTable<String> table;
		if (!isGT(so)) {
			List<GTable<String>> tables = m_es.getIndexTables(index, ext, p);
			table = Tables.mergeTables(tables, index == Index.EPO ? 0 : 1);
			table.setSortedColumn(index == Index.EPO ? 0 : 1);
		} else {
			table = m_es.getIndexTable(index, ext, p, so);
		}
		
		loaded += table.rowCount();

		table.setColumnName(0, col1);
		table.setColumnName(1, col2);
		t.end(Timings.DATA);
		return table;
	}
	
	private Set<String> getDistinctValues(GTable<String> table, String colName, Index index, String ext) throws StorageException {
		t.start(Timings.DATA_E);
		Set<String> vals = new HashSet<String>();
		int col = table.getColumn(colName);
		for (String[] row : table) {
			if (index == Index.SE) {
				if (getExtensions(index, row[col]).contains(ext))
					vals.add(row[col]);
			}
			else {
				if (m_es.getExtension(row[col]).equals(ext))
					vals.add(row[col]);
			}
		}
		t.end(Timings.DATA_E);
		return vals;
	}
	
	private Set<String> getDistinctValues(GTable<String> table, String colName, Index index, String property, String ext) throws StorageException {
		Set<String> vals = new HashSet<String>();
		int col = table.getColumn(colName);
		for (String[] row : table)
			if (m_es.hasTriples(index == Index.SE ? Index.EPS : Index.EPO, ext, property, row[col]))
				vals.add(row[col]);
		return vals;
	}

	private Set<String> getDistinctValues(GTable<String> table, String colName) throws StorageException {
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
		t = new Timings();
		
		List<EvaluationClass> classes = new ArrayList<EvaluationClass>();
		EvaluationClass evc = new EvaluationClass(mappings);
		classes.add(evc);

		// nodes that were added to the evaluated area of the query graph (the result sets)
		final Set<String> matchedNodes = new HashSet<String>();
		
		long time = System.currentTimeMillis();
//		final Map<String,Integer> cardinality = evc.getCardinalityMap();
//		log.debug("card map: " + (System.currentTimeMillis() - time));
		
//		for (String node : cardinality.keySet())
//			if (isGT(node))
//				cardinality.put(node, 1);
//		log.debug("cardinalityMap: " + cardinality);
		
		
//		final Map<String,Integer> scores = getScores(queryGraph);
		
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

					if (e2s.get(es1) < e2s.get(es2))
						return -1;
					else
						return 1;
				
//				int e1score = scores.get(s1) * scores.get(d1);
//				int e2score = scores.get(s2) * scores.get(d2);
//
//				if (e1score < e2score)
//					return -1;
//				else
//					return 1;

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
		
		toVisit.addAll(queryGraph.edges());
		
		Set<String> removedTargets = new HashSet<String>();
		
		for (Iterator<GraphEdge<QueryNode>> i = toVisit.iterator(); i.hasNext(); ) {
			GraphEdge<QueryNode> e = i.next();
			if (QueryEvaluator.removeNodes.contains(queryGraph.getNode(e.getSrc()).getSingleMember())) {
				i.remove();
				removedTargets.add(queryGraph.getNode(e.getDst()).getSingleMember());
			}
		}
		log.debug("removed targets: " + removedTargets);
			
		
		// TODO for start edge prefer edge where the src node has no incoming edges
		
//		log.debug(toVisit);
		
//		ExecutorService executor = Executors.newFixedThreadPool(m_indexReader.getNumEvalThreads());
//		ExecutorCompletionService<EvaluationClass> completionService = new ExecutorCompletionService<EvaluationClass>(executor);
//		ExecutorCompletionService<EvaluationClass> completionService = new ExecutorCompletionService<EvaluationClass>(executor);
		
		while (toVisit.size() > 0) {
			long edgeStart = System.currentTimeMillis();
			long dataStart = t.getTimings()[Timings.DATA];
			long joinStart = Tables.timings.getTimings()[Timings.JOIN];
			GraphEdge<QueryNode> currentEdge = toVisit.poll();
			
//			if (visited.contains(currentEdge))
//				continue;
			
//			visited.add(currentEdge);
			
			String srcNode = queryGraph.getNode(currentEdge.getSrc()).getSingleMember();
			String dstNode = queryGraph.getNode(currentEdge.getDst()).getSingleMember();
			String property = currentEdge.getLabel();
			log.debug("");
			log.debug(srcNode + " -> " + dstNode);
			
			List<EvaluationClass> remainingClasses = new ArrayList<EvaluationClass>();

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
							if (table.rowCount() > 0)
								remainingClasses.add(ec);
						}
					}
					
				}
				else {
					for (String dstExt : val2ec.keySet()) {
						List<EvaluationClass> ecs = val2ec.get(dstExt);
						GTable<String> table = getTable(index, dstExt, property, dstNode, srcNode, dstNode);
						log.debug(table.rowCount());
						Map<String,EvaluationClass> ext2ec = new HashMap<String,EvaluationClass>();
						for (EvaluationClass ec : ecs) {
							if (ext2ec.containsKey(ec.getMatch(srcNode)))
								log.error("error");
							ext2ec.put(ec.getMatch(srcNode), ec);
						}
						
						if (removedTargets.contains(srcNode)) {
							int x = 0;
							for (String[] row : table) {
								String validExt = m_es.getExtension(row[0]);
//								for (EvaluationClass ec : ecs) {
//									String srcExt = ec.getMatch(srcNode);
//									if (srcExt.equals(validExt)) {
//										GTable<String> t2 = ec.findResult(srcNode);
//										if (t2 == null) {
//											t2 = new GTable<String>(srcNode, dstNode);
//											t2.setSortedColumn(0);
//											ec.getResults().add(t2);
//										}
//										t2.addRow(row);
//									}
//								}
								EvaluationClass ec = ext2ec.get(validExt);
								if (ec != null) {
									GTable<String> t2 = ec.findResult(srcNode);
									if (t2 == null) {
										t2 = new GTable<String>(srcNode, dstNode);
										t2.setSortedColumn(0);
										ec.getResults().add(t2);
									}
									t2.addRow(row);
								}
								x++;
								if (x % 100000 == 0)
									log.debug(x);
							}
							
							for (EvaluationClass ec : ecs) {
								GTable<String> t2 = ec.findResult(srcNode);
								if (table.rowCount() == 0)
									continue;
								ec.getResults().add(t2);
								remainingClasses.add(ec);
							}
							
//							for (EvaluationClass ec : ecs) {
//								String srcExt = ec.getMatch(srcNode);
//								
//								GTable<String> t2 = new GTable<String>(srcNode, dstNode);
//								for (String row[] : table)
//									if (srcExt.equals(m_es.getExtension(row[0])))
//										t2.addRow(row);
//								
//								if (t2.rowCount() > 0) {
//									ec.getResults().add(t2);
//									remainingClasses.add(ec);
//								}
//							}
						}
						else {
							for (EvaluationClass ec : ecs) {
								ec.getResults().add(table);
								if (table.rowCount() > 0)
									remainingClasses.add(ec);
							}
						}
					}
				}
				
				matchedNodes.add(srcNode);
				matchedNodes.add(dstNode);
			}
			else if (matchedNodes.contains(srcNode) && !matchedNodes.contains(dstNode)) {
				// dst node is new node, edge points out of a intermediary result
				// if dst is not a ground term, use the subjects already mapped to src
				// to load data, otherwise use the ground term
				
				log.debug("dst unmapped");
				
				List<EvaluationClass> classesBefore = new ArrayList<EvaluationClass>(classes);
				updateClasses(classes, dstNode);
				matchedNodes.add(dstNode);
				Map<String,List<EvaluationClass>> val2ec = getValueMap(classes, dstNode);
				
				if (isGT(dstNode)) {//  && !isGT(dstNode)) {
					// index is PO
					// load ext/property/dstNode
					log.debug("dst is gt, loading by object");
					
//					for (String dstExt : val2ec.keySet()) {
//						GTable<String> right = getTable(Index.EPO, dstExt, property, dstNode, srcNode, dstNode);
//						for (EvaluationClass ec : val2ec.get(dstExt)) {
//							GTable<String> left = ec.findResult(srcNode);
//							ec.getResults().remove(left);
//							
//							if (!left.isSortedBy(srcNode))
//								left.sort(srcNode);
//							
//							left = Tables.mergeJoin(left, right, srcNode);
//							
//							if (left.rowCount() == 0)
//								ec.setEmpty(true);
//							else
//								ec.getResults().add(left);
//						}
//					}
					
					for (String dstExt : val2ec.keySet()) {
						Set<String> subjects = new HashSet<String>();
						for (EvaluationClass ec : val2ec.get(dstExt))
							subjects.addAll(getDistinctValues(ec.findResult(srcNode), srcNode));
						
						GTable<String> table = new GTable<String>(srcNode, dstNode);
						for (String s : subjects) {
							for (String[] row : getTable(Index.EPS, dstExt, property, s, srcNode, dstNode).getRows()) {
								int cmp = row[1].compareTo(dstNode);
								if (cmp < 0)
									continue;
								if (cmp > 0)
									break;
								table.addRow(row);
							}
						}

						if (table.rowCount() == 0)
							continue;

						if (!table.isSortedBy(srcNode))
							table.sort(0);

//						log.debug("distinct values at src node: " + subjects.size());

						for (EvaluationClass ec : val2ec.get(dstExt)) {
							GTable<String> left = ec.findResult(srcNode);
							ec.getResults().remove(left);
							
							if (!left.isSortedBy(srcNode))
								left.sort(srcNode);
							
							String srcExt = ec.getMatch(srcNode);
							String objectExt = m_es.getExtension(srcExt);
							GTable<String> t2 = new GTable<String>(srcNode, dstNode);
							if (removedTargets.contains(srcNode)) {
								for (String[] row : table) {
									if (row[0].equals(objectExt))
										t2.addRow(row);
								}
								t2.setSortedColumn(0);
							}
							else
								t2 = table;
							
							if (t2.rowCount() == 0)
								continue;
							
							left = Tables.mergeJoin(left, t2, srcNode);
							
							if (left.rowCount() > 0) {
								ec.getResults().add(left);
								remainingClasses.add(ec);
							}
						}
					}
				}
				else {
					// index is PS
					// for each value in table column srcNode
					//  load ext/property/value
					log.debug("dst is not gt, loading by subject");
					t.start(Timings.EXTSETUP);
					time = System.currentTimeMillis();
					Set<String> subjects = new HashSet<String>();
					for (EvaluationClass ec : classesBefore)
						subjects.addAll(getDistinctValues(ec.findResult(srcNode), srcNode));

					Set<String> exts = new HashSet<String>();
					for (String s : subjects)
						exts.addAll(getExtensions(Index.SE, s));
					t.end(Timings.EXTSETUP);
//					log.debug(System.currentTimeMillis() - time);
					log.debug("subjects: " + subjects.size());
					log.debug("exts: " + exts.size());
					for (String dstExt : val2ec.keySet()) {
						if (!exts.contains(dstExt))
							continue;
							
						subjects.clear();
						for (EvaluationClass ec : val2ec.get(dstExt))
							subjects.addAll(getDistinctValues(ec.findResult(srcNode), srcNode));
						
						GTable<String> table = new GTable<String>(srcNode, dstNode);
						for (String s : subjects)
							if (getExtensions(Index.SE, s).contains(dstExt))
								table.addRows(getTable(Index.EPS, dstExt, property, s, srcNode, dstNode).getRows());

						if (table.rowCount() == 0) {
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

							if (result.rowCount() > 0) {
								ec.getResults().add(result);
								remainingClasses.add(ec);
							}
						}
					}
				}
				
			}
			else if (!matchedNodes.contains(srcNode) && matchedNodes.contains(dstNode)) {
				// src node is new node, ie. the edge points into a intermediary result
				// if src is not a ground term, use the objects already mapped to dst
				// to load data, otherwise use the ground term
				
				log.debug("src unmapped");
				
				List<EvaluationClass> classesBefore = new ArrayList<EvaluationClass>(classes);
				updateClasses(classes, srcNode);
				matchedNodes.add(srcNode);
				Map<String,List<EvaluationClass>> val2ec = getValueMap(classes, dstNode);
				
				if (isGT(srcNode)) {// && !isGT(srcNode)) {
					// index is PS
					// load ext/property/srcNode
					log.debug("src is gt, loading by subject");
					for (String dstExt : val2ec.keySet()) {
						GTable<String> left = getTable(Index.EPS, dstExt, property, srcNode, srcNode, dstNode);
						for (EvaluationClass ec : val2ec.get(dstExt)) {
							GTable<String> right = ec.findResult(dstNode);
							ec.getResults().remove(right);
							
							if (!right.isSortedBy(dstNode))
								right.sort(dstNode);
							
							right = Tables.mergeJoin(right, left, dstNode);
							
							if (right.rowCount() > 0) {
								remainingClasses.add(ec);
								ec.getResults().add(right);
							}
						}
					}
				}
				else {
					// index is PO
					// for each value in table column dstNode
					//  load ext/property/value
					log.debug("src is not gt, loading by object");
					t.start(Timings.EXTSETUP);
					Set<String> objects = new HashSet<String>();
					for (EvaluationClass ec : classesBefore)
						objects.addAll(getDistinctValues(ec.findResult(dstNode), dstNode));

					Set<String> exts = new HashSet<String>();
					for (String o : objects)
						exts.add(m_es.getExtension(o));
					t.end(Timings.EXTSETUP);
					log.debug("o: " + objects.size());
					log.debug("exts: " + exts.size());
					
					for (String dstExt : val2ec.keySet()) {
						if (!exts.contains(dstExt))
							continue;
						
						GTable<String> table = new GTable<String>(srcNode, dstNode);
						for (String o : objects) 
							if (m_es.getExtension(o).equals(dstExt))
								table.addRows(getTable(Index.EPO, dstExt, property, o, srcNode, dstNode).getRows());
						
						if (table.rowCount() == 0) 
							continue;
						
						table.sort(1);

//						log.debug("distinct values at dst node: " + objects.size());

						for (EvaluationClass ec : val2ec.get(dstExt)) {
							GTable<String> result = ec.findResult(dstNode);
							ec.getResults().remove(result);
							
							String srcExt = ec.getMatch(srcNode);
							
							GTable<String> t2 = new GTable<String>(srcNode, dstNode);
							if (removedTargets.contains(srcNode)) {
								for (String[] row : table) {
									if (m_es.getExtension(row[0]).equals(srcExt))
										t2.addRow(row);
								}
								t2.setSortedColumn(1);
							}
							else
								t2 = table;
							
							if (!result.isSortedBy(dstNode))
								result.sort(dstNode);
							result = Tables.mergeJoin(result, t2, dstNode);

							if (result.rowCount() > 0) {
								remainingClasses.add(ec);
								ec.getResults().add(result);
							}
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
				
				Set<String> objects = new HashSet<String>();
				for (EvaluationClass ec : classes)
					objects.addAll(getDistinctValues(ec.findResult(dstNode), dstNode));

				Set<String> exts = new HashSet<String>();
				for (String o : objects)
					exts.add(m_es.getExtension(o));
				log.debug("o: " + objects.size());
				log.debug("exts: " + exts.size());
				
				for (String dstExt : val2ec.keySet()) {
					GTable<String> middle = new GTable<String>(srcNode, dstNode);
//					Set<String> objects = new HashSet<String>();
//					for (EvaluationClass ec : val2ec.get(dstExt)) {
//						GTable<String> right = ec.findResult(dstNode);
//						objects.addAll(getDistinctValues(right, dstNode, Index.OE, dstExt));
//					}
					
//					log.debug("distinct values for dst: " + objects.size());
					for (String o : objects) {
						if (m_es.getExtension(o).equals(dstExt))
							middle.addRows(getTable(Index.EPO, dstExt, property, o, srcNode, dstNode).getRows());
					}
					if (middle.rowCount() == 0) {
						continue;
					}

					middle.sort(1);
					
					for (EvaluationClass ec : val2ec.get(dstExt)) {
						GTable<String> right = ec.findResult(dstNode);
						GTable<String> left = ec.findResult(srcNode);
						GTable<String> temp;
						
						if (right == left) {
							temp = Tables.hashJoin(left, middle, Arrays.asList(srcNode, dstNode));
						}
						else {
							if (!right.isSortedBy(dstNode));
								right.sort(dstNode);
							
							temp = Tables.mergeJoin(middle, right, dstNode);
							
							if (!temp.isSortedBy(srcNode))
								temp.sort(srcNode);
							if (!left.isSortedBy(srcNode))
								left.sort(srcNode);
							
							temp = Tables.mergeJoin(left, temp, srcNode);
						}
						
						ec.getResults().remove(left);
						ec.getResults().remove(right);
						if (temp.rowCount() > 0) {
							remainingClasses.add(ec);
							ec.getResults().add(temp);
						}
					}
				}
			}
			
			
//			Map<String,Integer> resCardinality = new HashMap<String,Integer>();
//			int x = 0;
//			for (EvaluationClass ec : classes) {
//				for (GTable<String> table : ec.getResults()) {
//					x += table.rowCount();
//					for (String col : table.getColumnNames()) {
//						if (resCardinality.containsKey(col))
//							resCardinality.put(col, resCardinality.get(col) + table.rowCount());
//						else
//							resCardinality.put(col, table.rowCount());
//					}
//				}
//			}
//			log.debug(resCardinality);

			classes = remainingClasses;
			log.debug("remaining classes: " + classes.size());

			log.debug("total edge: " + (System.currentTimeMillis() - edgeStart) + ", load: " + (t.getTimings()[Timings.DATA] - dataStart) + ", join: " + (Tables.timings.getTimings()[Timings.JOIN] - joinStart));
		}

		log.debug("classes: " + classes.size());
		List<String[]> result = new ArrayList<String[]>();
		for (EvaluationClass ec : classes) {
			if (ec.getResults().size() == 0)
				continue;
			result.addAll(ec.getResults().get(0).getTable());
		}
		log.debug("extloaded: " + extLoaded);
		log.debug("loaded: " + loaded);
		extLoaded = 0;
		loaded = 0;
		m_collector.addTimings(t);
		
		return result;
	}
	
	public void clearCaches() {
		m_subjectExtCache.clear();
		m_objectExtCache.clear();
	}
	
	public List<String[]> call() throws Exception {
		List<String[]> set = null;//validateMappings(m_queryGraph, m_mappings);
		m_collector.addTimings(t);
		return set;
	}

}
