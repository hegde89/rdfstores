package edu.unika.aifb.graphindex.query;

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

import edu.unika.aifb.graphindex.StructureIndex;
import edu.unika.aifb.graphindex.StructureIndexReader;
import edu.unika.aifb.graphindex.data.GTable;
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
import edu.unika.aifb.graphindex.util.StatisticsCollector;
import edu.unika.aifb.graphindex.util.Timings;

public class MappingSetValidator implements Callable<List<String[]>> {
	
	private class ClassEvaluator implements Callable<EvaluationClass> {
		private EvaluationClass m_ec;
		private GraphEdge<QueryNode> currentEdge;
		private String dstLabel;
		private String srcLabel;

		public ClassEvaluator(EvaluationClass ec, String srcLabel, String dstLabel, GraphEdge<QueryNode> currentEdge) {
			m_ec = ec;
			this.srcLabel = srcLabel;
			this.dstLabel = dstLabel;
			this.currentEdge = currentEdge;
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
				result = getTable(srcLabel, dstLabel, dstExt, currentEdge.getLabel(), dstLabel);
				result.setColumnName(0, srcLabel);
				result.setColumnName(1, dstLabel);
			}
			else if (left == null) {
				// current edge points into a result area
				// load triples from dst ext with label of current edge and map subjects to src node
				left = getTable(srcLabel, dstLabel, dstExt, currentEdge.getLabel(), dstLabel);
				left.setColumnName(0, srcLabel);
				left.setColumnName(1, dstLabel);
				
				result = join(left, right, Arrays.asList(dstLabel));
			}
			else if (right == null) {
				// current edge points out of a result table
				right = getTable(srcLabel, dstLabel, dstExt, currentEdge.getLabel(), dstLabel);
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
				for (String object : objects) {
//					log.debug(object);
					GTable<String> table = m_es.getTable(dstExt, currentEdge.getLabel(), object, null);
					for (String[] t : table)
						middle.addRow(t);
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

	private StructureIndexReader m_indexReader;
	private Graph<QueryNode> m_queryGraph;
	private List<Map<String,String>> m_mappings;
	private ExtensionManager m_em;
	private StatisticsCollector m_collector;
	private Timings t;
	private ExtensionStorage m_es;
	
	private static final Logger log = Logger.getLogger(MappingSetValidator.class);
	
	public MappingSetValidator(StructureIndexReader indexReader, Graph<QueryNode> queryGraph, List<Map<String,String>> mappings, StatisticsCollector collector) {
		m_indexReader = indexReader;

		m_queryGraph = queryGraph;
		m_em = m_indexReader.getIndex().getExtensionManager();
		m_es = m_em.getExtensionStorage();
		m_collector = collector;
		t = new Timings();
		m_mappings = mappings;
	}
	
	private String getJoinAttribute(String[] row, int[] cols) {
		String s = "";
		for (int i : cols)
			s += row[i] + "_";
		return s;
	}
	
	private GTable<String> join(GTable<String> left, GTable<String> right, List<String> cols) {
		t.start(Timings.JOIN);
		long start = System.currentTimeMillis();
		
		if (left.rowCount() >= right.rowCount()) {
//			log.debug("should swap");

//			List<String> tmp = leftCols;
//			leftCols = rightCols;
//			rightCols = tmp;
//			
//			Table tmpTable = left;
//			left = right;
//			right = tmpTable;
		}
		
//		log.debug(left + " " + right + " " + cols);
		
		int[] lc = new int [cols.size()];
		for (int i = 0; i < lc.length; i++) {
			lc[i] = left.getColumn(cols.get(i));
		}
		
		int[] rc = new int [cols.size()];
		int[] src = new int [cols.size()];
		for (int i = 0; i < rc.length; i++) {
			rc[i] = right.getColumn(cols.get(i));
			src[i] = right.getColumn(cols.get(i));
		}
		
		Arrays.sort(src);
			
		List<String> resultColumns = new ArrayList<String>();
		for (String s : left.getColumnNames())
			resultColumns.add(s);
		for (String s : right.getColumnNames())
			if (!cols.contains(s))
				resultColumns.add(s);
		
		GTable<String> result = new GTable<String>(resultColumns);
		
		Map<String,List<String[]>> leftVal2Rows = new HashMap<String,List<String[]>>();
		for (String[] row : left) {
			String joinAttribute = getJoinAttribute(row, lc);
			List<String[]> rows = leftVal2Rows.get(joinAttribute);
			if (rows == null) {
				rows = new ArrayList<String[]>();
				leftVal2Rows.put(joinAttribute, rows);
			}
			rows.add(row);
		}
		
		int count = 0;
		for (String[] row : right) {
			List<String[]> leftRows = leftVal2Rows.get(getJoinAttribute(row, rc));
			if (leftRows != null && leftRows.size() > 0) {
				for (String[] leftRow : leftRows) {
					String[] resultRow = new String [result.columnCount()];
					System.arraycopy(leftRow, 0, resultRow, 0, leftRow.length);
					int s = 0, d = leftRow.length;
					for (int i = 0; i < src.length; i++) {
						System.arraycopy(row, s, resultRow, d, src[i] - s);
						d += src[i] - s;
						s = src[i] + 1;
					}
					if (s < row.length)
						System.arraycopy(row, s, resultRow, d, resultRow.length - d);
					result.addRow(resultRow);
					count++;
//					if (count % 100000 == 0)
//						log.debug(" rows: " + count);
				}
			}
		}
//		log.debug(left + " " + right + " => " + result + ", " + count + " in " + (System.currentTimeMillis() - start) / 1000.0 + " seconds");
		t.end(Timings.JOIN);
		return result;
	}
	
	private GTable<String> toTable(String leftCol, String rightCol, List<Triple> triples) {
		GTable<String> table = new GTable<String>(leftCol, rightCol);
		for (Triple t : triples)
			table.addRow(new String [] { t.getSubject(), t.getObject() });
		return table;
	}
	
	private void updateClasses(List<EvaluationClass> classes, String key) {
		List<EvaluationClass> newClasses = new ArrayList<EvaluationClass>();
		for (EvaluationClass ec : classes)
			newClasses.addAll(ec.addMatch(key));
		classes.addAll(newClasses);
	}
	
	private List<String[]> validateMappings(final Graph<QueryNode> queryGraph, List<Map<String,String>> mappings) throws StorageException, InterruptedException, ExecutionException {
		List<EvaluationClass> classes = new ArrayList<EvaluationClass>();
		EvaluationClass evc = new EvaluationClass(mappings);
		classes.add(evc);

		// nodes that were added to the evaluated area of the query graph (the result sets)
		final Set<String> matchedNodes = new HashSet<String>();
		
		final Map<String,Integer> cardinality = evc.getCardinalityMap();
		log.debug("cardinalityMap: " + cardinality);
		
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
				
//				log.debug(" " + cardinality.get(s1) + " " + cardinality.get(d1) + " " + cardinality.get(s2) + " " + cardinality.get(d2));
//				log.debug(" " + matchedNodes);
				
				// if both edges sources and destinations are all matched, make an arbitrary choice
//				if (matchedNodes.contains(s1) && matchedNodes.contains(s2) && matchedNodes.contains(d1) && matchedNodes.contains(d2))
//					return 1;
				
				// if all nodes of both edges are not matched, prefer the edge where the product 
				// of the cardinalities of source and destination is the lowest
//				if (!matchedNodes.contains(s1) && !matchedNodes.contains(s2) && !matchedNodes.contains(d1) && !matchedNodes.contains(d2)) {
					int c1 = cardinality.get(s1) * cardinality.get(d1);
					int c2 = cardinality.get(s2) * cardinality.get(d2);
//					log.debug(e1 + " " + e2 + " " + c1 + " " + c2);
					
					if (c1 == c2) {
//						if (queryGraph.inDegreeOf(e1.getSrc()) < queryGraph.inDegreeOf(e2.getSrc()))
//							return -1;
//						else
//							return 1;
						if (cardinality.get(d1) < cardinality.get(d2))
							return 1;
						else
							return -1;
					}
					
					return c1 < c2 ? -1 : 1;
//				}

				// evaluate edges between already matched nodes first
//				if (matchedNodes.contains(s1) && matchedNodes.contains(d1))
//					return -1;
//				else if (matchedNodes.contains(s2) && matchedNodes.contains(d2))
//					return 1;

				// otherwise, prefer the edge were the unmapped node has the lowest cardinality				
//				String e1unmatched;
//				if (matchedNodes.contains(s1))
//					e1unmatched = d1;
//				else
//					e1unmatched = s1;
//				
//				String e2unmatched;
//				if (matchedNodes.contains(s2))
//					e2unmatched = d2;
//				else
//					e2unmatched = s2;
//				
//				if (cardinality.get(e1unmatched) < cardinality.get(e2unmatched))
//					return -1;
//				else
//					return 1;
			}
		});
		
		Set<GraphEdge<QueryNode>> visited = new HashSet<GraphEdge<QueryNode>>();
		
		toVisit.addAll(queryGraph.edges());
		GraphEdge<QueryNode> startEdge = toVisit.peek();
		toVisit.clear();
//		toVisit.offer(startEdge);
		toVisit.addAll(queryGraph.edges());
		
		// TODO for start edge prefer edge where the src node has no incoming edges
		
//		log.debug(toVisit);
		
		ExecutorService executor = Executors.newFixedThreadPool(m_indexReader.getNumEvalThreads());
		ExecutorCompletionService<EvaluationClass> completionService = new ExecutorCompletionService<EvaluationClass>(executor);
		
		while (toVisit.size() > 0) {
			GraphEdge<QueryNode> currentEdge = toVisit.poll();
			
			if (visited.contains(currentEdge))
				continue;
			
			visited.add(currentEdge);
			
			String srcLabel = queryGraph.getNode(currentEdge.getSrc()).getSingleMember();
			
			if (!matchedNodes.contains(srcLabel)) {
				updateClasses(classes, srcLabel);
				matchedNodes.add(srcLabel);
			}
			
			String dstLabel = queryGraph.getNode(currentEdge.getDst()).getSingleMember();
			log.debug(srcLabel + " -> " + dstLabel);
			
			if (!matchedNodes.contains(dstLabel)) {
				updateClasses(classes, dstLabel);
				matchedNodes.add(dstLabel);
			}
			
			log.debug(" classes before eval: " + classes.size() + " (matched nodes: " + matchedNodes + ")");
			
			for (EvaluationClass ec : classes)
				completionService.submit(new ClassEvaluator(ec, srcLabel, dstLabel, currentEdge));
			
			List<EvaluationClass> nonEmptyClasses = new ArrayList<EvaluationClass>();
			for (int i = 0; i < classes.size(); i++) {
				Future<EvaluationClass> f = completionService.take();
				EvaluationClass ec = f.get();
				
				if (!ec.isEmpty())
					nonEmptyClasses.add(ec);
			}
			
			classes = nonEmptyClasses;
			
			log.debug(" classes after eval: " + classes.size());
			
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
			if (classes.size() > 0 && classes.get(0).getResults().size() > 0)
				log.debug(" x: " + x + " " + classes.get(0).getResults().size() + " " + classes.get(0).getResults());
		}
		
		executor.shutdown();

		log.debug("classes: " + classes.size());
		List<String[]> result = new ArrayList<String[]>();
		Set<Map<String,String>> resultMappings = new HashSet<Map<String,String>>();
		for (EvaluationClass ec : classes) {
			if (ec.getResults().size() == 0)
				continue;

			result.addAll(ec.getResults().get(0).getTable());
//			for (String[] row : ec.getResults().get(0)) {
//				Map<String,String> map = new HashMap<String,String>();
//				for (int i = 0; i < row.length; i++) {
//					map.put(ec.getResults().get(0).getColumnNames()[i], row[i]);
//				}
//				resultMappings.add(map);
//	 		}
		}
//		log.debug("results: " + resultMappings.size());
		
		return result;
	}
	
	public List<String[]> call() throws Exception {
		List<String[]> set = validateMappings(m_queryGraph, m_mappings);
		m_collector.addTimings(t);
		return set;
	}

}
