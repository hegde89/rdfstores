package edu.unika.aifb.graphindex.vp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.data.Tables;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphEdge;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.query.IQueryEvaluator;
import edu.unika.aifb.graphindex.query.QueryExecution;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Counters;
import edu.unika.aifb.graphindex.util.StatisticsCollector;
import edu.unika.aifb.graphindex.util.Timings;
import edu.unika.aifb.graphindex.util.Util;
import edu.unika.aifb.graphindex.vp.LuceneStorage.Index;

public class VPQueryEvaluator implements IQueryEvaluator {
	private LuceneStorage m_ls;
	private Timings t;
	private Counters m_counters;
	private StatisticsCollector m_collector;

	private final static Logger log = Logger.getLogger(VPQueryEvaluator.class);

	public VPQueryEvaluator(LuceneStorage ls) {
		m_ls = ls;
	}

	public VPQueryEvaluator(LuceneStorage ls, StatisticsCollector c) {
		m_ls = ls;
		m_collector = c;
	}

	int loaded = 0;
	private QueryExecution m_qe;
	private GTable<String> getTable(Index index, String p, String so, String col1, String col2) throws IOException, StorageException {
		t.start(Timings.VP_LOAD);
		GTable<String> table;
		if (isVariable(so)) {
			List<GTable<String>> tables = m_ls.getIndexTables(index, p);
			table = Tables.mergeTables(tables, index == Index.OP || index == Index.PO ? 0 : 1);
			table.setSortedColumn(index == Index.OP || index == Index.PO ? 0 : 1);
		} else {
			table = m_ls.getIndexTable(index, p, so);
		}
		loaded += table.rowCount();
		table.setColumnName(0, col1);
		table.setColumnName(1, col2);
		t.end(Timings.VP_LOAD);
		return table;
	}

	public boolean isVariable(String label) {
		return label.startsWith("?");
	}

	private class ResultArea {
		private List<GraphEdge<QueryNode>> m_edges;
		private Set<String> m_nodes;
		private GTable<String> m_result;

		public ResultArea() {
			m_nodes = new HashSet<String>();
			m_edges = new ArrayList<GraphEdge<QueryNode>>();
		}

		public ResultArea(ResultArea ra1, ResultArea ra2) {
			this();
			if (ra1 != null) {
				m_edges.addAll(ra1.getEdges());
				m_nodes.addAll(ra1.getNodes());
			}

			if (ra2 != null) {
				m_edges.addAll(ra2.getEdges());
				m_nodes.addAll(ra2.getNodes());
			}
		}

		private Set<String> getNodes() {
			return m_nodes;
		}

		public boolean containsNode(String node) {
			return m_nodes.contains(node);
		}

		public void addEdge(GraphEdge<QueryNode> edge, String src, String dst) {
			m_edges.add(edge);
			m_nodes.add(src);
			m_nodes.add(dst);
		}

		public GTable<String> getResult() {
			return m_result;
		}

		public void setResult(GTable<String> result) {
			m_result = result;
//			m_result.removeDuplicates();
		}

		public List<GraphEdge<QueryNode>> getEdges() {
			return m_edges;
		}

		public String toString() {
			return "ResultArea(" + m_result + "," + m_nodes + ")";
		}
	}

	public GTable<String> joinEdges(Graph<QueryNode> queryGraph, GraphEdge<QueryNode> currentEdge, GraphEdge<QueryNode> other) throws IOException, StorageException {
		log.debug("joinEdges " + currentEdge + " " + other);
		String srcNode = queryGraph.getNode(currentEdge.getSrc()).getSingleMember();
		String dstNode = queryGraph.getNode(currentEdge.getDst()).getSingleMember();
		String otherSrcNode = queryGraph.getNode(other.getSrc()).getSingleMember();
		String otherDstNode = queryGraph.getNode(other.getDst()).getSingleMember();

		GTable<String> result;
		// four cases
		if (currentEdge.getSrc() == other.getSrc()) { // edges have the source in common
			// tables for both edges sorted by subject
			GTable<String> left = getTable(Index.PO, currentEdge.getLabel(), dstNode, srcNode, dstNode);

			GTable<String> right = getTable(Index.PO, other.getLabel(), otherDstNode, otherSrcNode, otherDstNode);

			// TODO join on second field

			if (!left.isSorted())
				left.sort(0);

			if (!right.isSorted())
				right.sort(0);

			result = Tables.mergeJoin(left, right, srcNode);
		} else if (currentEdge.getDst() == other.getDst()) { // edges have the dest in common
			// get table for both edges, sorted by object
			GTable<String> left = getTable(Index.PS, currentEdge.getLabel(), srcNode, srcNode, dstNode);
			GTable<String> right = getTable(Index.PS, other.getLabel(), otherSrcNode, otherSrcNode, otherDstNode);

			result = Tables.mergeJoin(left, right, dstNode);
		} else if (currentEdge.getDst() == other.getSrc()) { // dest of cur edge is source of other
			// table for cur sorted by object, table for other by subject
			GTable<String> left = getTable(Index.PS, currentEdge.getLabel(), srcNode, srcNode, dstNode);
			GTable<String> right = getTable(Index.PO, other.getLabel(), otherDstNode, otherSrcNode, otherDstNode);

			result = Tables.mergeJoin(left, right, dstNode);
		} else {  // vice versa
			// table for cur sorted by subject, table for other by subject
			GTable<String> left = getTable(Index.PO, currentEdge.getLabel(), dstNode, srcNode, dstNode);
			GTable<String> right = getTable(Index.PS, other.getLabel(), otherSrcNode, otherSrcNode, otherDstNode);

			result = Tables.mergeJoin(left, right, srcNode);
		}

		return result;
	}

	public GTable<String> joinEdgeWithTable(GraphEdge<QueryNode> e, String src, String dst, GTable<String> table) throws IOException, StorageException {
		// TODO what if table contains both source and dest
		log.debug("joinEdgeWithTable " + e + " " + table);
		if (table.hasColumn(dst)) { // current edge points into result
			// join on dst node of current edge
			if (!table.isSortedBy(dst))
				table.sort(dst);

			GTable<String> left = new GTable<String>(src, dst);
//			if (isVariable(src)) {
				int col = table.getColumn(dst);
				
				Set<String> objects = new HashSet<String>();
				for (String[] row : table)
					objects.add(row[col]);
				log.debug(objects.size() + " unique o");
				
				Set<String> values = new HashSet<String>();
//				for (String[] row : table) {
//					GTable<String> t = getTable(Index.PO, e.getLabel(), row[col], src, dst);
				for (String o : objects) {
					GTable<String> t = getTable(Index.PO, e.getLabel(), o, src, dst);
//					for (String[] r : t.getRows())
//						if (!values.contains(r[0])) {
//							left.addRow(r);
//							values.add(r[0]);
//						}
					if (isVariable(src))
						left.addRows(t.getRows());
					else {
						int st = t.getColumn(src);
						for (String[] row : t.getRows())
							if (row[st].equals(dst)) {
								left.addRow(row);
								break;
							}
					}
				}
				left.sort(1);
				left.setSortedColumn(1);
				log.debug("left size: " + left.rowCount());
//			}
//			else
//				left = getTable(Index.PS, e.getLabel(), src, src, dst);
			
			return Tables.mergeJoin(left, table, dst);
		} else { // current edges points out of result
			// join on src node of current edge
			if (!table.isSortedBy(src))
				table.sort(src);

			GTable<String> right = new GTable<String>(src, dst);
			
//			if (isVariable(dst)) {
				int col = table.getColumn(src);
				
				Set<String> subjects = new HashSet<String>();
				for (String[] row : table)
					subjects.add(row[col]);
				log.debug(subjects.size() + " unique s");
				Set<String> values = new HashSet<String>();
//				for (String[] row : table) {
//					GTable<String> t = getTable(Index.PS, e.getLabel(), row[col], src, dst);
				for (String s : subjects) {
					GTable<String> t = getTable(Index.PS, e.getLabel(), s, src, dst);
//					for (String[] r : t.getRows()) {
//						if (!values.contains(r[1])) {
//							right.addRow(r);
//							values.add(r[1]);
//						}
//					}
					if (isVariable(dst))
						right.addRows(t.getRows());
					else {
						int dt = t.getColumn(dst);
						for (String[] row : t.getRows())
							if (row[dt].equals(dst))
								right.addRow(row);
					}
				}
				right.sort(0);
				right.setSortedColumn(0);
				log.debug("right size: " + right.rowCount());
//				Tables.verifySorted(right);
//			}
//			else
//				right = getTable(Index.PO, e.getLabel(), dst, src, dst);

			return Tables.mergeJoin(table, right, src);
		}
	}
	
	public Map<String,Integer> getScores(Graph<QueryNode> queryGraph) {
		Set<Integer> visited = new HashSet<Integer>();
		int startNode = -1;
		final Map<String,Integer> scores = new HashMap<String,Integer>();
		for (int i = 0; i < queryGraph.nodeCount(); i++) {
			String node = queryGraph.getNode(i).getSingleMember();
			if (!isVariable(node)) {
				scores.put(node, 0);
				startNode = i;
			}
		}
		
		if (startNode == -1)
			startNode = 0;
		
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

	public List<String[]> evaluate(Query q) throws StorageException, IOException {
		t = new Timings();
		m_counters = new Counters();
		m_collector.addTimings(t);
		m_collector.addCounters(m_counters);
		
		GTable.timings = t;
		Tables.timings = t;
		long start = System.currentTimeMillis();
		t.start(Timings.TOTAL_QUERY_EVAL);
		final Graph<QueryNode> queryGraph = q.getGraph();

		final Map<String,Integer> scores = getScores(queryGraph);
		log.debug(scores);

		for (GraphEdge<QueryNode> edge : queryGraph.edges()) {
			if (m_ls.getObjectCardinality(edge.getLabel()) == null) {
				log.debug("unknown edge: aborting");
				return new ArrayList<String[]>();
			}
		}

		Queue<GraphEdge<QueryNode>> toVisit = new PriorityQueue<GraphEdge<QueryNode>>(queryGraph.edgeCount(), new Comparator<GraphEdge<QueryNode>>() {
			public int compare(GraphEdge<QueryNode> e1, GraphEdge<QueryNode> e2) {
				String s1 = queryGraph.getNode(e1.getSrc()).getSingleMember();
				String s2 = queryGraph.getNode(e2.getSrc()).getSingleMember();
				String d1 = queryGraph.getNode(e1.getDst()).getSingleMember();
				String d2 = queryGraph.getNode(e2.getDst()).getSingleMember();
				
				int e1score = scores.get(s1) * scores.get(d1);
				int e2score = scores.get(s2) * scores.get(d2);
				
				if (e1score == e2score) {
					Integer ce1 = m_ls.getObjectCardinality(e1.getLabel());
					Integer ce2 = m_ls.getObjectCardinality(e2.getLabel());
					
					if (ce1 != null && ce2 != null && ce1.intValue() != ce2.intValue()) {
						if (ce1 < ce2)
							return 1;
						else
							return -1;
					}

				}
				
				if (e1score < e2score)
					return -1;
				else
					return 1;
			}
		});

		toVisit.addAll(queryGraph.edges());

		List<ResultArea> results = new ArrayList<ResultArea>();
		boolean empty = false;
		Set<String> visited = new HashSet<String>();
		
		if (m_qe != null) {
			toVisit.clear();
			toVisit.addAll(m_qe.toVisit());
			if (m_qe.getResultTables() != null) {
				results.clear();
				for (GTable<String> resultTable : m_qe.getResultTables()) {
					ResultArea ra = new ResultArea();
					ra.setResult(resultTable);
					
					for (GraphEdge<QueryNode> edge : m_qe.getVisited()) {
						String src = queryGraph.getSourceNode(edge).getName();
						String dst = queryGraph.getTargetNode(edge).getName();
						if (resultTable.hasColumn(src) && resultTable.hasColumn(dst)) {
							ra.addEdge(edge, src, dst);
							visited.add(src);
							visited.add(dst);
						}
					}
					results.add(ra);
				}
			}
		}

		while (toVisit.size() > 0) {
			long edgeStart = System.currentTimeMillis();
			GraphEdge<QueryNode> currentEdge;

			String srcNode;
			String dstNode;
			String property;

			List<GraphEdge<QueryNode>> skipped = new ArrayList<GraphEdge<QueryNode>>();
			do {
				currentEdge = toVisit.poll();
				skipped.add(currentEdge);
				property = currentEdge.getLabel();
				srcNode = queryGraph.getNode(currentEdge.getSrc()).getSingleMember();
				dstNode = queryGraph.getNode(currentEdge.getDst()).getSingleMember();
			}
			while ((!visited.contains(srcNode) && !visited.contains(dstNode) && Util.isVariable(srcNode) && Util.isVariable(dstNode)));
//				|| (Util.isConstant(dstNode) && !visited.contains(srcNode) && visited.contains(dstNode)));
			
			log.debug(" ");
			log.debug(srcNode + " -> " + dstNode);
			
			skipped.remove(currentEdge);
			toVisit.addAll(skipped);
			visited.add(srcNode);
			visited.add(dstNode);

			// GTable<String> table = getTable(srcNode, property, dstNode);

			ResultArea leftArea = null, rightArea = null;
			for (ResultArea ra : results) {
				if (ra.containsNode(srcNode))
					leftArea = ra;
				if (ra.containsNode(dstNode))
					rightArea = ra;
			}

			if (Util.isConstant(dstNode))
				rightArea = null;
			log.debug("la: " + leftArea + ", ra: " + rightArea);

			ResultArea ra = new ResultArea(leftArea, rightArea);
			if (leftArea == null && rightArea == null) {
				ra = new ResultArea();
				ra.addEdge(currentEdge, srcNode, dstNode);

				if (toVisit.size() == 0) { // special case: query graph contains only one edge
					if (isVariable(srcNode))
						ra.setResult(getTable(Index.PO, property, dstNode, srcNode, dstNode));
					else if (isVariable(dstNode))
						ra.setResult(getTable(Index.PS, property, srcNode, srcNode, dstNode));
					else {
						log.debug("unsupported");
					}
				}
				else if (!isVariable(dstNode)) {
					ra.setResult(getTable(Index.PO, property, dstNode, srcNode, dstNode));
				}
				else if (!isVariable(srcNode)) {
					ra.setResult(getTable(Index.PS, property, srcNode, srcNode, dstNode));
				}
				else {
					ra.setResult(getTable(Index.PO, property, dstNode, srcNode, dstNode));
				}
			} else if (leftArea == null || rightArea == null) {
				ra.addEdge(currentEdge, srcNode, dstNode);
				if ((rightArea != null && rightArea.getResult() == null) || (leftArea != null && leftArea.getResult() == null)) {
					log.debug("one area is null and result of other area is null");
					// if the result of the area is null, there is only one edge
					// in the area (it was the first edge)
					// we need to retrieve both edges
					GraphEdge<QueryNode> other = rightArea != null ? rightArea.getEdges().get(0) : leftArea.getEdges().get(0);
					ra.setResult(joinEdges(queryGraph, currentEdge, other));
				} else {
					log.debug("one area is null, but result of the other area is not");
					// there is already a result table in the result area
					// check if it is possible to retrieve table for current
					// edge sorted by the attribute as the
					// result table is sorted
					// if not, sort the result table

					ra.setResult(joinEdgeWithTable(currentEdge, srcNode, dstNode, leftArea == null ? rightArea.getResult() : leftArea.getResult()));
				}
			} else {
				// edge is between two intermediary results
				// we need to load triples from the dst ext with the label of
				// the current edge
				// probably use the objects already mapped there

				if (leftArea.getResult() == null && rightArea.getResult() == null) {
					// both result are null

					// TODO take advantage of second field joins, ie. if both
					// edges have only variables
					GTable<String> left = joinEdges(queryGraph, leftArea.getEdges().get(0), currentEdge);

					log.debug("rnrn");
					GraphEdge<QueryNode> re = rightArea.getEdges().get(0);
					String rsrc = queryGraph.getNode(re.getSrc()).getSingleMember();
					String rdst = queryGraph.getNode(re.getDst()).getSingleMember();

					GTable<String> right;
					if (re.getDst() == currentEdge.getDst()) {
						if (!isVariable(rdst)) {
							right = getTable(Index.PO, re.getLabel(), rdst, rsrc, rdst);
							right.sort(1);
						} else {
							if (!isVariable(rsrc))
								right = getTable(Index.PS, re.getLabel(), rsrc, rsrc, rdst);
							else
								right = getTable(Index.PO, re.getLabel(), rdst, rsrc, rdst); // depends on property  which is faster
							if (!right.isSorted())
								right.sort(1);
						}

						if (!left.isSortedBy(rdst))
							left.sort(rdst);

						ra.setResult(Tables.mergeJoin(left, right, rdst));
					} else {
						right = getTable(Index.PO, re.getLabel(), rdst, rsrc, rdst);

						if (!right.isSorted())
							right.sort(0);

						if (!left.isSortedBy(rsrc))
							left.sort(rsrc);

						ra.setResult(Tables.mergeJoin(left, right, rsrc));
					}
				} else if (leftArea.getResult() == null || rightArea.getResult() == null) {
					// one of the results is null
					GTable<String> table = leftArea.getResult() != null ? leftArea.getResult() : rightArea.getResult();
					table = joinEdgeWithTable(currentEdge, srcNode, dstNode, table);

					GraphEdge<QueryNode> e = leftArea.getResult() == null ? leftArea.getEdges().get(0) : rightArea.getEdges().get(0);
					String esrc = queryGraph.getNode(e.getSrc()).getSingleMember();
					String edst = queryGraph.getNode(e.getDst()).getSingleMember();

					ra.setResult(joinEdgeWithTable(e, esrc, edst, table));
				} else {
					// neither of the results is null
					GTable<String> left = leftArea.getResult();
					GTable<String> right = rightArea.getResult();
					
					GTable<String> middle = new GTable<String>(srcNode, dstNode);
					if (left == right) {
						int col = left.getColumn(srcNode);
						for (String[] row : left) {
							middle.addRows(getTable(Index.PS, property, row[col], srcNode, dstNode).getRows());
						}
						ra.setResult(Tables.hashJoin(left, middle, Arrays.asList(srcNode, dstNode)));
					}
					else {
						if (left.rowCount() < right.rowCount()) {
							if (!left.isSortedBy(srcNode))
								left.sort(srcNode);
							
							int col = left.getColumn(srcNode);
							for (String[] row : left) {
								middle.addRows(getTable(Index.PS, property, row[col], srcNode, dstNode).getRows());
							}
							middle.setSortedColumn(0);
							
							middle = Tables.mergeJoin(left, middle, srcNode);
	
							middle.sort(dstNode);
							if (!right.isSortedBy(dstNode))
								right.sort(dstNode);
							
							ra.setResult(Tables.mergeJoin(middle, right, dstNode));
						}
						else {
							if (!right.isSortedBy(dstNode))
								right.sort(dstNode);
							
							int col = right.getColumn(dstNode);
							for (String[] row : right) {
								middle.addRows(getTable(Index.PO, property, row[col], srcNode, dstNode).getRows());
							}
							middle.setSortedColumn(1);
							
							middle = Tables.mergeJoin(middle, right, dstNode);
							
							middle.sort(srcNode);
							if (!left.isSortedBy(srcNode))
								left.sort(srcNode);
							
							ra.setResult(Tables.mergeJoin(left, middle, srcNode));
						}
					}
				}
			}

			results.remove(leftArea);
			results.remove(rightArea);
			
			log.debug("rows: " + ra.getResult().rowCount());

			if (ra.getResult() == null || ra.getResult().rowCount() > 0)
				results.add(ra);
			else {
				empty = true;
				break;
			}
			
			log.debug(System.currentTimeMillis() - edgeStart);
		}
		log.debug("loaded: " + loaded);
		loaded = 0;
		log.debug("duration: " + (System.currentTimeMillis() - start) / 1000.0);
		if (empty) {
			log.debug("size: 0");
			m_counters.set(Counters.RESULTS, 0);
//			t.end(Timings.TOTAL_QUERY_EVAL);
			return new ArrayList<String[]>();
		} else {
			if (m_qe != null) {
				m_qe.addResult(results.get(0).getResult(), true);
				return m_qe.getResult().getRows();
			}
			else {
				log.debug("size: " + results.get(0).getResult().rowCount());
				List<String[]> result = new ArrayList<String[]>();
				Set<String> sigs = new HashSet<String>();
				GTable<String> table = results.get(0).getResult();
				
				log.debug(q.getSelectVariables());
				int[] cols = new int [q.getSelectVariables().size()];
				for (int i = 0; i < q.getSelectVariables().size(); i++)
					cols[i] = table.getColumn(q.getSelectVariables().get(i));
				for (String[] row : table) {
					String[] selectRow = new String [cols.length];
					StringBuilder sb = new StringBuilder();
					
					for (int i = 0; i < cols.length; i++) {
						selectRow[i] = row[cols[i]];
						sb.append(row[cols[i]]).append("__");
					}
					
					String sig = sb.toString();
					if (!sigs.contains(sig)) {
						sigs.add(sig);
						result.add(selectRow);
					}
				}
				t.end(Timings.TOTAL_QUERY_EVAL);
				m_counters.set(Counters.RESULTS, result.size());
				log.debug("size: " + result.size());
				return result;
			}
		}
	}
	
	public void clearCaches() throws StorageException {
		m_ls.clearCaches();
		m_ls.reopenAndWarmup();
	}

	public long[] getTimings() {
		return t.getTimings();
	}

	public Timings getT() {
		return t;
	}

	public void setQueryExecution(QueryExecution qe) {
		m_qe = qe;
	}
}
