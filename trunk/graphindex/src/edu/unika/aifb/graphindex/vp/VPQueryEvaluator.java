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
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.vp.LuceneStorage.Index;

public class VPQueryEvaluator implements IQueryEvaluator {
	private LuceneStorage m_ls;

	private final static Logger log = Logger.getLogger(VPQueryEvaluator.class);

	public VPQueryEvaluator(LuceneStorage ls) {
		m_ls = ls;
	}

	private void sortTable(GTable<String> table, final int col) {
		long start = System.currentTimeMillis();
		String s = table.toString();
		Collections.sort(table.getRows(), new Comparator<String[]>() {
			public int compare(String[] r1, String[] r2) {
				return r1[col].compareTo(r2[col]);
			}
		});
		table.setSortedColumn(col);
		log.debug(" sorted " + s + " by " + table.getColumnName(col) + " in " + (System.currentTimeMillis() - start) + " ms");
	}

	private void sortTable(GTable<String> table, String col) {
		sortTable(table, table.getColumn(col));
	}

	private GTable<String> getTable(String subject, String property, String object) throws IOException {
		GTable<String> table;
		if (isVariable(subject) && isVariable(object)) {
			List<GTable<String>> tables = m_ls.getIndexTables(LuceneStorage.Index.PO, property);
			table = Tables.mergeTables(tables, 0);
			table.setSortedColumn(0);
		} else if (isVariable(subject)) {
			table = m_ls.getIndexTable(LuceneStorage.Index.PO, property, object);
			table.setSortedColumn(0);
		} else {
			table = m_ls.getIndexTable(LuceneStorage.Index.PS, property, subject);
			table.setSortedColumn(1);
		}

		table.setColumnName(0, subject);
		table.setColumnName(1, object);

		return table;
	}

	private GTable<String> getTable(Index index, String p, String so, String col1, String col2) throws IOException {
		GTable<String> table;
		if (isVariable(so)) {
			List<GTable<String>> tables = m_ls.getIndexTables(index, p);
			table = Tables.mergeTables(tables, index == Index.OP || index == Index.PO ? 0 : 1);
			table.setSortedColumn(index == Index.OP || index == Index.PO ? 0 : 1);
		} else {
			table = m_ls.getIndexTable(index, p, so);
		}

		table.setColumnName(0, col1);
		table.setColumnName(1, col2);

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
		}

		public List<GraphEdge<QueryNode>> getEdges() {
			return m_edges;
		}

		public String toString() {
			return "ResultArea(" + m_result + "," + m_nodes + ")";
		}
	}

	public GTable<String> joinEdges(Graph<QueryNode> queryGraph, GraphEdge<QueryNode> currentEdge, GraphEdge<QueryNode> other) throws IOException {
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
				sortTable(left, 0);

			if (!right.isSorted())
				sortTable(right, 0);

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

	public GTable<String> joinEdgeWithTable(GraphEdge<QueryNode> e, String src, String dst, GTable<String> table) throws IOException {
		// TODO what if table contains both source and dest
		log.debug("joinEdgeWithTable " + e + " " + table);
		if (table.hasColumn(dst)) { // current edge points into result
			// join on dst node of current edge
			if (!table.isSortedBy(dst))
				sortTable(table, table.getColumn(dst));

			GTable<String> left = new GTable<String>(src, dst);
			if (isVariable(src)) {
				int col = table.getColumn(dst);
				Set<String> values = new HashSet<String>();
				for (String[] row : table) {
					GTable<String> t = getTable(Index.PO, e.getLabel(), row[col], src, dst);
					for (String[] r : t.getRows())
						if (!values.contains(r[0])) {
							left.addRow(r);
							values.add(r[0]);
						}
//					left.addRows(t.getRows());
				}
				left.setSortedColumn(1);
//				Tables.verifySorted(left);
			}
			else
				left = getTable(Index.PS, e.getLabel(), src, src, dst);
			
			return Tables.mergeJoin(left, table, dst);
		} else { // current edges points out of result
			// join on src node of current edge
			if (!table.isSortedBy(src))
				sortTable(table, table.getColumn(src));

			GTable<String> right = new GTable<String>(src, dst);
			
			if (isVariable(dst)) {
				int col = table.getColumn(src);
				for (String[] row : table) {
					GTable<String> t = getTable(Index.PS, e.getLabel(), row[col], src, dst);
					right.addRows(t.getRows());
				}
				right.setSortedColumn(0);
//				Tables.verifySorted(right);
			}
			else
				right = getTable(Index.PO, e.getLabel(), dst, src, dst);

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

	public void evaluateQuad(Query q) throws StorageException, IOException {
		long start = System.currentTimeMillis();
		final Graph<QueryNode> queryGraph = q.toGraph();

		final Map<String,Integer> scores = getScores(queryGraph);
		log.debug(scores);

		final Map<String,Integer> e2s = q.getEvalOrder();

		Queue<GraphEdge<QueryNode>> toVisit = new PriorityQueue<GraphEdge<QueryNode>>(queryGraph.edgeCount(), new Comparator<GraphEdge<QueryNode>>() {
			public int compare(GraphEdge<QueryNode> e1, GraphEdge<QueryNode> e2) {
				String s1 = queryGraph.getNode(e1.getSrc()).getSingleMember();
				String s2 = queryGraph.getNode(e2.getSrc()).getSingleMember();
				String d1 = queryGraph.getNode(e1.getDst()).getSingleMember();
				String d2 = queryGraph.getNode(e2.getDst()).getSingleMember();
								
//				int e1score = (isVariable(s1) ? 1 : 0) + (isVariable(d1) ? 1 : 0);
//				int e2score = (isVariable(s2) ? 1 : 0) + (isVariable(d2) ? 1 : 0);
				
				int e1score = scores.get(s1) * scores.get(d1);
				int e2score = scores.get(s2) * scores.get(d2);
				
				String es1 = s1 + " " + e1.getLabel() + " " + d1;
				String es2 = s2 + " " + e2.getLabel() + " " + d2;

				if (e2s.get(es1) != null && e2s.get(es2) != null) {
					if (e2s.get(es1) < e2s.get(es2))
						return -1;
					else
						return 1;
				}
				
				if (e1score < e2score)
					return -1;
				else
					return 1;
//				return 0;
			}
		});

		toVisit.addAll(queryGraph.edges());

		List<ResultArea> results = new ArrayList<ResultArea>();
		boolean empty = false;
		;
		while (toVisit.size() > 0) {
			GraphEdge<QueryNode> currentEdge = toVisit.poll();

			String srcNode = queryGraph.getNode(currentEdge.getSrc()).getSingleMember();
			String dstNode = queryGraph.getNode(currentEdge.getDst()).getSingleMember();
			String property = currentEdge.getLabel();
			log.debug(" ");
			log.debug(srcNode + " -> " + dstNode);

			// GTable<String> table = getTable(srcNode, property, dstNode);

			ResultArea leftArea = null, rightArea = null;
			for (ResultArea ra : results) {
				if (ra.containsNode(srcNode))
					leftArea = ra;
				if (ra.containsNode(dstNode))
					rightArea = ra;
			}

			log.debug("la: " + leftArea + ", ra: " + rightArea);

			ResultArea ra = new ResultArea(leftArea, rightArea);
			if (leftArea == null && rightArea == null) {
				ra = new ResultArea();
				ra.addEdge(currentEdge, srcNode, dstNode);

				if (toVisit.size() == 0) { // special case: query graph contains
					// only one edge
					if (isVariable(srcNode))
						ra.setResult(getTable(Index.PO, property, dstNode, srcNode, dstNode));
					else
						ra.setResult(getTable(Index.PS, property, srcNode, srcNode, dstNode));
				}
				else if (!isVariable(dstNode)) {
					ra.setResult(getTable(Index.PO, property, dstNode, srcNode, dstNode));
				}
				else if (!isVariable(srcNode)) {
					ra.setResult(getTable(Index.PS, property, srcNode, srcNode, dstNode));
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
							sortTable(right, 1);
						} else {
							if (!isVariable(rsrc))
								right = getTable(Index.PS, re.getLabel(), rsrc, rsrc, rdst);
							else
								right = getTable(Index.PO, re.getLabel(), rdst, rsrc, rdst); // depends on property  which is faster
							if (!right.isSorted())
								sortTable(right, 1);
						}

						if (!left.isSortedBy(rdst))
							sortTable(left, left.getColumn(rdst));

						ra.setResult(Tables.mergeJoin(left, right, rdst));
					} else {
						right = getTable(Index.PO, re.getLabel(), rdst, rsrc, rdst);

						if (!right.isSorted())
							sortTable(right, 0);

						if (!left.isSortedBy(rsrc))
							sortTable(left, left.getColumn(rsrc));

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
					if (left.rowCount() < right.rowCount()) {
						if (!left.isSortedBy(srcNode))
							sortTable(left, srcNode);
						
						int col = left.getColumn(srcNode);
						for (String[] row : left) {
							middle.addRows(getTable(Index.PS, property, row[col], srcNode, dstNode).getRows());
						}
						middle.setSortedColumn(0);
						
						middle = Tables.mergeJoin(left, middle, srcNode);
						
						sortTable(middle, dstNode);
						if (!right.isSortedBy(dstNode))
							sortTable(right, dstNode);
						
						ra.setResult(Tables.mergeJoin(middle, right, dstNode));
					}
					else {
						if (!right.isSortedBy(dstNode))
							sortTable(right, dstNode);
						
						int col = right.getColumn(dstNode);
						for (String[] row : right) {
							middle.addRows(getTable(Index.PO, property, row[col], srcNode, dstNode).getRows());
						}
						middle.setSortedColumn(1);
						
						middle = Tables.mergeJoin(middle, right, dstNode);
						
						sortTable(middle, srcNode);
						if (!left.isSortedBy(srcNode))
							sortTable(left, srcNode);
						
						ra.setResult(Tables.mergeJoin(left, middle, srcNode));
					}
				}
			}

			results.remove(leftArea);
			results.remove(rightArea);

			if (ra.getResult() == null || ra.getResult().rowCount() > 0)
				results.add(ra);
			else {
				empty = true;
				break;
			}
		}

		if (empty) {
			log.debug("size: 0");
		} else {
			log.debug("size: " + results.get(0).getResult().rowCount());
			if (results.get(0).getResult().rowCount() < 30)
				log.debug(results.get(0).getResult().toDataString());
		}
		log.debug("duration: " + (System.currentTimeMillis() - start) / 1000.0);
	}

	public void evaluate(Query q) throws StorageException, IOException {
		long start = System.currentTimeMillis();
		Graph<QueryNode> queryGraph = q.toGraph();

		Queue<GraphEdge<QueryNode>> toVisit = new PriorityQueue<GraphEdge<QueryNode>>(queryGraph.edgeCount(), new Comparator<GraphEdge<QueryNode>>() {

			public int compare(GraphEdge<QueryNode> o1, GraphEdge<QueryNode> o2) {
				// TODO Auto-generated method stub
				return 0;
			}

		});
		Set<GraphEdge<QueryNode>> visited = new HashSet<GraphEdge<QueryNode>>();

		toVisit.addAll(queryGraph.edges());

		List<GTable<String>> results = new ArrayList<GTable<String>>();

		boolean empty = false;
		;
		while (toVisit.size() > 0) {
			GraphEdge<QueryNode> currentEdge = toVisit.poll();

			if (visited.contains(currentEdge))
				continue;

			visited.add(currentEdge);

			String srcLabel = queryGraph.getNode(currentEdge.getSrc()).getSingleMember();
			String dstLabel = queryGraph.getNode(currentEdge.getDst()).getSingleMember();
			log.debug(srcLabel + " -> " + dstLabel);

			GTable<String> table = getTable(srcLabel, currentEdge.getLabel(), dstLabel);
			table.setColumnName(0, srcLabel);
			table.setColumnName(1, dstLabel);

			GTable<String> left = null, right = null;
			for (GTable<String> t : results) {
				if (t.hasColumn(srcLabel))
					left = t;
				if (t.hasColumn(dstLabel))
					right = t;
			}

			GTable<String> result;

			if (left == null && right == null) {
				result = table;
			} else if (left == null) {
				result = Tables.hashJoin(table, right, Arrays.asList(dstLabel));
			} else if (right == null) {
				result = Tables.hashJoin(left, table, Arrays.asList(srcLabel));
			} else {
				// edge is between two intermediary results
				// we need to load triples from the dst ext with the label of
				// the current edge
				// probably use the objects already mapped there

				Set<String> objects = new HashSet<String>();
				int col = right.getColumn(dstLabel);
				for (String[] t : right) {
					objects.add(t[col]);
				}

				GTable<String> middle = new GTable<String>(Arrays.asList(srcLabel, dstLabel));

				for (String[] row : table) {
					if (objects.contains(row[1]))
						middle.addRow(row);
				}

				if (left.rowCount() < right.rowCount()) {
					result = Tables.hashJoin(left, middle, Arrays.asList(srcLabel));
					result = Tables.hashJoin(result, right, Arrays.asList(dstLabel));
				} else {
					result = Tables.hashJoin(middle, right, Arrays.asList(dstLabel));
					result = Tables.hashJoin(left, result, Arrays.asList(srcLabel));
				}
			}

			results.remove(left);
			results.remove(right);

			if (result.rowCount() > 0)
				results.add(result);
			else {
				empty = true;
				break;
			}
		}

		if (empty) {
			log.debug("size: 0");
		} else {
			log.debug("size: " + results.get(0).rowCount());
		}
		log.debug("duration: " + (System.currentTimeMillis() - start) / 1000.0);
	}

	// public static void main(String[] args) {
	// GTable<String> t1 = new GTable<String>("a", "b");
	// t1.addRow(new String[] { "a", "g1" });
	// t1.addRow(new String[] { "a", "d1" });
	// t1.addRow(new String[] { "b", "f1" });
	// t1.addRow(new String[] { "c", "e1" });
	// t1.addRow(new String[] { "z", "r1" });
	//
	// GTable<String> t2 = new GTable<String>("a", "b");
	// t2.addRow(new String[] { "a", "a2" });
	// t2.addRow(new String[] { "b", "d2" });
	// t2.addRow(new String[] { "b", "f2" });
	// t2.addRow(new String[] { "d", "e2" });
	// t2.addRow(new String[] { "f", "r2" });
	// t2.addRow(new String[] { "t", "g2" });
	// t2.addRow(new String[] { "y", "x2" });
	//		
	// t1.setSortedColumn(0);
	// t2.setSortedColumn(0);
	//		
	// VPQueryEvaluator vp = new VPQueryEvaluator(null);
	// // GTable<String> t = vp.mergeTables(Arrays.asList(t1, t2), 0);
	// GTable<String> t = vp.mergeJoin(t1, t2, "a");
	// for (String[] row : t) {
	// for (String s : row)
	// System.out.print(s + " ");
	// System.out.println();
	// }
	// System.out.println();
	//
	// vp.sortTable(t, 1);
	// for (String[] row : t) {
	// for (String s : row)
	// System.out.print(s + " ");
	// System.out.println();
	// }
	// System.out.println(t);
	//		
	// }
}
