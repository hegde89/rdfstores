package edu.unika.aifb.graphindex.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.Stack;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphEdge;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.graph.isomorphism.MappingListener;
import edu.unika.aifb.graphindex.graph.isomorphism.VertexMapping;
import edu.unika.aifb.graphindex.util.Timings;

public class JoinMatcher {

	private Graph<QueryNode> m_queryGraph;
	private Graph<String> m_indexGraph;
	private Map<String,List<GraphEdge<String>>> m_l2e;
	private MappingListener m_listener;
	private VCompatibilityCache m_vcc;
	private Timings m_timings;
//	private Map<String,List<GraphEdge<String>>> m_s2e, m_d2e;
	
	private final static Logger log = Logger.getLogger(JoinMatcher.class);
	
	public JoinMatcher(Graph<QueryNode> queryGraph, Graph<String> indexGraph, MappingListener listener, VCompatibilityCache vcc, Timings timings) {
		m_queryGraph = queryGraph;
		m_indexGraph = indexGraph;
		m_listener = listener;
		m_timings = timings;
		m_vcc = vcc;
		
		m_l2e = new HashMap<String,List<GraphEdge<String>>>();
//		m_s2e = new HashMap<String,List<GraphEdge<String>>>();
//		m_d2e = new HashMap<String,List<GraphEdge<String>>>();
		
		for (GraphEdge<String> e : indexGraph.edges()) {
			List<GraphEdge<String>> edges = m_l2e.get(e.getLabel());
			if (edges == null) {
				edges = new ArrayList<GraphEdge<String>>();
				m_l2e.put(e.getLabel(), edges);
			}
			edges.add(e);
			
//			String src = m_indexGraph.getNode(e.getSrc());
//			edges = m_s2e.get(src);
//			if (edges == null) {
//				edges = new ArrayList<GraphEdge<String>>();
//				m_s2e.put(src, edges);
//			}
//			edges.add(e);
//
//			String dst = m_indexGraph.getNode(e.getDst());
//			edges = m_d2e.get(dst);
//			if (edges == null) {
//				edges = new ArrayList<GraphEdge<String>>();
//				m_d2e.put(dst, edges);
//			}
//			edges.add(e);
		}
	}
	
	private Table join(Table left, Table right, List<String> cols) {
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
		
		Table result = new Table(resultColumns);
		
		Map<String,List<int[]>> leftVal2Rows = new HashMap<String,List<int[]>>();
		for (int[] row : left) {
			String joinAttribute = getJoinAttribute(row, lc);
			List<int[]> rows = leftVal2Rows.get(joinAttribute);
			if (rows == null) {
				rows = new ArrayList<int[]>();
				leftVal2Rows.put(joinAttribute, rows);
			}
			rows.add(row);
		}
		
		int count = 0;
		for (int[] row : right) {
			List<int[]> leftRows = leftVal2Rows.get(getJoinAttribute(row, rc));
			if (leftRows != null && leftRows.size() > 0) {
				for (int[] leftRow : leftRows) {
					int[] resultRow = new int [result.columnCount()];
					System.arraycopy(leftRow, 0, resultRow, 0, leftRow.length);
					int s = 0, d = leftRow.length;
					for (int i = 0; i < src.length; i++) {
						System.arraycopy(row, s, resultRow, d, src[i] - s);
						s = src[i] + 1;
						d += src[i] - s + 1;
					}
					if (s < row.length)
						System.arraycopy(row, s, resultRow, d, resultRow.length - d);
//					System.arraycopy(row, 0, resultRow, leftRow.length, rc);
//					System.arraycopy(row, rc + 1, resultRow, leftRow.length + rc, row.length - rc - 1);
//					System.arraycopy(leftRow, 0, resultRow, 0, leftRow.length);
//					System.arraycopy(row, 0, resultRow, leftRow.length, row.length);
					result.addRow(resultRow);
					count++;
//					if (count % 100000 == 0)
//						log.debug(" rows: " + count);
				}
			}
		}
		log.debug(left + " " + right + ", " + count + " in " + (System.currentTimeMillis() - start) / 1000.0 + " seconds");
		return result;
	}
	private Table join(Table left, Table right, String leftCol, String rightCol) {
		long start = System.currentTimeMillis();

		boolean swapped = false;
		if (left.rowCount() >= right.rowCount()) {
			swapped = true;
			
			String tmp = leftCol;
			leftCol = rightCol;
			rightCol = tmp;
			
			Table tmpTable = left;
			left = right;
			right = tmpTable;
		}
		
//		log.debug("left: " + left.rowCount() + ", right: " + right.rowCount());
		
		int lc = left.getColumn(leftCol);
		int rc = right.getColumn(rightCol);
		
		List<String> resultColumns = new ArrayList<String>();
		for (String s : left.getColumnNames())
			resultColumns.add(s);
		for (String s : right.getColumnNames())
			if (!s.equals(rightCol))
				resultColumns.add(s);
		
		Table result = new Table(resultColumns);
		
		Map<Integer,List<int[]>> leftVal2Rows = new HashMap<Integer,List<int[]>>();
		for (int[] row : left) {
			List<int[]> rows = leftVal2Rows.get(row[lc]);
			if (rows == null) {
				rows = new ArrayList<int[]>();
				leftVal2Rows.put(row[lc], rows);
			}
			rows.add(row);
		}
		
		int count = 0;
		for (int[] row : right) {
			List<int[]> leftRows = leftVal2Rows.get(row[rc]);
			if (leftRows != null && leftRows.size() > 0) {
				for (int[] leftRow : leftRows) {
					int[] resultRow = new int [result.columnCount()];
					System.arraycopy(leftRow, 0, resultRow, 0, leftRow.length);
					System.arraycopy(row, 0, resultRow, leftRow.length, rc);
					System.arraycopy(row, rc + 1, resultRow, leftRow.length + rc, row.length - rc - 1);
//					System.arraycopy(leftRow, 0, resultRow, 0, leftRow.length);
//					System.arraycopy(row, 0, resultRow, leftRow.length, row.length);
					result.addRow(resultRow);
					count++;
//					if (count % 100000 == 0)
//						log.debug(" rows:" + count);
				}
			}
		}
		log.debug(left + " " + right + ", " + count + " in " + (System.currentTimeMillis() - start) / 1000.0 + " seconds");
		return result;
	}
	
	private String getJoinAttribute(int[] row, int[] cols) {
		String s = "";
		for (int col : cols)
			s += row[col] + "_";
		return s;
	}
	
	private Table join(Table left, Table right, List<String> leftCols, List<String> rightCols) {
		long start = System.currentTimeMillis();
		
//		if (left.rowCount() >= right.rowCount()) {
//			swapped = true;
//			
//			List<String> tmp = leftCols;
//			leftCols = rightCols;
//			rightCols = tmp;
//			
//			Table tmpTable = left;
//			left = right;
//			right = tmpTable;
//		}
		
//		log.debug("left: " + left.rowCount() + ", right: " + right.rowCount());
		
		int[] lc = new int [leftCols.size()];
		int[] slc = new int [leftCols.size()];
		for (int i = 0; i < lc.length; i++) {
			lc[i] = left.getColumn(leftCols.get(i));
			slc[i] = left.getColumn(leftCols.get(i));
		}
		
		int[] rc = new int [rightCols.size()];
		int[] src = new int [rightCols.size()];
		for (int i = 0; i < rc.length; i++) {
			rc[i] = right.getColumn(rightCols.get(i));
			src[i] = right.getColumn(rightCols.get(i));
		}
		
		Arrays.sort(slc);
		Arrays.sort(src);
			
		List<String> resultColumns = new ArrayList<String>();
		for (String s : left.getColumnNames())
			resultColumns.add(s);
		for (String s : right.getColumnNames())
			if (!rightCols.contains(s))
				resultColumns.add(s);
		
		Table result = new Table(resultColumns);
		
		Map<String,List<int[]>> leftVal2Rows = new HashMap<String,List<int[]>>();
		for (int[] row : left) {
			String joinAttribute = getJoinAttribute(row, slc);
			List<int[]> rows = leftVal2Rows.get(joinAttribute);
			if (rows == null) {
				rows = new ArrayList<int[]>();
				leftVal2Rows.put(joinAttribute, rows);
			}
			rows.add(row);
		}
		
		int count = 0;
		for (int[] row : right) {
			List<int[]> leftRows = leftVal2Rows.get(getJoinAttribute(row, src));
			if (leftRows != null && leftRows.size() > 0) {
				for (int[] leftRow : leftRows) {
					int[] resultRow = new int [result.columnCount()];
					System.arraycopy(leftRow, 0, resultRow, 0, leftRow.length);
					int s = 0, d = leftRow.length;
					for (int i = 0; i < src.length; i++) {
						System.arraycopy(row, s, resultRow, d, src[i] - s);
						s = rc[i];
						d += rc[i] - s;
					}
//					System.arraycopy(row, 0, resultRow, leftRow.length, rc);
//					System.arraycopy(row, rc + 1, resultRow, leftRow.length + rc, row.length - rc - 1);
//					System.arraycopy(leftRow, 0, resultRow, 0, leftRow.length);
//					System.arraycopy(row, 0, resultRow, leftRow.length, row.length);
					result.addRow(resultRow);
					count++;
//					if (count % 100000 == 0)
//						log.debug(" rows: " + count);
				}
			}
		}
		log.debug(left + " " + right + ", " + count + " in " + (System.currentTimeMillis() - start) / 1000.0 + " seconds");
		return result;
	}

	private Table toTable(String sourceCol, String targetCol, List<GraphEdge<String>> edges) {
		Table t = new Table(Arrays.asList(sourceCol, targetCol));
		
		for (GraphEdge<String> edge : edges) {
			int[] row = new int[] { edge.getSrc(), edge.getDst() };
			t.addRow(row);
		}
		
		return t;
	}
	
	private String getSourceColumn(GraphEdge<QueryNode> edge) {
//		return edge.toString() + "_src";
		return edge.getSrc() + "";
	}
	
	private String getTargetColumn(GraphEdge<QueryNode> edge) {
//		return edge.toString() + "_dst";
		return edge.getDst() + "";
	}
	
	private List<GraphEdge<String>> getEdges(String edgeLabel, int src, int dst) {
		QueryNode qnSrc = m_queryGraph.getNode(src);
		QueryNode qnDst = m_queryGraph.getNode(dst);
		if (qnSrc.hasVariables() && qnDst.hasVariables())
			return m_l2e.get(edgeLabel);
		
		boolean checkSrc = qnSrc.hasGroundTerms() && !qnSrc.hasVariables();
		boolean checkDst = qnDst.hasGroundTerms() && !qnDst.hasVariables();
		
		List<GraphEdge<String>> indexEdges = m_l2e.get(edgeLabel);
		List<GraphEdge<String>> edges = new ArrayList<GraphEdge<String>>();
		for (GraphEdge<String> edge : indexEdges) {
			if (!edge.getLabel().equals(edgeLabel))
				continue;
			
			if (checkSrc) {
				boolean foundAll = true;
				for (String gt : qnSrc.getGroundTerms()) {
					if (!m_vcc.get(gt, m_indexGraph.getNode(edge.getSrc()))) {
						foundAll = false;
						break;
					}
				}
				
				if (!foundAll)
					continue;
			}
			
			if (checkDst) {
				boolean foundAll = true;
				for (String gt : qnDst.getGroundTerms()) {
					if (!m_vcc.get(gt, m_indexGraph.getNode(edge.getDst()))) {
						foundAll = false;
						break;
					}
				}
				
				if (!foundAll)
					continue;
			}
			
			edges.add(edge);
		}
		return edges;
	}
	
	private List<GraphEdge<String>> getEdges(GraphEdge<QueryNode> queryEdge) {
		QueryNode qnSrc = m_queryGraph.getNode(queryEdge.getSrc());
		QueryNode qnDst = m_queryGraph.getNode(queryEdge.getDst());
		if (qnSrc.hasVariables() && qnDst.hasVariables())
			return m_l2e.get(queryEdge.getLabel());
		
		boolean checkSrc = qnSrc.hasGroundTerms() && !qnSrc.hasVariables();
		boolean checkDst = qnDst.hasGroundTerms() && !qnDst.hasVariables();
		
		List<GraphEdge<String>> indexEdges = m_l2e.get(queryEdge.getLabel());
		List<GraphEdge<String>> edges = new ArrayList<GraphEdge<String>>();
		for (GraphEdge<String> edge : indexEdges) {
			if (!edge.getLabel().equals(queryEdge.getLabel()))
				continue;
			
			if (checkSrc) {
				boolean foundAll = true;
				for (String gt : qnSrc.getGroundTerms()) {
					if (!m_vcc.get(gt, m_indexGraph.getNode(edge.getSrc()))) {
						foundAll = false;
						break;
					}
				}
				
				if (!foundAll)
					continue;
			}
			
			if (checkDst) {
				boolean foundAll = true;
				for (String gt : qnDst.getGroundTerms()) {
					if (!m_vcc.get(gt, m_indexGraph.getNode(edge.getDst()))) {
						foundAll = false;
						break;
					}
				}
				
				if (!foundAll)
					continue;
			}
			
			edges.add(edge);
		}
		return edges;
	}

	public void match2() {
		if (m_queryGraph.edgeCount() == 1) {
			GraphEdge<QueryNode> e = m_queryGraph.edges().get(0);
			String s = m_queryGraph.getNode(e.getSrc()).getName();
			String t = m_queryGraph.getNode(e.getDst()).getName();
			List<GraphEdge<String>> l = m_l2e.get(e.getLabel());
			
			for (GraphEdge<String> ie : l) {
				Map<String,String> map = new HashMap<String,String>();
				map.put(s, m_indexGraph.getNode(ie.getSrc()));
				map.put(t, m_indexGraph.getNode(ie.getDst()));
				m_listener.mapping(new VertexMapping(map));
			}
			return;
		}
		
		final Map<GraphEdge<QueryNode>,List<GraphEdge<String>>> queryEdge2IndexEdges = new HashMap<GraphEdge<QueryNode>,List<GraphEdge<String>>>();
		
		for (GraphEdge<QueryNode> queryEdge : m_queryGraph.edges()) {
			queryEdge2IndexEdges.put(queryEdge, getEdges(queryEdge));
		}
		
		PriorityQueue<GraphEdge<QueryNode>> toVisit = new PriorityQueue<GraphEdge<QueryNode>>(m_queryGraph.edgeCount(), new Comparator<GraphEdge<QueryNode>>() {
			public int compare(GraphEdge<QueryNode> e1, GraphEdge<QueryNode> e2) {
				if (queryEdge2IndexEdges.get(e1).size() < queryEdge2IndexEdges.get(e2).size())
					return -1;
				else
					return 1;
			}
		});
		
		Set<GraphEdge<QueryNode>> visited = new HashSet<GraphEdge<QueryNode>>();
		
		toVisit.addAll(m_queryGraph.edges());
		GraphEdge<QueryNode> startEdge = toVisit.peek();
		toVisit.clear();
		toVisit.offer(startEdge);
		
		Table result = null;
		
		while (toVisit.size() > 0) {
			GraphEdge<QueryNode> currentEdge = toVisit.poll();
			
			if (visited.contains(currentEdge))
				continue;
			visited.add(currentEdge);
			
			String sourceCol = getSourceColumn(currentEdge);
			String targetCol = getTargetColumn(currentEdge);
			List<GraphEdge<String>> currentEdges = queryEdge2IndexEdges.get(currentEdge);
			
			log.debug(m_queryGraph.getNode(currentEdge.getSrc()).getSingleMember() + " -> " + m_queryGraph.getNode(currentEdge.getDst()).getSingleMember() + " (" + currentEdges.size() + " rows)");
			
			if (result == null) {
				result = toTable(sourceCol, targetCol, currentEdges);
			}
			else {
				Table t2 = toTable(sourceCol, targetCol, currentEdges);
				
				if (result.hasColumn(sourceCol) && result.hasColumn(targetCol))
					result = join(result, t2, Arrays.asList(sourceCol, targetCol));
				else if (result.hasColumn(sourceCol))
					result = join(result, t2, Arrays.asList(sourceCol));
				else
					result = join(result, t2, Arrays.asList(targetCol));
			}
			
			if (result.rowCount() == 0)
				return;
			
			for (GraphEdge<QueryNode> e : m_queryGraph.outgoingEdges(currentEdge.getSrc()))
				if (!visited.contains(e))
					toVisit.add(e);
			for (GraphEdge<QueryNode> e : m_queryGraph.incomingEdges(currentEdge.getSrc()))
				if (!visited.contains(e))
					toVisit.add(e);
			
			for (GraphEdge<QueryNode> e : m_queryGraph.outgoingEdges(currentEdge.getDst()))
				if (!visited.contains(e))
					toVisit.add(e);
			for (GraphEdge<QueryNode> e : m_queryGraph.incomingEdges(currentEdge.getDst()))
				if (!visited.contains(e))
					toVisit.add(e);
		}
		
		for (int[] row : result) {
			Map<String,String> map = new HashMap<String,String>();
			for (int i = 0; i < row.length; i++)
				map.put(m_queryGraph.getNode(i).getName(), m_indexGraph.getNode(row[result.getColumn(i + "")]));
//			log.debug(map);
			VertexMapping vm = new VertexMapping(map);
			m_listener.mapping(vm);
		}
	}

	public void match() {
		if (m_queryGraph.edgeCount() == 1) {
			GraphEdge<QueryNode> e = m_queryGraph.edges().get(0);
			String s = m_queryGraph.getNode(e.getSrc()).getName();
			String t = m_queryGraph.getNode(e.getDst()).getName();
			List<GraphEdge<String>> l = m_l2e.get(e.getLabel());
			
			for (GraphEdge<String> ie : l) {
				Map<String,String> map = new HashMap<String,String>();
				map.put(s, m_indexGraph.getNode(ie.getSrc()));
				map.put(t, m_indexGraph.getNode(ie.getDst()));
				m_listener.mapping(new VertexMapping(map));
			}
			return;
		}
		
		Table result = null;
		Stack<Integer> toVisit = new Stack<Integer>();
		Set<Integer> visited = new HashSet<Integer>();
		Set<GraphEdge<QueryNode>> usedEdges = new HashSet<GraphEdge<QueryNode>>();
		
		for (int i = 0; i < m_queryGraph.nodeCount(); i++) {
			if (m_queryGraph.inDegreeOf(i) + m_queryGraph.outDegreeOf(i) >= 2) {
				toVisit.push(i);
//				log.debug("start node: " + i);
				break;
			}
		}
		
		while (toVisit.size() > 0) {
//			log.debug("--------------------------");
			int currentNode = toVisit.pop();
			
			if (visited.contains(currentNode))
				continue;
			
			visited.add(currentNode);
			
//			log.debug("currentNode: " + currentNode);
//			log.debug("out: " + m_queryGraph.outDegreeOf(currentNode) + ", in: " + m_queryGraph.inDegreeOf(currentNode));
			
			for (GraphEdge<QueryNode> e : m_queryGraph.incomingEdges(currentNode))
				toVisit.push(e.getSrc());
			
			for (GraphEdge<QueryNode> e : m_queryGraph.outgoingEdges(currentNode))
				toVisit.push(e.getDst());
			
//			log.debug("toVisit: " + toVisit);
				
			if (m_queryGraph.inDegreeOf(currentNode) + m_queryGraph.outDegreeOf(currentNode) <= 1) {
				continue;
			}
	
			List<GraphEdge<QueryNode>> joinEdges = new ArrayList<GraphEdge<QueryNode>>();
			for (GraphEdge<QueryNode> e : m_queryGraph.incomingEdges(currentNode))
				joinEdges.add(e);
			for (GraphEdge<QueryNode> e : m_queryGraph.outgoingEdges(currentNode))
				joinEdges.add(e);
			
			if (result == null) {
				GraphEdge<QueryNode> e1 = joinEdges.get(0);
				GraphEdge<QueryNode> e2 = joinEdges.get(1);
				usedEdges.add(e1);
				usedEdges.add(e2);
//				log.debug("edge: " + e1);
//				log.debug("edge: " + e2);
				
				Table t1 = toTable(getSourceColumn(e1), getTargetColumn(e1), getEdges(e1.getLabel(), e1.getSrc(), e1.getDst()));
				Table t2 = toTable(getSourceColumn(e2), getTargetColumn(e2), getEdges(e2.getLabel(), e2.getSrc(), e2.getDst()));
				
				String t1joinNode = null;
				if (e1.getDst() == currentNode)
					t1joinNode = getTargetColumn(e1);
				else
					t1joinNode = getSourceColumn(e1);

				String t2joinNode = null;
				if (e2.getDst() == currentNode)
					t2joinNode = getTargetColumn(e2);
				else
					t2joinNode = getSourceColumn(e2);
				
//				log.debug(t1joinNode + " = " + t2joinNode);
				
				if ((e1.getSrc() == e2.getSrc() && e1.getDst() == e2.getDst()) || (e1.getSrc() == e2.getDst() && e1.getDst() == e2.getDst()))
					result = join(t1, t2, Arrays.asList(getTargetColumn(e1), getSourceColumn(e1)), Arrays.asList(getTargetColumn(e2), getSourceColumn(e2)));
				else
					result = join(t1, t2, t1joinNode, t2joinNode);
				
				joinEdges.remove(0);
				joinEdges.remove(0);
				
//				log.debug("cols: " + atos(result.getColumnNames()));
			}
			
			for (GraphEdge<QueryNode> e : joinEdges) {
				if (usedEdges.contains(e))
					continue;
				usedEdges.add(e);
//				log.debug("edge: " + e);
				Table t = toTable(getSourceColumn(e), getTargetColumn(e), getEdges(e.getLabel(), e.getSrc(), e.getDst()));
				
				String tJoinNode = null;
				if (e.getDst() == currentNode)
					tJoinNode = getTargetColumn(e);
				else
					tJoinNode = getSourceColumn(e);
				
				String rJoinNode = currentNode + "";
				
				if (result.hasColumn(getTargetColumn(e)) && result.hasColumn(getSourceColumn(e))) {
					result = join(result, t, Arrays.asList(getTargetColumn(e), getSourceColumn(e)), Arrays.asList(getTargetColumn(e), getSourceColumn(e)));
				}
				else
					result = join(result, t, rJoinNode, tJoinNode);
//				log.debug("cols: " + atos(result.getColumnNames()));
			}
			
		}
		
		log.debug(result.rowCount());
		for (int[] row : result) {
			Map<String,String> map = new HashMap<String,String>();
			for (int i = 0; i < row.length; i++)
				map.put(m_queryGraph.getNode(i).getName(), m_indexGraph.getNode(row[result.getColumn(i + "")]));
//			log.debug(map);
			VertexMapping vm = new VertexMapping(map);
			m_listener.mapping(vm);
		}
	}
}
