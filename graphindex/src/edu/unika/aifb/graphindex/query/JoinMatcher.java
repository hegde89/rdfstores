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

import edu.unika.aifb.graphindex.StructureIndex;
import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphEdge;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.graph.isomorphism.MappingListener;
import edu.unika.aifb.graphindex.graph.isomorphism.VertexMapping;
import edu.unika.aifb.graphindex.storage.ExtensionStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.ExtensionStorage.Index;
import edu.unika.aifb.graphindex.util.Timings;

public class JoinMatcher {

	private Graph<QueryNode> m_queryGraph;
	private Graph<String> m_indexGraph;
	private Map<String,List<GraphEdge<String>>> m_l2e;
	private MappingListener m_listener;
	private VCompatibilityCache m_vcc;
	private Timings m_timings;
	private StructureIndex m_index;
	private ExtensionStorage m_es;
//	private Map<String,List<GraphEdge<String>>> m_s2e, m_d2e;
	
	private final static Logger log = Logger.getLogger(JoinMatcher.class);
	
	public JoinMatcher(Graph<QueryNode> queryGraph, Graph<String> indexGraph, MappingListener listener, StructureIndex index, Timings timings) {
		m_queryGraph = queryGraph;
		m_indexGraph = indexGraph;
		m_listener = listener;
		m_timings = timings;
		m_index = index;
		m_es = index.getExtensionManager().getExtensionStorage();
		
		m_l2e = new HashMap<String,List<GraphEdge<String>>>();
		
		for (GraphEdge<String> e : indexGraph.edges()) {
			List<GraphEdge<String>> edges = m_l2e.get(e.getLabel());
			if (edges == null) {
				edges = new ArrayList<GraphEdge<String>>();
				m_l2e.put(e.getLabel(), edges);
			}
			edges.add(e);
			
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
//		log.debug(left + " " + right + ", " + count + " in " + (System.currentTimeMillis() - start) / 1000.0 + " seconds");
		return result;
	}
	private String getJoinAttribute(int[] row, int[] cols) {
		String s = "";
		for (int col : cols)
			s += row[col] + "_";
		return s;
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
	
	private List<GraphEdge<String>> getEdges(GraphEdge<QueryNode> queryEdge) throws StorageException {
		QueryNode qnSrc = m_queryGraph.getNode(queryEdge.getSrc());
		QueryNode qnDst = m_queryGraph.getNode(queryEdge.getDst());
		if (qnSrc.hasVariables() && qnDst.hasVariables())
			return m_l2e.get(queryEdge.getLabel());
		
		boolean checkSrc = qnSrc.hasGroundTerms() && !qnSrc.hasVariables();
		boolean checkDst = qnDst.hasGroundTerms() && !qnDst.hasVariables();
		
		List<GraphEdge<String>> indexEdges = m_l2e.get(queryEdge.getLabel());
//		return indexEdges;
		List<GraphEdge<String>> edges = new ArrayList<GraphEdge<String>>();
		for (GraphEdge<String> edge : indexEdges) {
			if (!edge.getLabel().equals(queryEdge.getLabel()))
				continue;
			
			m_timings.start(Timings.GT);
			boolean foundAll = true;
			if (checkSrc) {
				for (String gt : qnSrc.getGroundTerms()) {
					if (!m_es.hasTriples(Index.EPS, m_indexGraph.getNode(edge.getDst()), edge.getLabel(), gt)) {
						foundAll = false;
						break;
					}
				}
				
			}
			
			if (foundAll && checkDst) {
				for (String gt : qnDst.getGroundTerms()) {
					if (!m_es.hasTriples(Index.EPO, m_indexGraph.getNode(edge.getDst()), edge.getLabel(), gt)) {
						foundAll = false;
						break;
					}
				}
			}
			m_timings.end(Timings.GT);
			
			if (foundAll)
				edges.add(edge);
		}
		return edges;
	}

	public void match() throws StorageException {
		if (m_queryGraph.edgeCount() == 1) {
			GraphEdge<QueryNode> e = m_queryGraph.edges().get(0);
			String s = m_queryGraph.getNode(e.getSrc()).getName();
			String t = m_queryGraph.getNode(e.getDst()).getName();
			List<GraphEdge<String>> l = m_l2e.get(e.getLabel());

			QueryNode qnSrc = m_queryGraph.getNode(e.getSrc());
			QueryNode qnDst = m_queryGraph.getNode(e.getDst());
			for (GraphEdge<String> ie : l) {
				m_timings.start(Timings.GT);
				if (qnSrc.hasGroundTerms()) {
					if (!m_es.hasTriples(Index.EPS, m_indexGraph.getNode(ie.getDst()), e.getLabel(), qnSrc.getSingleMember())) {
						m_timings.end(Timings.GT);
						continue;
					}
				}
				
				if (qnDst.hasGroundTerms()) {
					if (!m_es.hasTriples(Index.EPO, m_indexGraph.getNode(ie.getDst()), e.getLabel(), qnDst.getSingleMember())) {
						m_timings.end(Timings.GT);
						continue;
					}
				}
				m_timings.end(Timings.GT);

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
			
//			log.debug(m_queryGraph.getNode(currentEdge.getSrc()).getSingleMember() + " -> " + m_queryGraph.getNode(currentEdge.getDst()).getSingleMember() + " (" + currentEdges.size() + " rows)");
			
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
		
		List<GraphEdge<QueryNode>> gtedges = new ArrayList<GraphEdge<QueryNode>>();
		for (GraphEdge<QueryNode> e : m_queryGraph.edges()) {
			if (m_queryGraph.getNode(e.getSrc()).hasGroundTerms() || m_queryGraph.getNode(e.getDst()).hasGroundTerms())
				gtedges.add(e);
		}
		
		for (int[] row : result) {
//			boolean found = true;
//			for (GraphEdge<QueryNode> e : gtedges) {
//				QueryNode qnDst = m_queryGraph.getNode(e.getDst());
//				QueryNode qnSrc = m_queryGraph.getNode(e.getSrc());
//				
//				String dstExt = m_indexGraph.getNode(row[result.getColumn(getTargetColumn(e))]);
//				
//				m_timings.start(Timings.GT);
//				if (qnSrc.hasGroundTerms()) {
//					if (!m_es.hasTriples(Index.EPS, dstExt, e.getLabel(), qnSrc.getSingleMember())) {
//						found = false;
//						break;
//					}
//				}
//				
//				if (qnDst.hasGroundTerms()) {
//					if (!m_es.hasTriples(Index.EPO, dstExt, e.getLabel(), qnDst.getSingleMember())) {
//						found = false;
//						break;
//					}
//				}
//				m_timings.end(Timings.GT);
//			}
//		
//			if (!found)
//				continue;
			
			Map<String,String> map = new HashMap<String,String>();
			for (int i = 0; i < row.length; i++)
				map.put(m_queryGraph.getNode(i).getName(), m_indexGraph.getNode(row[result.getColumn(i + "")]));

			VertexMapping vm = new VertexMapping(map);
			m_listener.mapping(vm);
		}
	}
}
