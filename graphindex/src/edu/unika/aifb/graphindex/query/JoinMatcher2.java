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
import edu.unika.aifb.graphindex.data.Tables;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphEdge;
import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.graph.isomorphism.MappingListener;
import edu.unika.aifb.graphindex.graph.isomorphism.VertexMapping;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.ExtensionStorage;
import edu.unika.aifb.graphindex.storage.GraphStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.ExtensionStorage.Index;
import edu.unika.aifb.graphindex.util.Timings;

public class JoinMatcher2 {

	private Graph<QueryNode> m_queryGraph;
	private HashMap<String,GTable<String>> m_p2ts;
//	private Map<String,List<GraphEdge<String>>> m_l2e;
	private HashMap<String,GTable<String>> m_p2to;
	private MappingListener m_listener;
	private Timings m_timings;
	private ExtensionStorage m_es;
	private GraphStorage m_gs;
//	private Map<String,List<GraphEdge<String>>> m_s2e, m_d2e;
	private HashSet<String> m_signatures;
	private ArrayList<String> m_signatureNodes;
	
	private final static Logger log = Logger.getLogger(JoinMatcher2.class);
	
	public JoinMatcher2(StructureIndex index, String graphName, MappingListener listener) throws StorageException {
		m_listener = listener;
		m_es = index.getExtensionManager().getExtensionStorage();
		m_gs = index.getGraphManager().getGraphStorage();
		
		m_p2ts = new HashMap<String,GTable<String>>();
		m_p2to = new HashMap<String,GTable<String>>();
		
		Set<LabeledEdge<String>> edges = m_gs.loadEdges(graphName);
		
		long start = System.currentTimeMillis();
		for (LabeledEdge<String> e : edges) {

			GTable<String> table = m_p2ts.get(e.getLabel());
			if (table == null) {
				table = new GTable<String>("source", "target");
				m_p2ts.put(e.getLabel(), table);
			}
			table.addRow(new String[] { e.getSrc(), e.getDst() });

			table = m_p2to.get(e.getLabel());
			if (table == null) {
				table = new GTable<String>("source", "target");
				m_p2to.put(e.getLabel(), table);
			}
			table.addRow(new String[] { e.getSrc(), e.getDst() });
		}
		
		for (GTable<String> t : m_p2ts.values())
			t.sort(0);
		for (GTable<String> t : m_p2to.values())
			t.sort(1);
		
		log.debug(System.currentTimeMillis() - start);
	}
	
	private GTable<String> purgeTable(GTable<String> table) {
		long start = System.currentTimeMillis();
		GTable<String> purged = new GTable<String>(table.getColumnNames());
		if (table.isSorted())
			purged.setSortedColumn(table.getSortedColumn());
		
		List<Integer> sigCols = getSignatureColumns(table);
		Set<String> signatures = new HashSet<String>();
		for (String[] row : table) {
			String sig = getSignature(row, sigCols);
			if (!signatures.contains(sig)) {
				signatures.add(sig);
				purged.addRow(row);
			}
		}
		log.debug("purged " + table.rowCount() + " => " + purged.rowCount() + " in " + (System.currentTimeMillis() - start) + " msec");
		return purged;
	}
	
	public void setQueryGraph(Query query, Graph<QueryNode> queryGraph) {
		m_queryGraph = queryGraph;
		m_signatures = new HashSet<String>();
		m_signatureNodes = new ArrayList<String>();
		
		for (int i = 0; i < m_queryGraph.nodeCount(); i++) {
			if (m_queryGraph.inDegreeOf(i) > 0)
				m_signatureNodes.add(m_queryGraph.getNode(i).getSingleMember());
		}
		
		if (query.getRemovedNodes().size() > 0) {
			for (String node : query.getRemovedNodes()) {
				if (!m_signatureNodes.contains(node))
					m_signatureNodes.remove(node);
			}
			log.debug("sig nodes: " + m_signatureNodes);
		}
		
	}
	
	public void setTimings(Timings timings) {
		m_timings = timings;
	}
	
	private String getSourceColumn(GraphEdge<QueryNode> edge) {
//		return edge.toString() + "_src";
		return m_queryGraph.getNode(edge.getSrc()).getName();
	}
	
	private String getTargetColumn(GraphEdge<QueryNode> edge) {
//		return edge.toString() + "_dst";
		return m_queryGraph.getNode(edge.getDst()).getName();
	}
	
	private List<Integer> getSignatureColumns(GTable<String> table) {
		List<Integer> sigCols = new ArrayList<Integer>();
		for (String colName : table.getColumnNames())
			if (m_signatureNodes.contains(colName))
				sigCols.add(table.getColumn(colName));
		return sigCols;
	}
	
	private String getSignature(String[] row, List<Integer> sigCols) {
		StringBuilder sb = new StringBuilder();
		for (int sigCol : sigCols)
			sb.append(row[sigCol]).append("__");
		return sb.toString();
	}
	
	private GTable<String> getTable(GraphEdge<QueryNode> queryEdge, int col) throws StorageException {
		QueryNode qnSrc = m_queryGraph.getNode(queryEdge.getSrc());
		QueryNode qnDst = m_queryGraph.getNode(queryEdge.getDst());
		String srcNode = qnSrc.getName();
		String dstNode = qnDst.getName();

		GTable<String> edgeTable;
		if (col == 0)
			edgeTable = m_p2ts.get(queryEdge.getLabel());
		else
			edgeTable = m_p2to.get(queryEdge.getLabel());
		
		if (qnSrc.hasVariables() && qnDst.hasVariables()) {
			edgeTable.setColumnName(0, srcNode);
			edgeTable.setColumnName(1, dstNode);
			return purgeTable(edgeTable);
		}
		
		boolean checkSrc = qnSrc.hasGroundTerms() && !qnSrc.hasVariables();
		boolean checkDst = qnDst.hasGroundTerms() && !qnDst.hasVariables();
		
		String validObjectExt = null;
		Set<String> validSubjectExts = new HashSet<String>();

		m_timings.start(Timings.GT);
		if (checkSrc)
			validSubjectExts = new HashSet<String>(m_es.getExtensions(Index.SE, qnSrc.getSingleMember()));
		if (checkDst)
			validObjectExt = m_es.getExtension(qnDst.getSingleMember());
		m_timings.end(Timings.GT);

		
		GTable<String> table = new GTable<String>(srcNode, dstNode);
		List<Integer> sigCols = getSignatureColumns(table);
		Set<String> signatures = new HashSet<String>();
		for (String[] row : edgeTable) {
			boolean foundAll = true;
			if (checkSrc && !validSubjectExts.contains(row[1]))
				foundAll = false;
			
			if (foundAll && checkDst && !validObjectExt.equals(row[1]))
				foundAll = false;

			if (foundAll) {
				String sig = getSignature(row, sigCols);
				if (!signatures.contains(sig)) {
					table.addRow(row);
					signatures.add(sig);
				}
			}
		}
//		log.debug("=> " + table);
		table.setSortedColumn(col);

		return table;
	}
	
	public GTable<String> match() throws StorageException {
		if (m_queryGraph.edgeCount() == 1) {
			GraphEdge<QueryNode> e = m_queryGraph.edges().get(0);
			String s = m_queryGraph.getNode(e.getSrc()).getName();
			String t = m_queryGraph.getNode(e.getDst()).getName();
			GTable<String> table = getTable(e, 1);
			for (String[] row : table) {
				Map<String,String> map = new HashMap<String,String>();
				map.put(s, row[0]);
				map.put(t, row[1]);
				m_listener.mapping(map);
			}
			return null;
		}
		
//		final HashMap<GraphEdge<QueryNode>,GTable<Integer>> queryEdge2IndexEdges = new HashMap<GraphEdge<QueryNode>,GTable<Integer>>();
//		
//		for (GraphEdge<QueryNode> queryEdge : m_queryGraph.edges()) {
//			queryEdge2IndexEdges.put(queryEdge, getTable(queryEdge, 0));
//		}
//		
		PriorityQueue<GraphEdge<QueryNode>> toVisit = new PriorityQueue<GraphEdge<QueryNode>>(m_queryGraph.edgeCount(), new Comparator<GraphEdge<QueryNode>>() {
			public int compare(GraphEdge<QueryNode> e1, GraphEdge<QueryNode> e2) {
				String s1 = m_queryGraph.getNode(e1.getSrc()).getSingleMember();
				String s2 = m_queryGraph.getNode(e2.getSrc()).getSingleMember();
				String d1 = m_queryGraph.getNode(e1.getDst()).getSingleMember();
				String d2 = m_queryGraph.getNode(e2.getDst()).getSingleMember();
				
				if (!s1.startsWith("?") || !d1.startsWith("?"))
					return -1;
				if (!s2.startsWith("?") || !d2.startsWith("?"))
					return 1;
				return 0;
				
//				if (queryEdge2IndexEdges.get(e1).rowCount() < queryEdge2IndexEdges.get(e2).rowCount())
//					return -1;
//				else
//					return 1;
			}
		});
		
		Set<GraphEdge<QueryNode>> visited = new HashSet<GraphEdge<QueryNode>>();
		
		toVisit.addAll(m_queryGraph.edges());
		GraphEdge<QueryNode> startEdge = toVisit.peek();
		toVisit.clear();
		toVisit.offer(startEdge);
		
		GTable<String> result = null;
		
		while (toVisit.size() > 0) {
			GraphEdge<QueryNode> currentEdge = toVisit.poll();
			
			if (visited.contains(currentEdge))
				continue;
			visited.add(currentEdge);
			
			String sourceCol = getSourceColumn(currentEdge);
			String targetCol = getTargetColumn(currentEdge);
			
//			log.debug(m_queryGraph.getNode(currentEdge.getSrc()).getSingleMember() + " -> " + m_queryGraph.getNode(currentEdge.getDst()).getSingleMember() + " (" + " rows)");
			
			if (result == null) {
				result = getTable(currentEdge, 0);
			}
			else {
				if (result.hasColumn(sourceCol) && result.hasColumn(targetCol)) {
					GTable<String> currentEdges = getTable(currentEdge, 0);
					result = Tables.hashJoin(result, currentEdges, Arrays.asList(sourceCol, targetCol));
				}
				else if (result.hasColumn(sourceCol)) {
					GTable<String> currentEdges = getTable(currentEdge, 0);
					result.sort(sourceCol, true);
					result = Tables.mergeJoin(result, currentEdges, sourceCol);
				}
				else {
					GTable<String> currentEdges = getTable(currentEdge, 1);
					result.sort(targetCol, true);
					result = Tables.mergeJoin(result, currentEdges, targetCol);
				}
			}
			
			if (result.rowCount() == 0)
				return null;
			
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
//			log.debug(result);
//			result = purgeTable(result);
		}
		log.debug("rows: " + result.rowCount());

		result = purgeTable(result);
//		int[] cols = new int [result.getColumnNames().length];
//		for (int i = 0; i < result.getColumnNames().length; i++)
//			cols[i] = Integer.parseInt(result.getColumnNames()[i]);
//		
//		for (String[] row : result) {
//			Map<String,String> map = new HashMap<String,String>();
//			for (int i = 0; i < row.length; i++)
//				map.put(m_queryGraph.getNode(cols[i]).getName(), row[i]);
//
//			m_listener.mapping(map);
//		}
		return result;
	}
}
