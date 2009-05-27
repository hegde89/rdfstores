package edu.unika.aifb.graphindex.graph;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jgrapht.DirectedGraph;

import edu.unika.aifb.graphindex.StructureIndex;
import edu.unika.aifb.graphindex.storage.GraphManager;
import edu.unika.aifb.graphindex.storage.StorageException;

public class Graph<V extends Comparable<V>> {
	private class ArrayValueIterator<E> implements Iterator<E> {
		private int pos = 0;
		private E[] array;
		
		public ArrayValueIterator(E[] array) {
			this.array = array;
		}
		public boolean hasNext() {
			return pos < array.length;
		}

		public E next() {
			return array[pos];
		}

		public void remove() {
			throw new UnsupportedOperationException("remove not supported");
		}
	}
	
	private String m_name;
	private int m_nodeCount;
	private int m_edgeCount;
	private Set<String> m_edgeLabels;
	private Object[] m_nodeObjects;
	private int[] m_inDegrees, m_outDegrees;
	private List<Integer>[] m_predecessors, m_successors;
	private Map<String,List<Integer>>[] m_labelPredecessors, m_labelSuccessors;
	private Map<Integer,List<GraphEdge<V>>>[] m_predecessorEdges, m_successorEdges;
	private Set<String>[] m_outLabels, m_inLabels, m_allLabels;
	private Map<V,Integer> m_no2id;

	private final List<Integer> m_emptyIntegerList = new ArrayList<Integer>();
	private final Map<Integer,List<GraphEdge<V>>> m_emptyMap = new HashMap<Integer,List<GraphEdge<V>>>();
	private final Set<String> m_emptyStringSet = new HashSet<String>();
	private final List<GraphEdge<V>> m_emptyEdgeList = new ArrayList<GraphEdge<V>>();
	
	private static int m_id = 0;

	public Graph(DirectedGraph<V,LabeledEdge<V>> graph) {
		this(graph, null);
	}
	
	@SuppressWarnings("unchecked")
	public Graph(DirectedGraph<V,LabeledEdge<V>> graph, Comparator<V> comparator) {
		m_nodeCount = graph.vertexSet().size();
		
		List<V> vertices = new ArrayList<V>(graph.vertexSet());
		if (comparator == null)
			Collections.sort(vertices);
		else
			Collections.sort(vertices, comparator);

		m_name = "igraph" + ++m_id;
		
		m_nodeObjects = new Object [m_nodeCount];
		Map<V,Integer> v2i = new HashMap<V,Integer>();
		int i = 0;
		for (V vertex : vertices) {
			m_nodeObjects[i] = vertex;
			v2i.put(vertex, i);

			i++;
		}

		m_inDegrees = new int [m_nodeCount];
		m_outDegrees = new int [m_nodeCount];
		m_edgeLabels = new HashSet<String>();
		m_predecessors = new List [m_nodeCount];
		m_successors = new List [m_nodeCount];
		m_labelPredecessors = new Map [m_nodeCount];
		m_labelSuccessors = new Map [m_nodeCount];
		m_predecessorEdges = new Map [m_nodeCount];
		m_successorEdges = new Map [m_nodeCount];
		m_allLabels = new Set [m_nodeCount];
		m_inLabels = new Set [m_nodeCount];
		m_outLabels = new Set [m_nodeCount];
		
		for (i = 0; i < m_nodeCount; i++) {
			List<Integer> preds = new ArrayList<Integer>();
			Map<String,List<Integer>> labelPreds = new HashMap<String,List<Integer>>();
			Map<Integer,List<GraphEdge<V>>> predList = new HashMap<Integer,List<GraphEdge<V>>>();
			Set<String> inLabels = new HashSet<String>();
			for (LabeledEdge<V> e : graph.incomingEdgesOf((V)m_nodeObjects[i])) {
				int pred = v2i.get(e.getSrc());
				
				preds.add(pred);
				
				List<Integer> lpreds = labelPreds.get(e.getLabel());
				if (lpreds == null) {
					lpreds = new ArrayList<Integer>();
					labelPreds.put(e.getLabel(), lpreds);
				}
				lpreds.add(pred);

				List<GraphEdge<V>> edges = predList.get(pred);
				if (edges == null) {
					edges = new ArrayList<GraphEdge<V>>();
					predList.put(pred, edges);
				}
				edges.add(new GraphEdge<V>(e, pred, i, this));
				
				inLabels.add(e.getLabel());
				
				m_edgeLabels.add(e.getLabel());
				m_edgeCount++;
				processInEdge(i, (V)m_nodeObjects[i], e);
			}
			m_inDegrees[i] = preds.size();
			if (preds.size() > 0)
				m_predecessors[i] = preds;
			if (labelPreds.size() > 0)
				m_labelPredecessors[i] = labelPreds;
			if (predList.size() > 0)
				m_predecessorEdges[i] = predList;
			if (inLabels.size() > 0)
				m_inLabels[i] = inLabels;

			List<Integer> succs = new ArrayList<Integer>();
			Map<String,List<Integer>> labelSuccs = new HashMap<String,List<Integer>>();
			Map<Integer,List<GraphEdge<V>>> succList = new HashMap<Integer,List<GraphEdge<V>>>();
			Set<String> outLabels = new HashSet<String>();
			for (LabeledEdge<V> e : graph.outgoingEdgesOf((V)m_nodeObjects[i])) {
				int succ = v2i.get(e.getDst());
				
				succs.add(succ);

				List<Integer> lsuccs = labelSuccs.get(e.getLabel());
				if (lsuccs == null) {
					lsuccs = new ArrayList<Integer>();
					labelSuccs.put(e.getLabel(), lsuccs);
				}
				lsuccs.add(succ);

				List<GraphEdge<V>> edges = succList.get(succ);
				if (edges == null) {
					edges = new ArrayList<GraphEdge<V>>();
					succList.put(succ, edges);
				}
				edges.add(new GraphEdge<V>(e, i, succ, this));
				
				outLabels.add(e.getLabel());

				m_edgeLabels.add(e.getLabel());
				processOutEdge(i, (V)m_nodeObjects[i], e);
			}
			m_outDegrees[i] = succs.size();
			if (succs.size() > 0)
				m_successors[i] = succs;
			if (labelSuccs.size() > 0)
				m_labelSuccessors[i] = labelSuccs;
			if (succList.size() > 0)
				m_successorEdges[i] = succList;
			if (outLabels.size() > 0)
				m_outLabels[i] = outLabels;
			
			Set<String> allLabels = new HashSet<String>(inLabels);
			allLabels.addAll(outLabels);
			if (allLabels.size() > 0)
				m_allLabels[i] = allLabels;
		}
	}
	
	protected void processOutEdge(int node, V nodeObject, LabeledEdge<V> e) {
	}

	protected void processInEdge(int node, V nodeObject, LabeledEdge<V> e) {
	}

	public int edgeCount() {
		return m_edgeCount;
	}
	
	public int nodeCount() {
		return m_nodeCount;
	}
	
	public Object[] nodes() {
		return m_nodeObjects;
	}

	public List<Integer> predecessors(int node) {
		List<Integer> preds = m_predecessors[node];
		if (preds == null)
			return m_emptyIntegerList ;
		return preds;
	}

	public List<Integer> successors(int node) {
		List<Integer> succs = m_successors[node];
		if (succs == null)
			return m_emptyIntegerList ;
		return succs;
	}
	
	public List<Integer> predecessors(int node, String label) {
		Map<String,List<Integer>> labels = m_labelPredecessors[node];
		if (labels == null)
			return m_emptyIntegerList;
		List<Integer> nodes = labels.get(label);
		if (nodes == null)
			return m_emptyIntegerList;
		return nodes;
	}

	public List<Integer> successors(int node, String label) {
		Map<String,List<Integer>> labels = m_labelSuccessors[node];
		if (labels == null)
			return m_emptyIntegerList;
		List<Integer> nodes = labels.get(label);
		if (nodes == null)
			return m_emptyIntegerList;
		return nodes;
	}
	
	public Set<Integer> predecessors(int node, Set<String> labels) {
		Set<Integer> set = new HashSet<Integer>();
		for (String label : labels)
			set.addAll(predecessors(node, label));
		return set;
	}
	
	public Set<Integer> successors(int node, Set<String> labels) {
		Set<Integer> set = new HashSet<Integer>();
		for (String label : labels)
			set.addAll(successors(node, label));
		return set;
	}

	@SuppressWarnings("unchecked")
	public V getNode(int node) {
		return (V)m_nodeObjects[node];
	}
	
	public V getSourceNode(GraphEdge<V> edge) {
		return getNode(edge.getSrc());
	}

	public V getTargetNode(GraphEdge<V> edge) {
		return getNode(edge.getDst());
	}

	public int outDegreeOf(int node) {
		return m_successors[node] == null ? 0 : m_successors[node].size();
	}

	public int inDegreeOf(int node) {
		return m_predecessors[node] == null ? 0 : m_predecessors[node].size();
	}

	public Set<String> allEdgeLabels(int node) {
		Set<String> labels = m_allLabels[node];
		if (labels == null)
			return m_emptyStringSet;
		return labels;
	}

	public Set<String> outEdgeLabels(int node) {
		Set<String> labels = m_outLabels[node];
		if (labels == null)
			return m_emptyStringSet;
		return labels;
	}

	public Set<String> inEdgeLabels(int node) {
		Set<String> labels = m_inLabels[node];
		if (labels == null)
			return m_emptyStringSet;
		return labels;
	}

	public Map<Integer,List<GraphEdge<V>>> predecessorEdges(int node) {
		Map<Integer,List<GraphEdge<V>>> map = m_predecessorEdges[node];
		if (map == null)
			return m_emptyMap;
		return map;
	}

	public Map<Integer,List<GraphEdge<V>>> successorEdges(int node) {
		Map<Integer,List<GraphEdge<V>>> map = m_successorEdges[node];
		if (map == null)
			return m_emptyMap;
		return map;
	}
	
	public List<GraphEdge<V>> incomingEdges(int node) {
		Map<Integer,List<GraphEdge<V>>> predEdges = m_predecessorEdges[node];
		if (predEdges == null)
			return m_emptyEdgeList;
		List<GraphEdge<V>> edges = new ArrayList<GraphEdge<V>>();
		for (List<GraphEdge<V>> es : predEdges.values())
			edges.addAll(es);
		return edges;
	}
	
	public List<GraphEdge<V>> outgoingEdges(int node) {
		Map<Integer,List<GraphEdge<V>>> succEdges = m_successorEdges[node];
		if (succEdges == null)
			return m_emptyEdgeList;
		List<GraphEdge<V>> edges = new ArrayList<GraphEdge<V>>();
		for (List<GraphEdge<V>> es : succEdges.values())
			edges.addAll(es);
		return edges;
	}

	public List<GraphEdge<V>> edges() { 
		List<GraphEdge<V>> edges = new ArrayList<GraphEdge<V>>();
		for (int i = 0; i < nodeCount(); i++)
			edges.addAll(outgoingEdges(i));
		return edges;
	}
	
	public Set<String> edgeLabelSet() {
		return m_edgeLabels;
	}

	public void store(StructureIndex index) throws StorageException {
		GraphManager gm = index.getGraphManager();
		for (int i = 0; i < m_nodeCount; i++) {
			for (GraphEdge<V> e : outgoingEdges(i))
				gm.getGraphStorage().addEdge(m_name, getNode(i).toString(), e.getLabel(), getNode(e.getDst()).toString());
		}
	}
	
	public String toString() {
		return "Graph(n" + m_nodeCount + ",e" + m_edgeCount + ")";
	}

	public int getNodeId(V nodeObject) {
		if (m_no2id == null) {
			m_no2id = new HashMap<V,Integer>();
			for (int i = 0; i < nodeCount(); i++)
				m_no2id.put(getNode(i), i);
		}
		return m_no2id.get(nodeObject);
	}
	
	public void save() {
		try {
			Class.forName ("com.mysql.jdbc.Driver");
			Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/graphindex", "graphindex", "blah");
			Statement st = conn.createStatement();
			st.execute("DROP TABLE IF EXISTS graphdata");
			st.execute("CREATE TABLE graphdata (source int not null, edge int not null, target int not null)");
			st.execute("CREATE INDEX index_edge ON graphdata (edge)");
			st.execute("CREATE INDEX index_src ON graphdata (source)");
			st.execute("CREATE INDEX index_dst ON graphdata (target)");
			st.execute("LOCK TABLES graphdata WRITE");
			st.close();
			
			Map<String,Integer> l2i = new HashMap<String,Integer>();
			int i = 0;
			
			PreparedStatement pst = conn.prepareStatement("INSERT INTO graphdata (source, edge, target) VALUES(?, ?, ?)");
			for (GraphEdge<V> e : edges()){
				Integer id = l2i.get(e.getLabel());
				if (id == null) {
					id = ++i;
					l2i.put(e.getLabel(), id);
					System.out.println(id + " " + e.getLabel());
				}
				pst.setInt(1, e.getSrc());
				pst.setInt(2, id);
				pst.setInt(3, e.getDst());
				pst.addBatch();
			}
			pst.executeBatch();
			pst.close();
			
			st = conn.createStatement();
			st.execute("UNLOCK TABLES");
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}
