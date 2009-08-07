package edu.unika.aifb.graphindex.graph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.unika.aifb.graphindex.storage.GraphManager;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.StorageManager;

public class IndexGraph {
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
	private String[] m_labels;
	private int[] m_inDegrees, m_outDegrees;
	private List<Integer>[] m_predecessors, m_successors;
	private Map<String,List<Integer>>[] m_labelPredecessors, m_labelSuccessors;
	private Map<Integer,List<IndexEdge>>[] m_predecessorEdges, m_successorEdges;
	private Set<String>[] m_outLabels, m_inLabels, m_allLabels;

	private final List<Integer> m_emptyIntegerList = new ArrayList<Integer>();
	private final Map<Integer,List<IndexEdge>> m_emptyMap = new HashMap<Integer,List<IndexEdge>>();
	private final Set<String> m_emptyStringSet = new HashSet<String>();
	private final List<IndexEdge> m_emptyEdgeList = new ArrayList<IndexEdge>();
	
	private static int m_id = 0;

	public IndexGraph(NamedGraph<String,LabeledEdge<String>> graph) {
		this(graph, 1);
	}
	
	@SuppressWarnings("unchecked")
	public IndexGraph(NamedGraph<String,LabeledEdge<String>> graph, final int order) {
		m_nodeCount = graph.vertexSet().size();
		
		List<String> vertices = new ArrayList<String>(graph.vertexSet());
		Collections.sort(vertices, new Comparator<String>() {
			public int compare(String o1, String o2) {
				if (o1.startsWith("?") && !o2.startsWith("?"))
					return 1;
				else if (!o1.startsWith("?") && o2.startsWith("?"))
					return -1;
				else
					return o1.compareTo(o2) * order;
			}
		});
		Map<String,Integer> v2i = new HashMap<String,Integer>();
		
		m_name = "igraph" + ++m_id;
		
		int i = 0;
		m_labels = new String [m_nodeCount];
		for (String vertex : vertices) {
			m_labels[i] = vertex;
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
			Map<Integer,List<IndexEdge>> predList = new HashMap<Integer,List<IndexEdge>>();
			Set<String> inLabels = new HashSet<String>();
			for (LabeledEdge<String> e : graph.incomingEdgesOf(m_labels[i])) {
				int pred = v2i.get(e.getSrc());
				
				preds.add(pred);
				
				List<Integer> lpreds = labelPreds.get(e.getLabel());
				if (lpreds == null) {
					lpreds = new ArrayList<Integer>();
					labelPreds.put(e.getLabel(), lpreds);
				}
				lpreds.add(pred);

				List<IndexEdge> edges = predList.get(pred);
				if (edges == null) {
					edges = new ArrayList<IndexEdge>();
					predList.put(pred, edges);
				}
				edges.add(new IndexEdge(e, pred, i, this));
				
				inLabels.add(e.getLabel());
				
				m_edgeLabels.add(e.getLabel());
				m_edgeCount++;
				processInEdge(i, m_labels[i], e);
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
			Map<Integer,List<IndexEdge>> succList = new HashMap<Integer,List<IndexEdge>>();
			Set<String> outLabels = new HashSet<String>();
			for (LabeledEdge<String> e : graph.outgoingEdgesOf(m_labels[i])) {
				int succ = v2i.get(e.getDst());
				
				succs.add(succ);

				List<Integer> lsuccs = labelSuccs.get(e.getLabel());
				if (lsuccs == null) {
					lsuccs = new ArrayList<Integer>();
					labelSuccs.put(e.getLabel(), lsuccs);
				}
				lsuccs.add(succ);

				List<IndexEdge> edges = succList.get(succ);
				if (edges == null) {
					edges = new ArrayList<IndexEdge>();
					succList.put(succ, edges);
				}
				edges.add(new IndexEdge(e, i, succ, this));
				
				outLabels.add(e.getLabel());

				m_edgeLabels.add(e.getLabel());
				processOutEdge(i, m_labels[i], e);
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
	
	protected void processOutEdge(int node, String label, LabeledEdge<String> e) {
	}

	protected void processInEdge(int node, String label, LabeledEdge<String> e) {
	}

	public int edgeCount() {
		return m_edgeCount;
	}
	
	public int nodeCount() {
		return m_nodeCount;
	}
	
	public String[] nodeLabels() {
		return m_labels;
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

	public String getNodeLabel(int node) {
		return m_labels[node];
	}

	public int outDegreeOf(int node) {
		return m_successors[node] == null ? 0 : m_successors[node].size();
	}

	public int inDegreeOf(int node) {
		return m_predecessors[node] == null ? 0 : m_predecessors[node].size();
	}

	public Set<String> edgeLabels(int node) {
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

	public Map<Integer,List<IndexEdge>> predecessorEdges(int node) {
		Map<Integer,List<IndexEdge>> map = m_predecessorEdges[node];
		if (map == null)
			return m_emptyMap;
		return map;
	}

	public Map<Integer,List<IndexEdge>> successorEdges(int node) {
		Map<Integer,List<IndexEdge>> map = m_successorEdges[node];
		if (map == null)
			return m_emptyMap;
		return map;
	}
	
	public List<IndexEdge> incomingEdges(int node) {
		Map<Integer,List<IndexEdge>> predEdges = m_predecessorEdges[node];
		if (predEdges == null)
			return m_emptyEdgeList;
		List<IndexEdge> edges = new ArrayList<IndexEdge>();
		for (List<IndexEdge> es : predEdges.values())
			edges.addAll(es);
		return edges;
	}
	
	public List<IndexEdge> outgoingEdges(int node) {
		Map<Integer,List<IndexEdge>> succEdges = m_successorEdges[node];
		if (succEdges == null)
			return m_emptyEdgeList;
		List<IndexEdge> edges = new ArrayList<IndexEdge>();
		for (List<IndexEdge> es : succEdges.values())
			edges.addAll(es);
		return edges;
	}

	public Set<String> edgeLabelSet() {
		return m_edgeLabels;
	}

	public void store(GraphManager gm) throws StorageException {
		for (int i = 0; i < m_nodeCount; i++) {
			for (IndexEdge e : outgoingEdges(i))
				gm.getGraphStorage().addEdge(m_name, getNodeLabel(i), e.getLabel(), getNodeLabel(e.getDst()));
		}
	}
	
	public String toString() {
		return "IndexGraph(" + m_nodeCount + "," + m_edgeCount + ")";
	}
}