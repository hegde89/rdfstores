package edu.unika.aifb.graphindex.graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jgrapht.EdgeFactory;
import org.jgrapht.graph.DirectedMultigraph;

import edu.unika.aifb.graphindex.storage.GraphStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.StorageManager;

/**
 * A subclass of DirectedMultigraph, which adds a name and the ability to store and load the graph
 * using the graph storage interface. Vertices and edge labels have to be strings.
 * 
 * @author gl
 *
 * @param <V> vertex type
 * @param <E> edge type
 */
public class NamedGraph<V extends String, E extends LabeledEdge<String>> extends DirectedMultigraph<String,LabeledEdge<String>> {
	
	private static final long serialVersionUID = -6948953756502811617L;
	
	private String m_name;
	private GraphStorage m_gs;
	
	private Map<String,Set<String>> m_predecessors;
	private Map<String,Set<String>> m_successors;
	
	private Map<String,Map<String,Set<String>>> m_labelPredecessors;
	private Map<String,Map<String,Set<String>>> m_labelSuccessors;

	private Map<String,Map<String,List<LabeledEdge<String>>>> m_predecessorEdges;
	private Map<String,Map<String,List<LabeledEdge<String>>>> m_successorEdges;
	
	private Map<String,Set<String>> m_inEdgeLabels;
	private Map<String,Set<String>> m_outEdgeLabels;
	
	private Map<String,Set<LabeledEdge<String>>> m_label2Edges;
	
	
	
	public NamedGraph(String name, Class<? extends E> edgeClass) throws StorageException {
		super(edgeClass);
		m_name = name;
		initialize();
	}

	public NamedGraph(String name, EdgeFactory<String,LabeledEdge<String>> ef) throws StorageException {
		super(ef);
		m_name = name;
		initialize();
	}
	
	private void initialize() throws StorageException {
		if (StorageManager.getInstance().getGraphManager() != null) {
			m_gs = StorageManager.getInstance().getGraphManager().getGraphStorage();
			
			Set<LabeledEdge<String>> edges = m_gs.loadEdges(m_name);
			for (LabeledEdge<String> edge : edges) {
				addVertex(edge.getSrc());
				addVertex(edge.getDst());
				addEdge(edge.getSrc(), edge.getDst(), edge);
			}
		}
	}
	
	public String getName() {
		return m_name;
	}
	
	public void store() throws StorageException {
		m_gs.saveEdges(m_name, edgeSet());
	}
	
	public void calc() {
		m_predecessors = new HashMap<String,Set<String>>();
		m_successors = new HashMap<String,Set<String>>();
		m_predecessorEdges = new HashMap<String,Map<String,List<LabeledEdge<String>>>>();
		m_successorEdges = new HashMap<String,Map<String,List<LabeledEdge<String>>>>();
		m_inEdgeLabels = new HashMap<String,Set<String>>();
		m_outEdgeLabels = new HashMap<String,Set<String>>();
		m_label2Edges = new HashMap<String,Set<LabeledEdge<String>>>();
		m_labelPredecessors = new HashMap<String,Map<String,Set<String>>>();
		m_labelSuccessors = new HashMap<String,Map<String,Set<String>>>();
		
		for (String v : vertexSet()) {
			for (LabeledEdge<String> in : incomingEdgesOf(v)) {
				Set<String> labels = m_inEdgeLabels.get(v);
				if (labels == null) {
					labels = new HashSet<String>();
					m_inEdgeLabels.put(v, labels);
				}
				labels.add(in.getLabel());
				
				Set<String> preds = m_predecessors.get(v);
				if (preds == null) {
					preds = new HashSet<String>();
					m_predecessors.put(v, preds);
				}
				preds.add(in.getSrc());
				
				Map<String,List<LabeledEdge<String>>> predList = m_predecessorEdges.get(v);
				if (predList == null) {
					predList = new HashMap<String,List<LabeledEdge<String>>>();
					m_predecessorEdges.put(v, predList);
				}
				
				List<LabeledEdge<String>> edges = predList.get(in.getSrc());
				if (edges == null) {
					edges = new ArrayList<LabeledEdge<String>>();
					predList.put(in.getSrc(), edges);
				}
				edges.add(in);
			}
			
			for (LabeledEdge<String> out : outgoingEdgesOf(v)) {
				Set<String> labels = m_outEdgeLabels.get(v);
				if (labels == null) {
					labels = new HashSet<String>();
					m_outEdgeLabels.put(v, labels);
				}
				labels.add(out.getLabel());
				
				Set<String> succs = m_successors.get(v);
				if (succs == null) {
					succs = new HashSet<String>();
					m_successors.put(v, succs);
				}
				succs.add(out.getDst());

				Map<String,List<LabeledEdge<String>>> succList = m_successorEdges.get(v);
				if (succList == null) {
					succList = new HashMap<String,List<LabeledEdge<String>>>();
					m_successorEdges.put(v, succList);
				}
				
				List<LabeledEdge<String>> edges = succList.get(out.getDst());
				if (edges == null) {
					edges = new ArrayList<LabeledEdge<String>>();
					succList.put(out.getDst(), edges);
				}
				edges.add(out);
			}
		}
		
		for (LabeledEdge<String> e : edgeSet()) {
			Set<LabeledEdge<String>> edges = m_label2Edges.get(e.getLabel());
			if (edges == null) {
				edges = new HashSet<LabeledEdge<String>>();
				m_label2Edges.put(e.getLabel(), edges);
			}
			edges.add(e);
		}
		
		for (String label : m_label2Edges.keySet()) {
			Map<String,Set<String>> predecessors = new HashMap<String,Set<String>>();
			m_labelPredecessors.put(label, predecessors);
			
			Map<String,Set<String>> successors = new HashMap<String,Set<String>>();
			m_labelSuccessors.put(label, successors);
			
			for (String v : vertexSet()) {
				Set<String> preds = new HashSet<String>();
				predecessors.put(v, preds);

				Set<String> succs = new HashSet<String>();
				successors.put(v, succs);
				
				for (LabeledEdge<String> in : incomingEdgesOf(v))
					if (in.getLabel().equals(label))
						preds.add(in.getSrc());
				
				for (LabeledEdge<String> out : outgoingEdgesOf(v))
					if (out.getLabel().equals(label))
						preds.add(out.getDst());
			}
		}
	}
	
	public Set<LabeledEdge<String>> edgesByLabel(String label) {
		Set<LabeledEdge<String>> edges = m_label2Edges.get(label);
		if (edges == null)
			return new HashSet<LabeledEdge<String>>();
		return edges;
	}
	
	public Set<String> predecessors(V v) {
		Set<String> vs = m_predecessors.get(v);
		if (vs == null)
			return new HashSet<String>();
		return vs;
	}
	
	public Set<String> successors(V v) {
		Set<String> vs = m_successors.get(v);
		if (vs == null)
			return new HashSet<String>();
		return vs;
	}
	
	public Set<String> predecessors(V v, String label) {
		return m_labelPredecessors.get(label).get(v);
	}
	
	public Set<String> successors(V v, String label) {
		return m_labelSuccessors.get(label).get(v);
	}

	public Map<String,List<LabeledEdge<String>>> predecessorEdges(V v) {
		Map<String,List<LabeledEdge<String>>> map = m_predecessorEdges.get(v);
		if (map == null)
			return new HashMap<String,List<LabeledEdge<String>>>();
		return map;
	}
	
	public Map<String,List<LabeledEdge<String>>> successorEdges(V v) {
		Map<String,List<LabeledEdge<String>>> map = m_successorEdges.get(v);
		if (map == null)
			return new HashMap<String,List<LabeledEdge<String>>>();
		return map;
	}
	
	public Set<String> inEdgeLabels(V v) {
		Set<String> labels = m_inEdgeLabels.get(v);
		if (labels == null)
			return new HashSet<String>();
		return labels;
	}
	
	public Set<String> outEdgeLabels(V v) {
		Set<String> labels = m_outEdgeLabels.get(v);
		if (labels == null) 
			return new HashSet<String>();
		return labels;
	}
	
	/**
	 * Shortcut method to add an edge, without having to add the vertices beforehand.
	 * 
	 * @param src
	 * @param edge
	 * @param dst
	 */
	public void addEdge(V src, String edge, V dst) {
		addVertex(src);
		addVertex(dst);
		addEdge(src, dst, new LabeledEdge<String>(src, dst, edge));
	}
	
	public String toString() {
		return m_name + "(" + vertexSet().size() + "," + edgeSet().size() + ")";
	}
}
