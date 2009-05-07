package edu.unika.aifb.graphindex.storage;

import java.util.List;
import java.util.Set;

import org.jgrapht.graph.ClassBasedEdgeFactory;

import edu.unika.aifb.graphindex.StructureIndex;
import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.NamedGraph;

public class GraphManagerImpl implements GraphManager {

	protected GraphStorage m_gs;
	protected StructureIndex m_index;
	protected int m_id = 0;
	
	public GraphManagerImpl() {
		
	}
	
	public void initialize(boolean clean, boolean readonly) throws StorageException {
		m_gs.initialize(clean, readonly);
	}

	public void close() throws StorageException {
		m_gs.close();
	}

	public void setIndex(StructureIndex index) {
		m_index = index;
	}
	
	public StructureIndex getIndex() {
		return m_index;
	}

	public void setGraphStorage(GraphStorage gs) {
		m_gs = gs;
	}

	public GraphStorage getGraphStorage() {
		return m_gs;
	}

	public NamedGraph<String,LabeledEdge<String>> graph(String graphName) throws StorageException {
		NamedGraph<String,LabeledEdge<String>> g = new NamedGraph<String,LabeledEdge<String>>(graphName, this, new ClassBasedEdgeFactory<String,LabeledEdge<String>>((Class<? extends LabeledEdge<String>>)LabeledEdge.class));
		return g;
	}
	
	public NamedGraph<String,LabeledEdge<String>> graph() throws StorageException {
		NamedGraph<String,LabeledEdge<String>> g = new NamedGraph<String,LabeledEdge<String>>("ngraph" + ++m_id, this, new ClassBasedEdgeFactory<String,LabeledEdge<String>>((Class<? extends LabeledEdge<String>>)LabeledEdge.class));
		return g;
	}

	public Set<String> getStoredGraphs() throws StorageException {
		return m_gs.loadGraphList();
	}
}
