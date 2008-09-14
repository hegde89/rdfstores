package edu.unika.aifb.graphindex;

import java.util.ArrayList;
import java.util.List;

import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.NamedGraph;
import edu.unika.aifb.graphindex.storage.GraphManager;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.StorageManager;

public class StructureIndex {
	private GraphManager m_gm;
	private List<NamedGraph<String,LabeledEdge<String>>> m_graphs;
	
	public StructureIndex() {
		m_gm = StorageManager.getInstance().getGraphManager();
		m_graphs = new ArrayList<NamedGraph<String,LabeledEdge<String>>>();
	}
	
	public void load() throws StorageException {
		for (String name : m_gm.getStoredGraphs()) {
			m_graphs.add(m_gm.graph(name));
		}
	}
	
	public List<NamedGraph<String,LabeledEdge<String>>> getIndexGraphs() {
		return m_graphs;
	}
}
