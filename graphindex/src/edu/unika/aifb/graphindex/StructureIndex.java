package edu.unika.aifb.graphindex;

import java.util.ArrayList;
import java.util.List;

import edu.unika.aifb.graphindex.graph.IndexGraph;
import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.NamedGraph;
import edu.unika.aifb.graphindex.storage.GraphManager;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.StorageManager;

public class StructureIndex {
	private GraphManager m_gm;
	private List<IndexGraph> m_graphs;
	
	public StructureIndex() {
		m_gm = StorageManager.getInstance().getGraphManager();
		m_graphs = new ArrayList<IndexGraph>();
	}
	
	public void load() throws StorageException {
		for (String name : m_gm.getStoredGraphs()) {
			m_graphs.add(new IndexGraph(m_gm.graph(name), 1));
		}
	}
	
	public List<IndexGraph> getIndexGraphs() {
		return m_graphs;
	}
}
