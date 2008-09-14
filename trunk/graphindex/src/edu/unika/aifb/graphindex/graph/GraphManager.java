package edu.unika.aifb.graphindex.graph;

import java.util.List;

@Deprecated
public class GraphManager {
	private GraphStorageEngine m_engine;
	
	private static GraphManager m_instance;

	private GraphManager() {
	}
	
	public static GraphManager getInstance() {
		if (m_instance == null)
			m_instance = new GraphManager();
		return m_instance;
	}
	
	public void setStorageEngine(GraphStorageEngine engine) {
		m_engine = engine;
	}
	
	public void readGraph(Graph g) {
		m_engine.readGraph(g);
	}
	
	public void writeGraph(Graph g) {
		m_engine.writeGraph(g);
	}
	
	public void removeGraph(Graph g) {
		m_engine.removeGraph(g);
	}
	
	public boolean loadStub(Graph g) {
		return m_engine.loadStub(g);
	}
	
	public List<String> getStoredGraphs() {
		return m_engine.getStoredGraphs();
	}
}
