package edu.unika.aifb.graphindex.graph;

import java.util.List;

public interface GraphStorageEngine {
	public void writeGraph(Graph g);
	public void readGraph(Graph g);
	public void removeGraph(Graph g);
	public boolean loadStub(Graph g);
	public List<String> getStoredGraphs();
	public void setPrefix(String prefix);
	public void init();
}
