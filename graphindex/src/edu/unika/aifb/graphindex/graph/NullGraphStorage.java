package edu.unika.aifb.graphindex.graph;

import java.util.ArrayList;
import java.util.List;

public class NullGraphStorage implements GraphStorageEngine {

	public List<String> getStoredGraphs() {
		return new ArrayList<String>();
	}

	public void init() {
	}

	public boolean loadStub(Graph g) {
		return false;
	}

	public void readGraph(Graph g) {
	}

	public void removeGraph(Graph g) {
	}

	public void setPrefix(String prefix) {
	}

	public void writeGraph(Graph g) {
	}

}
