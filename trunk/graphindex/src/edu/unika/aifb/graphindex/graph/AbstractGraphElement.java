package edu.unika.aifb.graphindex.graph;

import java.io.Serializable;

public abstract class AbstractGraphElement implements GraphElement, GraphVisitable, Serializable {
	private static final long serialVersionUID = 4330418451784362205L;
	protected Graph m_graph;
	protected String m_label;
	
	protected AbstractGraphElement(String label) {
		m_label = label;
	}

	public void setGraph(Graph g) {
		m_graph = g;
	}

	public Graph getGraph() {
		return m_graph;
	}

	public String getLabel() {
		return m_label;
	}
}
