package edu.unika.aifb.graphindex.graph;

public class IndexEdge {

	private String m_label;
	
	public IndexEdge(LabeledEdge<String> e, IndexGraph graph) {
		m_label = e.getLabel();
	}

	public String getLabel() {
		return m_label;
	}
}
