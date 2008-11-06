package edu.unika.aifb.graphindex.graph;

public class IndexEdge {

	private int m_src, m_dst;
	private String m_label;
	
	public IndexEdge(LabeledEdge<String> e, int src, int dst, IndexGraph graph) {
		m_label = e.getLabel();
		m_src = src;
		m_dst = dst;
	}
	
	public int getSrc() {
		return m_src;
	}
	
	public int getDst() {
		return m_dst;
	}

	public String getLabel() {
		return m_label;
	}
}
