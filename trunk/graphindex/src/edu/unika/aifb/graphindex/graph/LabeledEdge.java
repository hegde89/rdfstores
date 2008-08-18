package edu.unika.aifb.graphindex.graph;

import org.jgrapht.graph.DefaultEdge;

public class LabeledEdge<V> extends DefaultEdge {
	private static final long serialVersionUID = -4026822838994568960L;
	
	private V m_src;
	private V m_dst;
	private String m_label;
	
	public LabeledEdge(V src, V dst, String label) {
		m_src = src;
		m_dst = dst;
		m_label = label;
	}

	public V getSrc() {
		return m_src;
	}

	public V getDst() {
		return m_dst;
	}

	public String getLabel() {
		return m_label;
	}
}
