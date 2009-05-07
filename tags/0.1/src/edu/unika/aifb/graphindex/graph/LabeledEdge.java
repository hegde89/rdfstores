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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((m_dst == null) ? 0 : m_dst.hashCode());
		result = prime * result + ((m_label == null) ? 0 : m_label.hashCode());
		result = prime * result + ((m_src == null) ? 0 : m_src.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		LabeledEdge other = (LabeledEdge)obj;
		if (m_dst == null) {
			if (other.m_dst != null)
				return false;
		} else if (!m_dst.equals(other.m_dst))
			return false;
		if (m_label == null) {
			if (other.m_label != null)
				return false;
		} else if (!m_label.equals(other.m_label))
			return false;
		if (m_src == null) {
			if (other.m_src != null)
				return false;
		} else if (!m_src.equals(other.m_src))
			return false;
		return true;
	}
}
