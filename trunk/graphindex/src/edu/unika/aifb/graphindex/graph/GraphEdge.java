package edu.unika.aifb.graphindex.graph;

public class GraphEdge<V> {

	private int m_src, m_dst;
	private String m_label;
	
	public GraphEdge(LabeledEdge<V> e, int src, int dst, Graph graph) {
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
	
	public String toString() {
		return m_label + "(" + m_src + "," + m_dst + ")";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + m_dst;
		result = prime * result + ((m_label == null) ? 0 : m_label.hashCode());
		result = prime * result + m_src;
		return result;
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		GraphEdge other = (GraphEdge)obj;
		if (m_dst != other.m_dst)
			return false;
		if (m_src != other.m_src)
			return false;
		if (m_label == null) {
			if (other.m_label != null)
				return false;
		} else if (!m_label.equals(other.m_label))
			return false;
		return true;
	}
	
	
}
