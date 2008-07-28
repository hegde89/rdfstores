package edu.unika.aifb.graphindex.graph;

import java.io.Serializable;

import org.jgrapht.graph.DefaultEdge;

import edu.unika.aifb.graphindex.Util;

public class Edge extends AbstractGraphElement implements Serializable {
	private static final long serialVersionUID = 7985131274155615879L;
	private Vertex m_source, m_target;
	
	public Edge() {
		super(null);
	}
	
	public Edge(Vertex source, Vertex target, String label) {
		super(label);
		m_source = source;
		m_target = target;
	}

	public Vertex getSource() {
		return m_source;
	}

	public Vertex getTarget() {
		return m_target;
	}

	public String toString() {
		return Util.truncateUri(m_label) + "(" + m_source + "," + m_target + ")";
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((m_label == null) ? 0 : m_label.hashCode());
		result = prime * result
				+ ((m_source == null) ? 0 : m_source.hashCode());
		result = prime * result
				+ ((m_target == null) ? 0 : m_target.hashCode());
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
		final Edge other = (Edge) obj;
		if (m_label == null) {
			if (other.m_label != null)
				return false;
		} else if (!m_label.equals(other.m_label))
			return false;
		if (m_source == null) {
			if (other.m_source != null)
				return false;
		} else if (!m_source.equals(other.m_source))
			return false;
		if (m_target == null) {
			if (other.m_target != null)
				return false;
		} else if (!m_target.equals(other.m_target))
			return false;
		return true;
	}
	
	public void acceptVisitor(GraphVisitor visitor) {
		visitor.visit(this);
	}
}
