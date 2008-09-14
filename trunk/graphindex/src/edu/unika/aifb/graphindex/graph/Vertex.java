package edu.unika.aifb.graphindex.graph;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.unika.aifb.graphindex.Util;

public class Vertex extends AbstractGraphElement implements Cloneable, Serializable {
	private static final long serialVersionUID = -8665993784469507490L;
	private int m_uniqueId;
	private static int m_currentId = 0;
	private Set<String> m_paths;
	private String m_canonicalLabel;
	
	public Vertex(String label) {
		super(label);
		m_paths = new HashSet<String>();
		m_uniqueId = ++m_currentId;
	}

	public Set<Edge> outgoingEdges() {
		return m_graph.outgoingEdges(this);
	}
	
	public Set<Edge> outgoingEdges(String label) {
		Set<Edge> edges = new HashSet<Edge>();
		for (Edge out : outgoingEdges()) {
			if (out.getLabel().equals(label)) {
				edges.add(out);
			}
		}
		return edges;
	}
	
	public Set<Edge> incomingEdges() {
		return m_graph.incomingEdges(this);
	}
	
	public Set<Edge> incomingEdges(String label) {
		Set<Edge> edges = new HashSet<Edge>();
		for (Edge out : incomingEdges()) {
			if (out.getLabel().equals(label)) {
				edges.add(out);
			}
		}
		return edges;
	}
	
	public Set<String> outgoingEdgeLabels() {
		return m_graph.outgoingEdgeLabels(this);
	}
	
	public Set<String> incomingEdgeLabels() {
		return m_graph.incomingEdgeLabels(this);
	}
	
	public Map<String,List<Vertex>> outgoingEdgeMap() {
		return m_graph.outgoingEdgeMap(this);
	}
	
	public Map<String,List<Vertex>> incomingEdgeMap() {
		return m_graph.incomingEdgeMap(this);
	}

	public int inDegree() {
		return m_graph.inDegreeOf(this);
	}
	
	public int outDegree() {
		return m_graph.outDegreeOf(this);
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((m_label == null) ? 0 : m_label.hashCode());
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
		final AbstractGraphElement other = (AbstractGraphElement) obj;
		if (m_label == null) {
			if (other.m_label != null)
				return false;
		} else if (!m_label.equals(other.m_label))
			return false;
		return true;
	}
	
	public String toString() {
		return Util.truncateUri(m_label);// + "(" + m_uniqueId + ")";
	}
	
	@Override
	public Object clone() {
		Vertex v = new Vertex(m_label);
		return v;
	}
	
	public void addPath(String path) {
		m_paths.add(path);
	}
	
	public Set<String> getPaths() {
		return m_paths;
	}
	
	public void acceptVisitor(GraphVisitor visitor) {
		visitor.visit(this);
	}

	public void setCanonicalLabel(String m_canonicalLabel) {
		this.m_canonicalLabel = m_canonicalLabel;
	}

	public String getCanonicalLabel() {
		return m_canonicalLabel;
	}
}
