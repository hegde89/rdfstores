package edu.unika.aifb.graphindex.query;

import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.query.model.Predicate;

public class LabeledQueryEdge<V extends String> extends LabeledEdge<String> {

	private static final long serialVersionUID = -2845664930936524135L;

	private Predicate m_predicate;
	
	public LabeledQueryEdge(V src, V dst, String label, Predicate pred) {
		super(src, dst, label);
		m_predicate = pred;
	}
	
	public Predicate getPredicate() {
		return m_predicate;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result
				+ ((m_predicate == null) ? 0 : m_predicate.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		LabeledQueryEdge other = (LabeledQueryEdge)obj;
		if (m_predicate == null) {
			if (other.m_predicate != null)
				return false;
		} else if (!m_predicate.equals(other.m_predicate))
			return false;
		return true;
	}
}
