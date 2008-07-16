package edu.unika.aifb.graphindex.query;

import edu.unika.aifb.graphindex.graph.GraphElement;
import edu.unika.aifb.graphindex.graph.Vertex;

public class QueryVertex extends Vertex {
	private Term m_term;
	
	public QueryVertex(Term term) {
		super(term.toString());
		m_term = term;
	}
	
	public QueryVertex(Vertex v) {
		super(v.getLabel());
		if (v instanceof QueryVertex) {
			m_term = ((QueryVertex)v).getTerm();
		}
	}

	public Term getTerm() {
		return m_term;
	}

	@Override
	public Object clone() {
		QueryVertex v = new QueryVertex(getTerm());
		return v;
	}
}
