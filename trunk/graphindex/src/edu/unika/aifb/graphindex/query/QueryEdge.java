package edu.unika.aifb.graphindex.query;

import edu.unika.aifb.graphindex.graph.Edge;
import edu.unika.aifb.graphindex.graph.Vertex;

public class QueryEdge extends Edge {
	private Predicate m_predicate;
	
	public QueryEdge(Vertex source, Vertex target, Predicate pred) {
		super(source, target, pred.getUri());
		m_predicate = pred;
	}
	
	public Predicate getPredicate() {
		return m_predicate;
	}
}
