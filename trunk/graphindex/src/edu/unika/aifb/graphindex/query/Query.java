package edu.unika.aifb.graphindex.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.Path;

public class Query {
	private List<Literal> m_literals;
	private List<String> m_selectVariables;
	
	public Query(String[] vars) {
		m_literals = new ArrayList<Literal>();
		m_selectVariables = Arrays.asList(vars);
	}
	
	public List<String> getSelectVariables() {
		return m_selectVariables;
	}
	
	public List<Literal> getLiterals() {
		return m_literals;
	}

	public boolean addLiteral(Literal o) {
		return m_literals.add(o);
	}

	public QueryGraph toQueryGraph() {
		QueryGraph g = new QueryGraph("query");
		for (Literal l : m_literals) {
			g.addEdge(l.getSubject(), l.getPredicate(), l.getObject());
		}
		return g;
	}
	
}
