package edu.unika.aifb.graphindex.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.jgrapht.graph.ClassBasedEdgeFactory;

import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.storage.StorageException;

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

	public NamedQueryGraph<String,LabeledQueryEdge<String>> toQueryGraph() throws StorageException {
		NamedQueryGraph<String,LabeledQueryEdge<String>> g = new NamedQueryGraph<String,LabeledQueryEdge<String>>("query", new ClassBasedEdgeFactory<String,LabeledEdge<String>>((Class<? extends LabeledEdge<String>>)LabeledQueryEdge.class));
		for (Literal l : m_literals) {
			g.addEdge(l.getSubject(), l.getPredicate(), l.getObject());
		}
		return g;
	}
	
}
