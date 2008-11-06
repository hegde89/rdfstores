package edu.unika.aifb.graphindex.query.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.ClassBasedEdgeFactory;
import org.jgrapht.graph.DirectedMultigraph;

import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.query.LabeledQueryEdge;
import edu.unika.aifb.graphindex.query.NamedQueryGraph;
import edu.unika.aifb.graphindex.storage.StorageException;

public class Query {
	private List<Literal> m_literals;
	private List<String> m_selectVariables;
	private String m_name;
	
	public Query(String[] vars) {
		m_literals = new ArrayList<Literal>();
		m_selectVariables = Arrays.asList(vars);
	}
	
	public String getName() {
		return m_name;
	}
	
	public void setName(String name) {
		m_name = name;
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
	
	public Graph<QueryNode> toGraph() throws StorageException {
		DirectedGraph<QueryNode,LabeledEdge<QueryNode>> g = new DirectedMultigraph<QueryNode,LabeledEdge<QueryNode>>(new ClassBasedEdgeFactory<QueryNode,LabeledEdge<QueryNode>>((Class<? extends LabeledEdge<QueryNode>>)LabeledEdge.class));
		Map<String,QueryNode> t2qn = new HashMap<String,QueryNode>();
		for (Literal l : m_literals) {
			QueryNode src = t2qn.get(l.getSubject().toString());
			if (src == null) {
				src = new QueryNode(l.getSubject().toString());
				src.addMember(l.getSubject().toString());
				src.setTerm(l.getSubject().toString(), l.getSubject());
				t2qn.put(l.getSubject().toString(), src);
				g.addVertex(src);
			}

			QueryNode dst = t2qn.get(l.getObject().toString());
			if (dst == null) {
				dst = new QueryNode(l.getObject().toString());
				dst.addMember(l.getObject().toString());
				dst.setTerm(l.getObject().toString(), l.getObject());
				t2qn.put(l.getObject().toString(), dst);
				g.addVertex(dst);
			}

			g.addEdge(src, dst, new LabeledEdge<QueryNode>(src, dst, l.getPredicate().getUri()));
		}
		
		return new Graph<QueryNode>(g);
	}
	
	public String toString() {
		String s = "";
		String nl = "";
		for (Literal l : m_literals) {
			s += nl + l.getSubject() + " " + l.getPredicate() + " " + l.getObject();
			nl = "\n";
		}
		return s;
	}
	
	public Map<String,Term> getTerms() {
		Map<String,Term> terms = new HashMap<String,Term>();
		for (Literal l : m_literals) {
			terms.put(l.getSubject().toString(), l.getSubject());
			terms.put(l.getObject().toString(), l.getObject());
		}
		return terms;
	}

	public Set<String> getVariables() {
		Set<String> vars = new HashSet<String>();
		for (Literal l : m_literals) {
			if (l.getSubject() instanceof Variable)
				vars.add(l.getSubject().toString());
			if (l.getObject() instanceof Variable)
				vars.add(l.getObject().toString());
		}
		return vars;
	}
}
