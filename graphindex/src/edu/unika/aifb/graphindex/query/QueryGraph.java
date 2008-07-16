package edu.unika.aifb.graphindex.query;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.Vertex;


public class QueryGraph extends Graph {

	public QueryGraph(String name) {
		super(name);
	}
	
	public QueryGraph(String name, boolean tryStub) {
		super(name, tryStub);
	}

	public QueryGraph(String name, int id) {
		super(name, id);
	}

	public void addEdge(Term subject, Predicate predicate, Term object) {
		QueryVertex source = new QueryVertex(subject);
		QueryVertex target = new QueryVertex(object);
		addVertex(source);
		addVertex(target);
		QueryEdge e = new QueryEdge(source, target, predicate);
		addEdge(e);
	}
	
	public Set<QueryVertex> getGroundTerms() {
		Set<QueryVertex> grounded = new HashSet<QueryVertex>();
		for (Vertex vertex : vertices()) {
			QueryVertex v = (QueryVertex)vertex;
			if (v.getTerm() instanceof Individual)
				grounded.add(v);
		}
		return grounded;
	}
	
	public int numberOfVariables() {
		int i = 0;
		for (Vertex vertex : vertices()) {
			QueryVertex v = (QueryVertex)vertex;
			if (v.getTerm() instanceof Variable)
				i++;
		}
		return i;
	}
	
	public String[] getVariables() {
		String[] vars = new String[numberOfVariables()];
		int i = 0;
		for (Vertex vertex : vertices()) {
			QueryVertex v = (QueryVertex)vertex;
			if (v.getTerm() instanceof Variable) {
				vars[i] = v.getTerm().toString();
				i++;
			}
		}
		return vars;
	}
	
	public Set<QueryVertex> getLeafVertices() {
		Set<QueryVertex> leafs = new HashSet<QueryVertex>();
		for (Vertex vertex : vertices()) 
			if (vertex.outDegree() == 0)
				leafs.add((QueryVertex)vertex);
		return leafs;
	}
}
