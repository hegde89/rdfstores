package edu.unika.aifb.graphindex.query;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.jgrapht.EdgeFactory;

import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.NamedGraph;
import edu.unika.aifb.graphindex.query.model.Constant;
import edu.unika.aifb.graphindex.query.model.Individual;
import edu.unika.aifb.graphindex.query.model.Predicate;
import edu.unika.aifb.graphindex.query.model.Term;
import edu.unika.aifb.graphindex.query.model.Variable;
import edu.unika.aifb.graphindex.storage.StorageException;

public class NamedQueryGraph<V extends String, E extends LabeledEdge<String>> extends NamedGraph<String,LabeledEdge<String>> {

	private static final long serialVersionUID = 2275819960809763457L;
	
	private Map<String,Term> m_terms;

	public NamedQueryGraph(String name, Class<? extends E> edgeClass) throws StorageException {
		super(name, edgeClass);
		m_terms = new HashMap<String,Term>();
	}

	public NamedQueryGraph(String name, EdgeFactory<String,LabeledEdge<String>> ef) throws StorageException {
		super(name, ef);
		m_terms = new HashMap<String,Term>();
	}
	
	public Term getTerm(String v) {
		return m_terms.get(v);
	}
	
	public void addEdge(Term subject, Predicate predicate, Term object) {
		String src = subject.toString();
		String dst = object.toString();
		String edge = predicate.getUri();
		
		addVertex(src);
		addVertex(dst);
		addEdge(src, dst, new LabeledQueryEdge<String>(src, dst, edge, predicate));
		m_terms.put((V)src, subject);
		m_terms.put((V)dst, object);
	}
	
	public Set<String> getGroundTerms() {
		Set<String> groundTerms = new HashSet<String>();
		for (String v : vertexSet()) {
			if (getTerm(v) instanceof Individual || getTerm(v) instanceof Constant)
				groundTerms.add(v);
		}
		return groundTerms;
	}
	
	public Set<String> getVariables() {
		Set<String> vars = new HashSet<String>();
		for (String v : vertexSet()) 
			if (getTerm(v) instanceof Variable)
				vars.add(v);
		return vars;
	}
}
