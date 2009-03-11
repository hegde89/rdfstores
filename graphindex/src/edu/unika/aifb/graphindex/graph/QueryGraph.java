package edu.unika.aifb.graphindex.graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.query.model.Term;

public class QueryGraph extends IndexGraph {

	private QueryNode[] m_queryNodes;
	private Set<String> m_vars;
	private Set<Integer> m_varNodes;
	private Set<String> m_selectVars;
	private Term[] m_terms;
	private Map<String,Integer> m_label2node;
	
	private final List<String> m_emptyList = new ArrayList<String>();
	private static final Logger log = Logger.getLogger(QueryGraph.class);
	
	public QueryGraph(NamedGraph<String,LabeledEdge<String>> graph, Map<String,List<String>> members, Map<String,Term> terms, int order) {
		super(graph, order);
		m_queryNodes = new QueryNode [nodeCount()];
		m_terms = new Term [nodeCount()];
		m_varNodes = new HashSet<Integer>();
		m_label2node = new HashMap<String,Integer>();
		
		for (int i = 0; i < nodeCount(); i++) {
			m_label2node.put(getNodeLabel(i), i);
			if (terms != null)
				m_terms[i] = terms.get(getNodeLabel(i));
			
			m_queryNodes[i] = new QueryNode();
			if (members != null && members.containsKey(getNodeLabel(i)))
				for (String m : members.get(getNodeLabel(i))) 
					m_queryNodes[i].addMember(m);
		}
	}
	
	public QueryGraph(NamedGraph<String,LabeledEdge<String>> graph, Map<String,List<String>> members) {
		this(graph, members, null, 1);
	}
	
	public QueryGraph(NamedGraph<String,LabeledEdge<String>> graph, Map<String,List<String>> members, int order) {
		this(graph, members, null, order);
	}
	
	public QueryGraph(NamedGraph<String,LabeledEdge<String>> graph, Map<String,List<String>> members, Map<String,Term> terms) {
		this(graph, members, terms, 1);
	}
	
	public void setVariables(Set<String> vars) {
		m_vars = vars;
		for (int i = 0; i < nodeCount(); i++)
			if (m_vars.contains(getNodeLabel(i)))
				m_varNodes.add(i);
	}
	
	public void setSelectVariables(Set<String> selectVars) {
		m_selectVars = selectVars;
	}
	
	public Set<Integer> getVariableNodes() {
		return m_varNodes;
	}
	
	public List<Integer> getNonCompoundNodes() {
		List<Integer> nodes = new ArrayList<Integer>();
		for (int i = 0; i < nodeCount(); i++)
			if (!m_queryNodes[i].isCompound())
				nodes.add(i);
		return nodes;
	}
	
	public List<Integer> getCompoundNodes() {
		List<Integer> nodes = new ArrayList<Integer>();
		for (int i = 0; i < nodeCount(); i++)
			if (m_queryNodes[i].isCompound())
				nodes.add(i);
		return nodes;
	}

	public void setTerms(Map<String,Term> terms) {
		for (int i = 0; i < nodeCount(); i++)
			m_terms[i] = terms.get(getNodeLabel(i));
	}
	
	public void setTerm(int node, Term term) {
		m_terms[node] = term;
	}

	private void addMember(int node, String v) {
		m_queryNodes[node].addMember(v);
	}
	
	public QueryNode getQueryNode(int node) {
		return m_queryNodes[node];
	}
	
	public Term getTerm(int node) {
		return m_terms[node];
	}
	
	public List<IndexEdge> edges() {
		List<IndexEdge> edges = new ArrayList<IndexEdge>();
		for (int i = 0; i < nodeCount(); i++)
			edges.addAll(outgoingEdges(i));
		return edges;
	}

	public Set<String> getVariables() {
		return m_vars;
	}

	public int getNode(String label) {
		return m_label2node.get(label);
	}
	
}
