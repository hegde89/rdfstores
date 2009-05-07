package edu.unika.aifb.graphindex.graph;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import edu.unika.aifb.graphindex.query.model.Term;

public class QueryNode implements Comparable<QueryNode> {
	private String m_name;
	private SortedSet<String> m_members;
	private Set<String> m_variables;
	private Set<String> m_groundTerms;
	private Map<String,Term> m_terms;
	private QueryNode m_condensed;
	
	public QueryNode() {
		this(null);
	}
	
	public QueryNode(String name) {
		m_name = name;
		m_members = new TreeSet<String>();
		m_variables = new HashSet<String>();
		m_groundTerms = new HashSet<String>();
		m_terms = new HashMap<String,Term>();
		m_condensed = null;
	}
	
	public void setCondensed(QueryNode q) {
		m_condensed = q;
	}
	
	public QueryNode getCondensed() {
		return m_condensed;
	}
	
	public void setTerm(String v, Term t) {
		m_terms.put(v, t);
	}
	
	public Term getTerm(String v) {
		return m_terms.get(v);
	}
	
	public void addMember(String v) {
		m_members.add(v);
		if (v.startsWith("?"))
			m_variables.add(v);
		else
			m_groundTerms.add(v);
	}
	
	public boolean isCompound() {
		return m_members.size() > 1;
	}
	
	public boolean hasGroundTerms() {
		return m_groundTerms.size() > 0;
	}
	
	public int numberOfGroundTerms() {
		return m_groundTerms.size();
	}
	
	public boolean hasVariables() {
		return m_variables.size() > 0;
	}
	
	public int numberOfVariables() {
		return m_variables.size();
	}
	
	public String getSingleMember() {
		if (isCompound())
			return null;
		else 
			return m_members.first();
	}
	
	public Set<String> getMembers() {
		return m_members;
	}
	
	public Set<String> getVariables() {
		return m_variables;
	}
	
	public Set<String> getGroundTerms() {
		return m_groundTerms;
	}
	
	public String toString() {
		return getName() + m_members.toString();
	}

	public int compareTo(QueryNode o) {
		if (o.hasGroundTerms() && !this.hasGroundTerms())
			return 1;
		if (!o.hasGroundTerms() && this.hasGroundTerms())
			return -1;
		if (o.hasGroundTerms() && this.hasGroundTerms()) {
			if (o.numberOfVariables() < this.numberOfVariables())
				return 1;
			else
				return -1;
		}
		return this.getName().compareTo(o.getName());
	}

	public String getName() {
		return m_name;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((m_name == null) ? 0 : m_name.hashCode());
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
		QueryNode other = (QueryNode)obj;
		if (m_name == null) {
			if (other.m_name != null)
				return false;
		} else if (!m_name.equals(other.m_name))
			return false;
		return true;
	}

	public int size() {
		return m_members.size();
	}
}
